

-- SP_STG_to_INT_EstadoPedido
IF OBJECT_ID('SP_STG_to_INT_EstadoPedido', 'P') IS NOT NULL
    DROP PROCEDURE SP_STG_to_INT_EstadoPedido;
GO

CREATE PROCEDURE SP_STG_to_INT_EstadoPedido
    @ID_Proceso INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RegistrosProcesados INT = 0;
    DECLARE @RegistrosRechazados INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Truncar tabla INT (política de carga completa)
        TRUNCATE TABLE INT_EstadoPedido;
        
        -- Insertar registros válidos con transformaciones
        INSERT INTO INT_EstadoPedido (
            CodEstado,
            Descripcion_Estado,
            Tipo_Estado,
            Orden_Secuencia,
            EsEstadoFinal,
            Fecha_Proceso,
            ID_Proceso
        )
        SELECT DISTINCT
            -- Limpieza de clave natural
            UPPER(LTRIM(RTRIM(CodEstado))) AS CodEstado,
            
            -- Limpieza de descripción
            LTRIM(RTRIM(Descripcion_Estado)) AS Descripcion_Estado,
            
            -- Clasificación de tipo de estado
            CASE 
                WHEN LOWER(Descripcion_Estado) LIKE '%entregad%' 
                  OR LOWER(Descripcion_Estado) LIKE '%completad%' THEN 'Completado'
                WHEN LOWER(Descripcion_Estado) LIKE '%cancelad%' 
                  OR LOWER(Descripcion_Estado) LIKE '%devuelt%' THEN 'Cancelado'
                ELSE 'En Proceso'
            END AS Tipo_Estado,
            
            -- Orden secuencial estimado
            CASE 
                WHEN LOWER(Descripcion_Estado) LIKE '%preparaci%' THEN 1
                WHEN LOWER(Descripcion_Estado) LIKE '%empaqu%' THEN 2
                WHEN LOWER(Descripcion_Estado) LIKE '%despach%' 
                  OR LOWER(Descripcion_Estado) LIKE '%env%' THEN 3
                WHEN LOWER(Descripcion_Estado) LIKE '%tr%nsito%' 
                  OR LOWER(Descripcion_Estado) LIKE '%camino%' THEN 4
                WHEN LOWER(Descripcion_Estado) LIKE '%entregad%' THEN 5
                WHEN LOWER(Descripcion_Estado) LIKE '%devuelt%' THEN 6
                WHEN LOWER(Descripcion_Estado) LIKE '%cancelad%' THEN 7
                ELSE 99
            END AS Orden_Secuencia,
            
            -- Estado final
            CASE 
                WHEN LOWER(Descripcion_Estado) LIKE '%entregad%' 
                  OR LOWER(Descripcion_Estado) LIKE '%cancelad%' 
                  OR LOWER(Descripcion_Estado) LIKE '%devuelt%' THEN 1
                ELSE 0
            END AS EsEstadoFinal,
            
            GETDATE() AS Fecha_Proceso,
            @ID_Proceso AS ID_Proceso
        FROM STG_EstadoDelPedido
        WHERE 
            -- Validaciones críticas
            CodEstado IS NOT NULL 
            AND LTRIM(RTRIM(CodEstado)) <> ''
            AND Descripcion_Estado IS NOT NULL
            AND LTRIM(RTRIM(Descripcion_Estado)) <> '';
        
        SET @RegistrosProcesados = @@ROWCOUNT;
        
        -- Registrar rechazados
        INSERT INTO ETL_Registros_Rechazados (
            ID_Proceso, Tabla_Origen, Registro_Original, Motivo_Rechazo
        )
        SELECT 
            @ID_Proceso,
            'STG_EstadoDelPedido',
            'CodEstado: ' + ISNULL(CodEstado, 'NULL') + 
            ', Descripcion: ' + ISNULL(Descripcion_Estado, 'NULL'),
            CASE 
                WHEN CodEstado IS NULL OR LTRIM(RTRIM(CodEstado)) = '' 
                    THEN 'CodEstado nulo o vacío'
                WHEN Descripcion_Estado IS NULL OR LTRIM(RTRIM(Descripcion_Estado)) = '' 
                    THEN 'Descripcion_Estado nulo o vacío'
            END
        FROM STG_EstadoDelPedido
        WHERE 
            CodEstado IS NULL 
            OR LTRIM(RTRIM(CodEstado)) = ''
            OR Descripcion_Estado IS NULL
            OR LTRIM(RTRIM(Descripcion_Estado)) = '';
        
        SET @RegistrosRechazados = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        
        PRINT 'SP_STG_to_INT_EstadoPedido: ' + 
              CAST(@RegistrosProcesados AS VARCHAR) + ' registros procesados, ' +
              CAST(@RegistrosRechazados AS VARCHAR) + ' rechazados';
        
        RETURN @RegistrosProcesados;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMsg VARCHAR(MAX) = ERROR_MESSAGE();
        PRINT 'ERROR en SP_STG_to_INT_EstadoPedido: ' + @ErrorMsg;
        
        THROW;
    END CATCH
END;
GO




-- SP_STG_to_INT_Almacen
IF OBJECT_ID('SP_STG_to_INT_Almacen', 'P') IS NOT NULL
    DROP PROCEDURE SP_STG_to_INT_Almacen;
GO

CREATE PROCEDURE SP_STG_to_INT_Almacen
    @ID_Proceso INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RegistrosProcesados INT = 0;
    DECLARE @RegistrosRechazados INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        TRUNCATE TABLE INT_Almacen;
        
        INSERT INTO INT_Almacen (
            CodAlmacen,
            NombreAlmacen,
            Ubicacion,
            Ciudad,
            Provincia,
            CodigoPostal,
            TipoAlmacen,
            Activo,
            Fecha_Proceso,
            ID_Proceso
        )
        SELECT DISTINCT
            UPPER(LTRIM(RTRIM(CodAlmacen))) AS CodAlmacen,
            LTRIM(RTRIM(Nombre_Almacen)) AS NombreAlmacen,
            LTRIM(RTRIM(Ubicacion)) AS Ubicacion,
            
            -- Parsear Ciudad (antes de la coma)
            CASE 
                WHEN CHARINDEX(',', Ubicacion) > 0 
                THEN LTRIM(RTRIM(SUBSTRING(Ubicacion, 1, CHARINDEX(',', Ubicacion) - 1)))
                ELSE NULL
            END AS Ciudad,
            
            -- Parsear Provincia (después de la coma)
            CASE 
                WHEN CHARINDEX(',', Ubicacion) > 0 
                THEN LTRIM(RTRIM(SUBSTRING(Ubicacion, CHARINDEX(',', Ubicacion) + 1, LEN(Ubicacion))))
                ELSE LTRIM(RTRIM(Ubicacion))
            END AS Provincia,
            
            NULL AS CodigoPostal, -- No disponible en STG
            'Principal' AS TipoAlmacen, -- Valor por defecto
            1 AS Activo,
            GETDATE(),
            @ID_Proceso
        FROM STG_Almacenes
        WHERE 
            CodAlmacen IS NOT NULL 
            AND LTRIM(RTRIM(CodAlmacen)) <> ''
            AND Nombre_Almacen IS NOT NULL
            AND LTRIM(RTRIM(Nombre_Almacen)) <> '';
        
        SET @RegistrosProcesados = @@ROWCOUNT;
        
        -- Registrar rechazados
        INSERT INTO ETL_Registros_Rechazados (ID_Proceso, Tabla_Origen, Registro_Original, Motivo_Rechazo)
        SELECT 
            @ID_Proceso,
            'STG_Almacenes',
            'CodAlmacen: ' + ISNULL(CodAlmacen, 'NULL') + ', Nombre: ' + ISNULL(Nombre_Almacen, 'NULL'),
            'Campos críticos nulos o vacíos'
        FROM STG_Almacenes
        WHERE CodAlmacen IS NULL OR LTRIM(RTRIM(CodAlmacen)) = ''
           OR Nombre_Almacen IS NULL OR LTRIM(RTRIM(Nombre_Almacen)) = '';
        
        SET @RegistrosRechazados = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        
        PRINT 'SP_STG_to_INT_Almacen: ' + 
              CAST(@RegistrosProcesados AS VARCHAR) + ' procesados, ' +
              CAST(@RegistrosRechazados AS VARCHAR) + ' rechazados';
        
        RETURN @RegistrosProcesados;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT 'ERROR en SP_STG_to_INT_Almacen: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO





-- SP_STG_to_INT_Cliente
IF OBJECT_ID('SP_STG_to_INT_Cliente', 'P') IS NOT NULL
    DROP PROCEDURE SP_STG_to_INT_Cliente;
GO

CREATE PROCEDURE SP_STG_to_INT_Cliente
    @ID_Proceso INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RegistrosProcesados INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        TRUNCATE TABLE INT_Cliente;
        
        INSERT INTO INT_Cliente (
            CodCliente, RazonSocial, Telefono, Mail, Direccion,
            Localidad, Provincia, CP, Fecha_Proceso, ID_Proceso
        )
        SELECT DISTINCT
            UPPER(LTRIM(RTRIM(CodCliente))) AS CodCliente,
            LTRIM(RTRIM(RazonSocial)) AS RazonSocial,
            NULLIF(LTRIM(RTRIM(Telefono)), '') AS Telefono,
            NULLIF(LTRIM(RTRIM(LOWER(Mail))), '') AS Mail, 
            NULLIF(LTRIM(RTRIM(Direccion)), '') AS Direccion,
            LTRIM(RTRIM(Localidad)) AS Localidad,
            UPPER(LTRIM(RTRIM(Provincia))) AS Provincia,
            NULLIF(LTRIM(RTRIM(CP)), '') AS CP,
            GETDATE(),
            @ID_Proceso
        FROM STG_Clientes
        WHERE 
            CodCliente IS NOT NULL AND LTRIM(RTRIM(CodCliente)) <> ''
            AND RazonSocial IS NOT NULL AND LTRIM(RTRIM(RazonSocial)) <> ''
            AND Localidad IS NOT NULL AND LTRIM(RTRIM(Localidad)) <> ''
            AND Provincia IS NOT NULL AND LTRIM(RTRIM(Provincia)) <> '';
        
        SET @RegistrosProcesados = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        
        PRINT 'SP_STG_to_INT_Cliente: ' + CAST(@RegistrosProcesados AS VARCHAR) + ' procesados';
        RETURN @RegistrosProcesados;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT 'ERROR en SP_STG_to_INT_Cliente: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO




-- SP_STG_to_INT_Producto
IF OBJECT_ID('SP_STG_to_INT_Producto', 'P') IS NOT NULL
    DROP PROCEDURE SP_STG_to_INT_Producto;
GO

CREATE PROCEDURE SP_STG_to_INT_Producto
    @ID_Proceso INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RegistrosProcesados INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        TRUNCATE TABLE INT_Producto;
        
        INSERT INTO INT_Producto (
            CodigoProducto, Descripcion, Categoria, Marca,
            PrecioCosto, PrecioVentaSugerido, Fecha_Proceso, ID_Proceso
        )
        SELECT DISTINCT
            UPPER(LTRIM(RTRIM(CodigoProducto))) AS CodigoProducto,
            LTRIM(RTRIM(Descripcion)) AS Descripcion,
            UPPER(LTRIM(RTRIM(Categoria))) AS Categoria, 
            UPPER(LTRIM(RTRIM(Marca))) AS Marca, 
            
            -- Casteo de precios con validación
            CAST(
                CASE 
                    WHEN TRY_CAST(REPLACE(PrecioCosto, ',', '.') AS DECIMAL(18,2)) IS NULL THEN 0
                    WHEN TRY_CAST(REPLACE(PrecioCosto, ',', '.') AS DECIMAL(18,2)) < 0 THEN 0
                    ELSE TRY_CAST(REPLACE(PrecioCosto, ',', '.') AS DECIMAL(18,2))
                END AS DECIMAL(18,2)
            ) AS PrecioCosto,
            
            CAST(
                CASE 
                    WHEN TRY_CAST(REPLACE(PrecioVentaSugerido, ',', '.') AS DECIMAL(18,2)) IS NULL THEN 0
                    WHEN TRY_CAST(REPLACE(PrecioVentaSugerido, ',', '.') AS DECIMAL(18,2)) < 0 THEN 0
                    ELSE TRY_CAST(REPLACE(PrecioVentaSugerido, ',', '.') AS DECIMAL(18,2))
                END AS DECIMAL(18,2)
            ) AS PrecioVentaSugerido,
            
            GETDATE(),
            @ID_Proceso
        FROM STG_Productos
        WHERE 
            CodigoProducto IS NOT NULL AND LTRIM(RTRIM(CodigoProducto)) <> ''
            AND Descripcion IS NOT NULL AND LTRIM(RTRIM(Descripcion)) <> ''
            AND Categoria IS NOT NULL AND LTRIM(RTRIM(Categoria)) <> ''
            AND Marca IS NOT NULL AND LTRIM(RTRIM(Marca)) <> '';
        
        SET @RegistrosProcesados = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        
        PRINT 'SP_STG_to_INT_Producto: ' + CAST(@RegistrosProcesados AS VARCHAR) + ' procesados';
        RETURN @RegistrosProcesados;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT 'ERROR en SP_STG_to_INT_Producto: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO



-- SP_STG_to_INT_Tienda
IF OBJECT_ID('SP_STG_to_INT_Tienda', 'P') IS NOT NULL
    DROP PROCEDURE SP_STG_to_INT_Tienda;
GO

CREATE PROCEDURE SP_STG_to_INT_Tienda
    @ID_Proceso INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RegistrosProcesados INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        TRUNCATE TABLE INT_Tienda;
        
        INSERT INTO INT_Tienda (
            CodigoTienda, Descripcion, Direccion, Localidad,
            Provincia, CP, TipoTienda, Fecha_Proceso, ID_Proceso
        )
        SELECT DISTINCT
            UPPER(LTRIM(RTRIM(CodigoTienda))) AS CodigoTienda,
            LTRIM(RTRIM(Descripcion)) AS Descripcion,
            NULLIF(LTRIM(RTRIM(Direccion)), '') AS Direccion,
            LTRIM(RTRIM(Localidad)) AS Localidad,
            UPPER(LTRIM(RTRIM(Provincia))) AS Provincia,
            NULLIF(LTRIM(RTRIM(CP)), '') AS CP,
            UPPER(LTRIM(RTRIM(TipoTienda))) AS TipoTienda,
            GETDATE(),
            @ID_Proceso
        FROM STG_Tiendas
        WHERE 
            CodigoTienda IS NOT NULL AND LTRIM(RTRIM(CodigoTienda)) <> ''
            AND Descripcion IS NOT NULL AND LTRIM(RTRIM(Descripcion)) <> ''
            AND Localidad IS NOT NULL AND LTRIM(RTRIM(Localidad)) <> ''
            AND Provincia IS NOT NULL AND LTRIM(RTRIM(Provincia)) <> ''
            AND TipoTienda IS NOT NULL AND LTRIM(RTRIM(TipoTienda)) <> '';
        
        SET @RegistrosProcesados = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        
        PRINT 'SP_STG_to_INT_Tienda: ' + CAST(@RegistrosProcesados AS VARCHAR) + ' procesados';
        RETURN @RegistrosProcesados;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT 'ERROR en SP_STG_to_INT_Tienda: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO





-- SP_STG_to_INT_Proveedor
IF OBJECT_ID('SP_STG_to_INT_Proveedor', 'P') IS NOT NULL
    DROP PROCEDURE SP_STG_to_INT_Proveedor;
GO

CREATE PROCEDURE SP_STG_to_INT_Proveedor
    @ID_Proceso INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Insertados INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        INSERT INTO INT_Proveedor (
            CodProveedor,
            NombreProveedor,
            TipoServicio,
            Activo,
            Fecha_Proceso,
            ID_Proceso
        )
        SELECT
            CodProveedor,
            MAX(NombreProveedor) AS NombreProveedor,
            'Estándar',
            1,
            GETDATE(),
            @ID_Proceso
        FROM (
            SELECT
                CAST(LTRIM(RTRIM(CodProveedor)) AS INT) AS CodProveedor,
                UPPER(LTRIM(RTRIM(Proveedor))) AS NombreProveedor
            FROM STG_Entregas
            WHERE CodProveedor IS NOT NULL
              AND Proveedor IS NOT NULL
              AND LTRIM(RTRIM(CodProveedor)) <> ''
        ) s
        WHERE NOT EXISTS (
            SELECT 1
            FROM INT_Proveedor p
            WHERE p.CodProveedor = s.CodProveedor
        )
        GROUP BY CodProveedor;

        SET @Insertados = @@ROWCOUNT;

        COMMIT TRANSACTION;

        PRINT 'SP_STG_to_INT_Proveedor OK | Insertados: ' + CAST(@Insertados AS VARCHAR);

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        PRINT 'ERROR SP_STG_to_INT_Proveedor: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO








-- SP_STG_to_INT_Ventas
-- Unifica STG_Ventas y STG_Ventas_Add

IF OBJECT_ID('SP_STG_to_INT_Ventas', 'P') IS NOT NULL
    DROP PROCEDURE SP_STG_to_INT_Ventas;
GO

CREATE PROCEDURE SP_STG_to_INT_Ventas
    @ID_Proceso INT
AS
BEGIN
    SET NOCOUNT ON;

    IF @ID_Proceso IS NULL
    BEGIN
        RAISERROR(
            'SP_STG_to_INT_Ventas debe ejecutarse desde el orquestador con ID_Proceso',
            16, 1
        );
        RETURN;
    END;

    DECLARE @RegistrosProcesados INT = 0;
    DECLARE @RegistrosRechazados INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        
        TRUNCATE TABLE INT_Ventas;

        INSERT INTO INT_Ventas (
            FechaVenta,
            CodigoProducto,
            CodigoCliente,
            CodigoTienda,
            Cantidad,
            PrecioVenta,
            Total_IVA,
            Fecha_Proceso,
            ID_Proceso
        )
        SELECT 
            TRY_CAST(FechaVenta AS DATE),
            UPPER(LTRIM(RTRIM(CodigoProducto))),
            UPPER(LTRIM(RTRIM(CodigoCliente))),
            UPPER(LTRIM(RTRIM(CodigoTienda))),
            TRY_CAST(Cantidad AS INT),
            TRY_CAST(REPLACE(PrecioVenta, ',', '.') AS DECIMAL(18,2)),
            TRY_CAST(REPLACE(PrecioVenta, ',', '.') AS DECIMAL(18,2)) *
            TRY_CAST(Cantidad AS INT) * 0.21,
            GETDATE(),
            @ID_Proceso
        FROM STG_Ventas
        WHERE 
            TRY_CAST(FechaVenta AS DATE) IS NOT NULL
            AND CodigoProducto IS NOT NULL AND LTRIM(RTRIM(CodigoProducto)) <> ''
            AND CodigoCliente IS NOT NULL AND LTRIM(RTRIM(CodigoCliente)) <> ''
            AND CodigoTienda IS NOT NULL AND LTRIM(RTRIM(CodigoTienda)) <> ''
            AND TRY_CAST(Cantidad AS INT) > 0
            AND TRY_CAST(REPLACE(PrecioVenta, ',', '.') AS DECIMAL(18,2)) >= 0

        UNION ALL

        SELECT 
            TRY_CAST(FechaVenta AS DATE),
            UPPER(LTRIM(RTRIM(CodigoProducto))),
            UPPER(LTRIM(RTRIM(CodigoCliente))),
            UPPER(LTRIM(RTRIM(CodigoTienda))),
            TRY_CAST(Cantidad AS INT),
            TRY_CAST(REPLACE(PrecioVenta, ',', '.') AS DECIMAL(18,2)),
            TRY_CAST(REPLACE(PrecioVenta, ',', '.') AS DECIMAL(18,2)) *
            TRY_CAST(Cantidad AS INT) * 0.21,
            GETDATE(),
            @ID_Proceso
        FROM STG_Ventas_Add
        WHERE 
            TRY_CAST(FechaVenta AS DATE) IS NOT NULL
            AND CodigoProducto IS NOT NULL AND LTRIM(RTRIM(CodigoProducto)) <> ''
            AND CodigoCliente IS NOT NULL AND LTRIM(RTRIM(CodigoCliente)) <> ''
            AND CodigoTienda IS NOT NULL AND LTRIM(RTRIM(CodigoTienda)) <> ''
            AND TRY_CAST(Cantidad AS INT) > 0
            AND TRY_CAST(REPLACE(PrecioVenta, ',', '.') AS DECIMAL(18,2)) >= 0;

        SET @RegistrosProcesados = @@ROWCOUNT;

        
        -- REGISTROS RECHAZADOS
        
        INSERT INTO ETL_Registros_Rechazados (
            ID_Proceso,
            Tabla_Origen,
            Registro_Original,
            Motivo_Rechazo,
            Fecha_Rechazo
        )
        SELECT 
            @ID_Proceso,
            'STG_Ventas',
            'Fecha=' + ISNULL(FechaVenta, 'NULL') +
            ' | Producto=' + ISNULL(CodigoProducto, 'NULL'),
            'Datos inválidos o campos críticos nulos',
            GETDATE()
        FROM STG_Ventas
        WHERE 
            TRY_CAST(FechaVenta AS DATE) IS NULL
            OR CodigoProducto IS NULL OR LTRIM(RTRIM(CodigoProducto)) = ''
            OR CodigoCliente IS NULL OR LTRIM(RTRIM(CodigoCliente)) = ''
            OR CodigoTienda IS NULL OR LTRIM(RTRIM(CodigoTienda)) = ''
            OR TRY_CAST(Cantidad AS INT) IS NULL OR TRY_CAST(Cantidad AS INT) <= 0
            OR TRY_CAST(REPLACE(PrecioVenta, ',', '.') AS DECIMAL(18,2)) IS NULL
            OR TRY_CAST(REPLACE(PrecioVenta, ',', '.') AS DECIMAL(18,2)) < 0;

        SET @RegistrosRechazados = @@ROWCOUNT;

        COMMIT TRANSACTION;

        PRINT 'SP_STG_to_INT_Ventas: ' +
              CAST(@RegistrosProcesados AS VARCHAR) + ' procesados, ' +
              CAST(@RegistrosRechazados AS VARCHAR) + ' rechazados';

        RETURN @RegistrosProcesados;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT 'ERROR en SP_STG_to_INT_Ventas: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO





-- SP_STG_to_INT_Entregas
IF OBJECT_ID('SP_STG_to_INT_Entregas', 'P') IS NOT NULL
    DROP PROCEDURE SP_STG_to_INT_Entregas;
GO

CREATE PROCEDURE SP_STG_to_INT_Entregas
    @ID_Proceso INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RegistrosProcesados INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Limpiamos la tabla de integración para la nueva carga
        TRUNCATE TABLE INT_Entregas;
        
        INSERT INTO INT_Entregas (
            CodEntrega, 
            CodVenta, 
            CodProveedor, 
            CodAlmacen, 
            CodEstado,
            Fecha_Envio, 
            Fecha_Entrega, 
            FechaEstimadaEntrega,
            CantidadProductos, 
            PesoKg,           -- Nueva columna
            VolumenM3,        -- Nueva columna
            DistanciaKm,      -- Nueva columna
            CostoEntrega, 
            Fecha_Proceso, 
            ID_Proceso
        )
        SELECT 
            UPPER(LTRIM(RTRIM(CodEntrega))) AS CodEntrega,
            TRY_CAST(CodVenta AS BIGINT) AS CodVenta,
            UPPER(LTRIM(RTRIM(CodProveedor))) AS CodProveedor,
            UPPER(LTRIM(RTRIM(CodAlmacen))) AS CodAlmacen,
            UPPER(LTRIM(RTRIM(CodEstado))) AS CodEstado,
            
            -- Manejo de Fechas
            TRY_CAST(Fecha_Envio AS DATE) AS Fecha_Envio,
            TRY_CAST(NULLIF(Fecha_Entrega, '') AS DATE) AS Fecha_Entrega,
            DATEADD(DAY, 5, TRY_CAST(Fecha_Envio AS DATE)) AS FechaEstimadaEntrega,
            
            -- Métricas: Si vienen NULL en el STG (CSV), se fuerzan a 0 o valor default
            1 AS CantidadProductos,
            0.00 AS PesoKg,      -- Como el CSV no lo trae, forzamos 0
            0.000 AS VolumenM3,  -- Como el CSV no lo trae, forzamos 0
            0.00 AS DistanciaKm, --  Forzamos 0
            
            -- Costo de entrega: Si se quiere un valor base, ponerlo aca
            150.00 AS CostoEntrega, 
            
            GETDATE(),
            @ID_Proceso
        FROM STG_Entregas
        WHERE 
            CodEntrega IS NOT NULL AND LTRIM(RTRIM(CodEntrega)) <> ''
            AND CodProveedor IS NOT NULL AND LTRIM(RTRIM(CodProveedor)) <> ''
            AND CodEstado IS NOT NULL AND LTRIM(RTRIM(CodEstado)) <> ''
            AND TRY_CAST(Fecha_Envio AS DATE) IS NOT NULL
            -- Validación de coherencia de fechas
            AND (
                TRY_CAST(NULLIF(Fecha_Entrega, '') AS DATE) IS NULL
                OR TRY_CAST(NULLIF(Fecha_Entrega, '') AS DATE) >= TRY_CAST(Fecha_Envio AS DATE)
            );
        
        SET @RegistrosProcesados = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        
        PRINT 'SP_STG_to_INT_Entregas: ' + CAST(@RegistrosProcesados AS VARCHAR) + ' procesados exitosamente.';
        RETURN @RegistrosProcesados;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT 'ERROR en SP_STG_to_INT_Entregas: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO





-- SP ORQUESTADOR: Ejecuta todos los procesos STG->INT
IF OBJECT_ID('SP_Orquestador_STG_to_INT', 'P') IS NOT NULL
    DROP PROCEDURE SP_Orquestador_STG_to_INT;
GO

CREATE PROCEDURE SP_Orquestador_STG_to_INT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ID_Proceso INT;
    DECLARE @FechaInicio DATETIME = GETDATE();
    DECLARE @TotalProcesados INT = 0;
    
    BEGIN TRY
        -- Registrar inicio del proceso
        INSERT INTO ETL_Control_Procesos (
            Nombre_Proceso, Fecha_Inicio, Estado
        )
        VALUES (
            'STG_to_INT_Completo', @FechaInicio, 'EN_PROCESO'
        );
        
        SET @ID_Proceso = SCOPE_IDENTITY();
        
        PRINT '';
        PRINT '========================================';
        PRINT 'INICIANDO PROCESO ETL: STG -> INT';
        PRINT 'ID Proceso: ' + CAST(@ID_Proceso AS VARCHAR);
        PRINT '========================================';
        PRINT '';
        
        -- Ejecutar transformaciones en orden
        EXEC SP_STG_to_INT_EstadoPedido @ID_Proceso;
        EXEC SP_STG_to_INT_Almacen @ID_Proceso;
        EXEC SP_STG_to_INT_Cliente @ID_Proceso;
        EXEC SP_STG_to_INT_Producto @ID_Proceso;
        EXEC SP_STG_to_INT_Tienda @ID_Proceso;
        EXEC SP_STG_to_INT_Proveedor @ID_Proceso;
        EXEC SP_STG_to_INT_Ventas @ID_Proceso;
        EXEC SP_STG_to_INT_Entregas @ID_Proceso;
        
        -- Actualizar proceso como completado
        UPDATE ETL_Control_Procesos
        SET 
            Fecha_Fin = GETDATE(),
            Estado = 'COMPLETADO',
            Registros_Procesados = (
                SELECT 
                    (SELECT COUNT(*) FROM INT_EstadoPedido) +
                    (SELECT COUNT(*) FROM INT_Almacen) +
                    (SELECT COUNT(*) FROM INT_Cliente) +
                    (SELECT COUNT(*) FROM INT_Producto) +
                    (SELECT COUNT(*) FROM INT_Tienda) +
                    (SELECT COUNT(*) FROM INT_Proveedor) +
                    (SELECT COUNT(*) FROM INT_Ventas) +
                    (SELECT COUNT(*) FROM INT_Entregas)
            ),
            Registros_Rechazados = (
                SELECT COUNT(*) 
                FROM ETL_Registros_Rechazados 
                WHERE ID_Proceso = @ID_Proceso
            )
        WHERE ID_Proceso = @ID_Proceso;
        
        PRINT '';
        PRINT '========================================';
        PRINT 'PROCESO ETL: STG -> INT COMPLETADO';
        PRINT 'Duración: ' + CAST(DATEDIFF(SECOND, @FechaInicio, GETDATE()) AS VARCHAR) + ' segundos';
        PRINT '========================================';
        
    END TRY
    BEGIN CATCH
        -- Registrar error
        UPDATE ETL_Control_Procesos
        SET 
            Fecha_Fin = GETDATE(),
            Estado = 'ERROR',
            Mensaje_Error = ERROR_MESSAGE()
        WHERE ID_Proceso = @ID_Proceso;
        
        PRINT 'ERROR CRÍTICO: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

PRINT '';
PRINT '========================================';
PRINT 'SPs STG->INT CREADOS EXITOSAMENTE';
PRINT '========================================';
PRINT '';
PRINT 'Stored Procedures creados:';
PRINT '  - SP_STG_to_INT_EstadoPedido';
PRINT '  - SP_STG_to_INT_Almacen';
PRINT '  - SP_STG_to_INT_Cliente';
PRINT '  - SP_STG_to_INT_Producto';
PRINT '  - SP_STG_to_INT_Tienda';
PRINT '  - SP_STG_to_INT_Proveedor';
PRINT '  - SP_STG_to_INT_Ventas';
PRINT '  - SP_STG_to_INT_Entregas';
PRINT '  - SP_Orquestador_STG_to_INT (Ejecuta todos)';
PRINT '';
PRINT 'Para ejecutar todo el proceso:';
PRINT '  EXEC SP_Orquestador_STG_to_INT;';
PRINT '';
GO  





------------------------------------------------------------------------------------------------------------

-- STORED PROCEDURES: INT -> DW (Tablas Finales)

-- SP_INT_to_DW_Dim_EstadoPedido

IF OBJECT_ID('SP_INT_to_DW_Dim_EstadoPedido', 'P') IS NOT NULL
    DROP PROCEDURE SP_INT_to_DW_Dim_EstadoPedido;
GO

CREATE PROCEDURE SP_INT_to_DW_Dim_EstadoPedido
    @ID_Proceso INT,
    @Reprocesar BIT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RegistrosActualizados INT = 0;
    DECLARE @RegistrosInsertados INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Mapeo de CodEstado a CodigoEstado
        UPDATE d
        SET 
            d.Descripcion_Estado = i.Descripcion_Estado,
            d.Tipo_Estado = i.Tipo_Estado,
            d.Orden_Secuencia = i.Orden_Secuencia,
            d.EsEstadoFinal = i.EsEstadoFinal,
            d.FechaModificacion = GETDATE()
        FROM Dim_EstadoPedido d
        INNER JOIN INT_EstadoPedido i ON d.CodigoEstado = i.CodEstado 
        WHERE d.Descripcion_Estado <> i.Descripcion_Estado
           OR d.Tipo_Estado <> i.Tipo_Estado
           OR d.Orden_Secuencia <> i.Orden_Secuencia
           OR d.EsEstadoFinal <> i.EsEstadoFinal;
        
        SET @RegistrosActualizados = @@ROWCOUNT;
        
        -- INSERT: Mapeo de CodEstado a CodigoEstado
        INSERT INTO Dim_EstadoPedido (
            CodigoEstado, Descripcion_Estado, Tipo_Estado,
            Orden_Secuencia, EsEstadoFinal, FechaCreacion, FechaModificacion
        )
        SELECT DISTINCT
            i.CodEstado, -- Origen
            i.Descripcion_Estado,
            i.Tipo_Estado,
            i.Orden_Secuencia,
            i.EsEstadoFinal,
            GETDATE(),
            GETDATE()
        FROM INT_EstadoPedido i
        LEFT JOIN Dim_EstadoPedido d ON i.CodEstado = d.CodigoEstado 
        WHERE d.ID_Estado IS NULL;
        
        SET @RegistrosInsertados = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        PRINT 'SP_INT_to_DW_Dim_EstadoPedido: ' + CAST(@RegistrosInsertados AS VARCHAR) + ' insertados, ' + CAST(@RegistrosActualizados AS VARCHAR) + ' actualizados';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO

-- =====================================================
-- SP_INT_to_DW_Dim_Almacen
IF OBJECT_ID('SP_INT_to_DW_Dim_Almacen', 'P') IS NOT NULL
    DROP PROCEDURE SP_INT_to_DW_Dim_Almacen;
GO
CREATE PROCEDURE SP_INT_to_DW_Dim_Almacen
    @ID_Proceso INT,
    @Reprocesar BIT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RegistrosActualizados INT = 0;
    DECLARE @RegistrosInsertados INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- UPDATE
        UPDATE d
        SET 
            d.NombreAlmacen = i.NombreAlmacen,
            d.Ubicacion = i.Ubicacion,
            d.Ciudad = i.Ciudad,
            d.Provincia = i.Provincia,
            d.CodigoPostal = i.CodigoPostal,
            d.TipoAlmacen = i.TipoAlmacen,
            d.Activo = i.Activo,
            d.FechaModificacion = GETDATE()
        FROM Dim_Almacen d
        INNER JOIN INT_Almacen i ON d.CodigoAlmacen = i.CodAlmacen; 
        
        SET @RegistrosActualizados = @@ROWCOUNT;
        
        -- INSERT
        INSERT INTO Dim_Almacen (
            CodigoAlmacen, NombreAlmacen, Ubicacion, Ciudad, Provincia,
            CodigoPostal, TipoAlmacen, Activo, FechaCreacion, FechaModificacion
        )
        SELECT DISTINCT
            i.CodAlmacen, -- Origen
            i.NombreAlmacen, i.Ubicacion, i.Ciudad, i.Provincia,
            i.CodigoPostal, i.TipoAlmacen, i.Activo, GETDATE(), GETDATE()
        FROM INT_Almacen i
        LEFT JOIN Dim_Almacen d ON i.CodAlmacen = d.CodigoAlmacen 
        WHERE d.ID_Almacen IS NULL;
        
        SET @RegistrosInsertados = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        PRINT 'SP_INT_to_DW_Dim_Almacen: ' + CAST(@RegistrosInsertados AS VARCHAR) + ' insertados, ' + CAST(@RegistrosActualizados AS VARCHAR) + ' actualizados';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO




-- SP_INT_to_DW_Dim_Cliente

IF OBJECT_ID('SP_INT_to_DW_Dim_Cliente', 'P') IS NOT NULL
    DROP PROCEDURE SP_INT_to_DW_Dim_Cliente;
GO

CREATE PROCEDURE SP_INT_to_DW_Dim_Cliente
    @ID_Proceso INT,
    @Reprocesar BIT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RegistrosActualizados INT = 0;
    DECLARE @RegistrosInsertados INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- UPDATE
        UPDATE d
        SET
            d.RazonSocial = i.RazonSocial,
            d.Telefono    = i.Telefono,
            d.Mail        = i.Mail,
            d.Direccion   = i.Direccion,
            d.Localidad   = i.Localidad,
            d.Provincia   = i.Provincia,
            d.CP          = i.CP
        FROM Dim_Cliente d
        INNER JOIN INT_Cliente i ON d.CodigoCliente = i.CodCliente; 

        SET @RegistrosActualizados = @@ROWCOUNT;

        -- INSERT
        INSERT INTO Dim_Cliente (
            CodigoCliente, RazonSocial, Telefono, Mail, Direccion, Localidad, Provincia, CP, FechaCreacion
        )
        SELECT
            i.CodCliente, -- Origen
            i.RazonSocial, i.Telefono, i.Mail, i.Direccion, i.Localidad, i.Provincia, i.CP, GETDATE()
        FROM INT_Cliente i
        LEFT JOIN Dim_Cliente d ON d.CodigoCliente = i.CodCliente 
        WHERE d.CodigoCliente IS NULL;

        SET @RegistrosInsertados = @@ROWCOUNT;

        COMMIT TRANSACTION;
        PRINT 'SP_INT_to_DW_Dim_Cliente: ' + CAST(@RegistrosInsertados AS VARCHAR) + ' insertados, ' + CAST(@RegistrosActualizados AS VARCHAR) + ' actualizados';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO





-- SP_INT_to_DW_Dim_Producto
IF OBJECT_ID('SP_INT_to_DW_Dim_Producto', 'P') IS NOT NULL
    DROP PROCEDURE SP_INT_to_DW_Dim_Producto;
GO

CREATE PROCEDURE SP_INT_to_DW_Dim_Producto
        @ID_Proceso INT,
        @Reprocesar BIT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RegistrosActualizados INT = 0;
    DECLARE @RegistrosInsertados INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- UPDATE
        UPDATE d
        SET 
            d.Descripcion = i.Descripcion,
            d.Categoria = i.Categoria,
            d.Marca = i.Marca,
            d.PrecioCosto = i.PrecioCosto,
            d.PrecioVentaSugerido = i.PrecioVentaSugerido
        FROM Dim_Producto d
        INNER JOIN INT_Producto i ON d.CodigoProducto = i.CodigoProducto;
        
        SET @RegistrosActualizados = @@ROWCOUNT;
        
        -- INSERT
        INSERT INTO Dim_Producto (
            CodigoProducto, Descripcion, Categoria, Marca,
            PrecioCosto, PrecioVentaSugerido, FechaCreacion
        )
        SELECT DISTINCT
            i.CodigoProducto, i.Descripcion, i.Categoria, i.Marca,
            i.PrecioCosto, i.PrecioVentaSugerido, GETDATE()
        FROM INT_Producto i
        LEFT JOIN Dim_Producto d ON i.CodigoProducto = d.CodigoProducto
        WHERE d.ID_Producto IS NULL;
        
        SET @RegistrosInsertados = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        
        PRINT 'SP_INT_to_DW_Dim_Producto: ' + 
              CAST(@RegistrosInsertados AS VARCHAR) + ' insertados, ' +
              CAST(@RegistrosActualizados AS VARCHAR) + ' actualizados';
        
        RETURN @RegistrosInsertados + @RegistrosActualizados;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT 'ERROR en SP_INT_to_DW_Dim_Producto: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO






-- SP_INT_to_DW_Dim_Tienda

IF OBJECT_ID('SP_INT_to_DW_Dim_Tienda', 'P') IS NOT NULL
    DROP PROCEDURE SP_INT_to_DW_Dim_Tienda;
GO

CREATE PROCEDURE SP_INT_to_DW_Dim_Tienda
    @ID_Proceso INT,
    @Reprocesar BIT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RegistrosActualizados INT = 0;
    DECLARE @RegistrosInsertados INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- UPDATE
        UPDATE d
        SET 
            d.Descripcion = i.Descripcion,
            d.Direccion = i.Direccion,
            d.Localidad = i.Localidad,
            d.Provincia = i.Provincia,
            d.CP = i.CP,
            d.TipoTienda = i.TipoTienda
        FROM Dim_Tienda d
        INNER JOIN INT_Tienda i ON d.CodigoTienda = i.CodigoTienda;
        
        SET @RegistrosActualizados = @@ROWCOUNT;
        
        -- INSERT
        INSERT INTO Dim_Tienda (
            CodigoTienda, Descripcion, Direccion, Localidad,
            Provincia, CP, TipoTienda, FechaCreacion
        )
        SELECT DISTINCT
            i.CodigoTienda, i.Descripcion, i.Direccion, i.Localidad,
            i.Provincia, i.CP, i.TipoTienda, GETDATE()
        FROM INT_Tienda i
        LEFT JOIN Dim_Tienda d ON i.CodigoTienda = d.CodigoTienda
        WHERE d.ID_Tienda IS NULL;
        
        SET @RegistrosInsertados = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        
        PRINT 'SP_INT_to_DW_Dim_Tienda: ' + 
              CAST(@RegistrosInsertados AS VARCHAR) + ' insertados, ' +
              CAST(@RegistrosActualizados AS VARCHAR) + ' actualizados';
        
        RETURN @RegistrosInsertados + @RegistrosActualizados;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        PRINT 'ERROR en SP_INT_to_DW_Dim_Tienda: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO






-- SP_INT_to_DW_Dim_Proveedor
CREATE OR ALTER PROCEDURE SP_INT_to_DW_Dim_Proveedor
    @ID_Proceso INT,
    @Reprocesar BIT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RegistrosActualizados INT = 0;
    DECLARE @RegistrosInsertados INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- UPDATE: Mapeo CodigoProveedor (Dim) <- CodProveedor (INT)
        UPDATE d
        SET 
            d.NombreProveedor = i.NombreProveedor,
            d.TipoServicio = i.TipoServicio,
            d.Activo = i.Activo,
            d.FechaModificacion = GETDATE()
        FROM Dim_Proveedor d
        INNER JOIN INT_Proveedor i ON d.CodigoProveedor = i.CodProveedor;
        
        SET @RegistrosActualizados = @@ROWCOUNT;
        
        -- INSERT: Solo nuevos
        INSERT INTO Dim_Proveedor (
            CodigoProveedor, NombreProveedor, TipoServicio, Activo,
            FechaCreacion, FechaModificacion
        )
        SELECT DISTINCT
            i.CodProveedor, i.NombreProveedor, i.TipoServicio, i.Activo,
            GETDATE(), GETDATE()
        FROM INT_Proveedor i
        LEFT JOIN Dim_Proveedor d ON i.CodProveedor = d.CodigoProveedor
        WHERE d.ID_Proveedor IS NULL;
        
        SET @RegistrosInsertados = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        PRINT 'SP_INT_to_DW_Dim_Proveedor: ' + CAST(@RegistrosInsertados AS VARCHAR) + ' insertados';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
GO




-- SP_INT_to_DW_Fact_Ventas 

IF OBJECT_ID('SP_INT_to_DW_Fact_Ventas', 'P') IS NOT NULL
    DROP PROCEDURE SP_INT_to_DW_Fact_Ventas;
GO

CREATE PROCEDURE SP_INT_to_DW_Fact_Ventas
    @ID_Proceso INT = NULL,
    @Reprocesar BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RegistrosInsertados INT = 0;
    DECLARE @RegistrosEliminados INT = 0;
    DECLARE @RegistrosRechazados INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- REPROCESO: Eliminar ventas del período
        IF @Reprocesar = 1
        BEGIN
            DELETE fv
            FROM Fact_Ventas fv
            WHERE EXISTS (
                SELECT 1 FROM INT_Ventas iv
                INNER JOIN Dim_Tiempo dt ON dt.Fecha = iv.FechaVenta
                WHERE fv.Tiempo_Key = dt.Tiempo_Key
            );

            SET @RegistrosEliminados = @@ROWCOUNT;
            PRINT '   Registros eliminados para reproceso: ' + CAST(@RegistrosEliminados AS VARCHAR);
        END


        INSERT INTO Fact_Ventas (
            Tiempo_Key,
            ID_Producto,
            ID_Cliente,
            ID_Tienda,
            Cantidad,
            PrecioVenta,
            Total_IVA,
            FechaCarga
        )
        SELECT 
            dt.Tiempo_Key,
            dp.ID_Producto,
            dc.ID_Cliente,
            dtie.ID_Tienda,
            iv.Cantidad,
            iv.PrecioVenta,
            iv.Total_IVA,
            GETDATE()
        FROM INT_Ventas iv
        

        INNER JOIN Dim_Tiempo dt 
            ON dt.Fecha = iv.FechaVenta
        
        INNER JOIN Dim_Producto dp 
            ON LTRIM(RTRIM(iv.CodigoProducto)) = LTRIM(RTRIM(dp.CodigoProducto))
        
        INNER JOIN Dim_Cliente dc 
            ON LTRIM(RTRIM(iv.CodigoCliente)) = LTRIM(RTRIM(dc.CodigoCliente))
        
        INNER JOIN Dim_Tienda dtie 
            ON LTRIM(RTRIM(iv.CodigoTienda)) = LTRIM(RTRIM(dtie.CodigoTienda))
        
        WHERE @Reprocesar = 1
           OR NOT EXISTS (
                SELECT 1
                FROM Fact_Ventas fv
                WHERE fv.Tiempo_Key = dt.Tiempo_Key
                  AND fv.ID_Producto = dp.ID_Producto
                  AND fv.ID_Cliente = dc.ID_Cliente
                  AND fv.ID_Tienda = dtie.ID_Tienda
                  AND fv.Cantidad = iv.Cantidad
                  AND fv.PrecioVenta = iv.PrecioVenta
           );

        SET @RegistrosInsertados = @@ROWCOUNT;

       
        -- REGISTROS RECHAZADOS
        
        INSERT INTO ETL_Registros_Rechazados (
            ID_Proceso,
            Tabla_Origen,
            Registro_Original,
            Motivo_Rechazo
        )
        SELECT
            @ID_Proceso,
            'INT_Ventas',
            'Fecha=' + CONVERT(VARCHAR, iv.FechaVenta, 23) +
                ', Producto=' + iv.CodigoProducto +
                ', Cliente=' + iv.CodigoCliente +
                ', Tienda=' + iv.CodigoTienda,
            CASE
                WHEN dt.Tiempo_Key IS NULL 
                    THEN 'Fecha [' + CONVERT(VARCHAR, iv.FechaVenta, 23) + '] no existe en Dim_Tiempo'
                WHEN dp.ID_Producto IS NULL 
                    THEN 'Producto [' + iv.CodigoProducto + '] no existe en Dim_Producto'
                WHEN dc.ID_Cliente IS NULL 
                    THEN 'Cliente [' + iv.CodigoCliente + '] no existe en Dim_Cliente'
                WHEN dtie.ID_Tienda IS NULL 
                    THEN 'Tienda [' + iv.CodigoTienda + '] no existe en Dim_Tienda'
                ELSE 'Error desconocido'
            END
        FROM INT_Ventas iv
        LEFT JOIN Dim_Tiempo dt 
            ON dt.Fecha = iv.FechaVenta
        LEFT JOIN Dim_Producto dp 
            ON LTRIM(RTRIM(iv.CodigoProducto)) = LTRIM(RTRIM(dp.CodigoProducto))
        LEFT JOIN Dim_Cliente dc 
            ON LTRIM(RTRIM(iv.CodigoCliente)) = LTRIM(RTRIM(dc.CodigoCliente))
        LEFT JOIN Dim_Tienda dtie 
            ON LTRIM(RTRIM(iv.CodigoTienda)) = LTRIM(RTRIM(dtie.CodigoTienda))
        WHERE dt.Tiempo_Key IS NULL
           OR dp.ID_Producto IS NULL
           OR dc.ID_Cliente IS NULL
           OR dtie.ID_Tienda IS NULL;

        SET @RegistrosRechazados = @@ROWCOUNT;

        COMMIT TRANSACTION;

        PRINT 'SP_INT_to_DW_Fact_Ventas: ' +
              CAST(@RegistrosInsertados AS VARCHAR) + ' insertados, ' +
              CAST(@RegistrosRechazados AS VARCHAR) + ' rechazados';

        RETURN @RegistrosInsertados;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        PRINT 'ERROR en SP_INT_to_DW_Fact_Ventas: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END;
GO

PRINT 'SP_INT_to_DW_Fact_Ventas corregido';
GO





CREATE OR ALTER PROCEDURE SP_INT_to_DW_Fact_Entregas
    @ID_Proceso INT = NULL,
    @Reprocesar BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Insertados INT = 0;
    DECLARE @Rechazados INT = 0;

    BEGIN TRY
        BEGIN TRANSACTION;

        
        -- 1. REPROCESO CONTROLADO
        
        IF @Reprocesar = 1
        BEGIN
            DELETE FROM Fact_Entregas;
        END

        
        -- 2. INSERTAR ENTREGAS VÁLIDAS
        
        INSERT INTO Fact_Entregas (
            CodigoEntrega,
            ID_Venta,
            Tiempo_Key_Envio,
            Tiempo_Key_Entrega,
            ID_Proveedor,
            ID_Almacen,
            ID_Estado,
            ID_Cliente,
            ID_Tienda,
            CantidadProductos,
            PesoKg,
            VolumenM3,
            DistanciaKm,
            CostoEntrega,
            FechaEstimadaEntrega,
            FechaCarga
        )
        SELECT
            CAST(ie.CodEntrega AS VARCHAR(50)),
            fv.ID_Venta,
            dt_env.Tiempo_Key,
            dt_ent.Tiempo_Key,
            ISNULL(dp.ID_Proveedor, -1),
            ISNULL(da.ID_Almacen, -1),
            ISNULL(de.ID_Estado, -1),
            fv.ID_Cliente,
            fv.ID_Tienda,
            1,
            0, 0, 0,
            0,
            DATEADD(
                DAY,
                5,
                CONVERT(DATE, CONVERT(CHAR(8), fv.Tiempo_Key))
            ),
            GETDATE()
        FROM INT_Entregas ie
        INNER JOIN Fact_Ventas fv
            ON CAST(ie.CodVenta AS INT) = fv.ID_Venta
        LEFT JOIN Dim_Tiempo dt_env
            ON CAST(ie.Fecha_Envio AS DATE) = dt_env.Fecha
        LEFT JOIN Dim_Tiempo dt_ent
            ON CAST(ie.Fecha_Entrega AS DATE) = dt_ent.Fecha
        LEFT JOIN Dim_Proveedor dp
            ON CAST(ie.CodProveedor AS INT) = dp.CodigoProveedor
        LEFT JOIN Dim_Almacen da
            ON CAST(ie.CodAlmacen AS INT) = da.CodigoAlmacen
        LEFT JOIN Dim_EstadoPedido de
            ON CAST(ie.CodEstado AS INT) = de.CodigoEstado
        WHERE
            NOT EXISTS (
                SELECT 1
                FROM Fact_Entregas fe
                WHERE fe.CodigoEntrega = CAST(ie.CodEntrega AS VARCHAR(50))
            );

        SET @Insertados = @@ROWCOUNT;

       
        -- 3. REGISTRAR RECHAZOS REALES
       
        INSERT INTO ETL_Registros_Rechazados (
            ID_Proceso,
            Tabla_Origen,
            Registro_Original,
            Motivo_Rechazo,
            Fecha_Rechazo
        )
        SELECT
            @ID_Proceso,
            'INT_Entregas',
            'CodEntrega=' + CAST(ie.CodEntrega AS VARCHAR),
            CASE
                WHEN fv.ID_Venta IS NULL THEN 'Venta inexistente'
                WHEN dt_ent.Tiempo_Key IS NULL THEN 'Fecha de entrega inválida'
                ELSE 'Error desconocido'
            END,
            GETDATE()
        FROM INT_Entregas ie
        LEFT JOIN Fact_Ventas fv
            ON CAST(ie.CodVenta AS INT) = fv.ID_Venta
        LEFT JOIN Dim_Tiempo dt_ent
            ON CAST(ie.Fecha_Entrega AS DATE) = dt_ent.Fecha
        WHERE fv.ID_Venta IS NULL
           OR dt_ent.Tiempo_Key IS NULL;

        SET @Rechazados = @@ROWCOUNT;

        COMMIT TRANSACTION;

        PRINT 'Fact_Entregas -> Insertados: ' + CAST(@Insertados AS VARCHAR);
        PRINT 'Fact_Entregas -> Rechazados: ' + CAST(@Rechazados AS VARCHAR);

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        THROW;
    END CATCH
END;
GO





-- SP ORQUESTADOR COMPLETO: INT -> DW
-- Incluye inicialización automática de Dim_Tiempo


USE DataShop;
GO

IF OBJECT_ID('SP_Orquestador_INT_to_DW', 'P') IS NOT NULL
    DROP PROCEDURE SP_Orquestador_INT_to_DW;
GO

CREATE PROCEDURE SP_Orquestador_INT_to_DW
    @Reprocesar BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ID_Proceso INT;
    DECLARE @FechaInicio DATETIME = GETDATE();
    DECLARE @CountTiempo INT;
    DECLARE @CountVentas INT;
    DECLARE @CountEntregas INT;
    DECLARE @TotalRechazados INT;

    BEGIN TRY
        
        -- Registrar inicio del proceso
        
        INSERT INTO ETL_Control_Procesos (
            Nombre_Proceso,
            Fecha_Inicio,
            Estado
        )
        VALUES (
            'INT_to_DW_Completo',
            @FechaInicio,
            'EN_PROCESO'
        );

        SET @ID_Proceso = SCOPE_IDENTITY();

        PRINT '';
        PRINT '========================================';
        PRINT 'INICIANDO PROCESO ETL COMPLETO';
        PRINT 'ID Proceso: ' + CAST(@ID_Proceso AS VARCHAR);
        PRINT 'Reprocesar: ' + CAST(@Reprocesar AS VARCHAR);
        PRINT '========================================';
        PRINT '';

        
        -- INICIALIZACIÓN: Poblar Dim_Tiempo si está vacía
        
        SELECT @CountTiempo = COUNT(*) FROM Dim_Tiempo;
        
        IF @CountTiempo = 0
        BEGIN
            PRINT 'INICIALIZACIÓN: Dim_Tiempo está vacía';
            PRINT 'Poblando Dim_Tiempo (2020-2030)...';
            
            EXEC Sp_Genera_Dim_Tiempo 
                @FechaInicio = '2020-01-01', 
                @FechaFin = '2030-12-31';
            
            SELECT @CountTiempo = COUNT(*) FROM Dim_Tiempo;
            PRINT 'Dim_Tiempo poblada con ' + CAST(@CountTiempo AS VARCHAR) + ' registros';
        END
        ELSE
        BEGIN
            PRINT 'Dim_Tiempo contiene ' + CAST(@CountTiempo AS VARCHAR) + ' registros';
        END
        PRINT '';

        
        -- VERIFICACIÓN: Tablas INT tienen datos
        
        DECLARE @INTVentas INT, @INTEntregas INT;
        SELECT @INTVentas = COUNT(*) FROM INT_Ventas;
        SELECT @INTEntregas = COUNT(*) FROM INT_Entregas;
        
        PRINT 'VERIFICACIÓN DE DATOS:';
        PRINT '  INT_Ventas: ' + CAST(@INTVentas AS VARCHAR) + ' registros';
        PRINT '  INT_Entregas: ' + CAST(@INTEntregas AS VARCHAR) + ' registros';
        
        IF @INTVentas = 0 AND @INTEntregas = 0
        BEGIN
            PRINT '';
            PRINT 'ADVERTENCIA: Tablas INT están vacías';
            PRINT 'Ejecute primero: SP_Orquestador_STG_to_INT';
            PRINT '';
        END
        PRINT '';

        
        -- PASO 1: CARGA DE DIMENSIONES
        
        PRINT 'PASO 1: Cargando Dimensiones...';
        PRINT '-----------------------------------';
        
        EXEC SP_INT_to_DW_Dim_EstadoPedido  @ID_Proceso, @Reprocesar;
        EXEC SP_INT_to_DW_Dim_Almacen       @ID_Proceso, @Reprocesar;
        EXEC SP_INT_to_DW_Dim_Cliente       @ID_Proceso, @Reprocesar;
        EXEC SP_INT_to_DW_Dim_Producto      @ID_Proceso, @Reprocesar;
        EXEC SP_INT_to_DW_Dim_Tienda        @ID_Proceso, @Reprocesar;
        EXEC SP_INT_to_DW_Dim_Proveedor     @ID_Proceso, @Reprocesar;
        PRINT '';

        
        -- PASO 2: CARGA DE HECHOS
       
        PRINT 'PASO 2: Cargando Tablas de Hechos...';
        PRINT '-----------------------------------';
        
        EXEC SP_INT_to_DW_Fact_Ventas       @ID_Proceso, @Reprocesar;
        EXEC SP_INT_to_DW_Fact_Entregas     @ID_Proceso, @Reprocesar;
        PRINT '';

        
        -- PASO 3: Actualizar proceso como completado
       
        -- Calcular métricas ANTES del UPDATE 
        SELECT @CountVentas = COUNT(*) FROM Fact_Ventas;
        SELECT @CountEntregas = COUNT(*) FROM Fact_Entregas;
        SELECT @TotalRechazados = COUNT(*) 
        FROM ETL_Registros_Rechazados 
        WHERE ID_Proceso = @ID_Proceso;
        
        UPDATE ETL_Control_Procesos
        SET
            Fecha_Fin = GETDATE(),
            Estado = 'COMPLETADO',
            Registros_Procesados = @CountVentas + @CountEntregas,
            Registros_Rechazados = @TotalRechazados
        WHERE ID_Proceso = @ID_Proceso;

        PRINT '========================================';
        PRINT 'PROCESO ETL COMPLETADO EXITOSAMENTE';
        PRINT 'Duración: ' +
              CAST(DATEDIFF(SECOND, @FechaInicio, GETDATE()) AS VARCHAR) +
              ' segundos';
        PRINT '========================================';
        PRINT '';
        
        -- Mostrar resumen
        PRINT 'RESUMEN FINAL:';
        PRINT '  Fact_Ventas   : ' + CAST(@CountVentas AS VARCHAR) + ' registros';
        PRINT '  Fact_Entregas : ' + CAST(@CountEntregas AS VARCHAR) + ' registros';
        PRINT '  Rechazados    : ' + CAST(@TotalRechazados AS VARCHAR) + ' registros';
        PRINT '';

    END TRY
    BEGIN CATCH
        
        IF @ID_Proceso IS NOT NULL
        BEGIN
            UPDATE ETL_Control_Procesos
            SET
                Fecha_Fin = GETDATE(),
                Estado = 'ERROR',
                Mensaje_Error = ERROR_MESSAGE()
            WHERE ID_Proceso = @ID_Proceso;
        END

        PRINT '';
        PRINT '========================================';
        PRINT 'ERROR CRÍTICO EN ETL';
        PRINT '========================================';
        PRINT 'Mensaje: ' + ERROR_MESSAGE();
        PRINT 'Línea: ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT 'Procedimiento: ' + ISNULL(ERROR_PROCEDURE(), 'Main');
        PRINT '========================================';
        PRINT '';
        
        THROW;
    END CATCH
END;
GO
