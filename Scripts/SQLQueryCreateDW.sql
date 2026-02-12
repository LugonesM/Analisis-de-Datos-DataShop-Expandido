-- SCRIPT DEL DATA WAREHOUSE
SET NOCOUNT ON;
SET XACT_ABORT ON; 

-- Verificar y crear la base de datos si no existe
IF NOT EXISTS(SELECT name FROM sys.databases WHERE name = 'DataShop')
BEGIN
    CREATE DATABASE DataShop;
    PRINT 'Base de datos DataShop creada exitosamente';
END
ELSE
    PRINT 'Base de datos DataShop ya existe';
GO

USE DataShop;
GO

PRINT '1. Iniciando eliminación de objetos existentes...';
GO

-- Eliminación en orden correcto considerando dependencias
IF OBJECT_ID('Fact_Entregas', 'U') IS NOT NULL
BEGIN
    DROP TABLE Fact_Entregas;
    PRINT 'Tabla Fact_Entregas eliminada.';
END
GO

IF OBJECT_ID('Fact_Ventas', 'U') IS NOT NULL
BEGIN
    DROP TABLE Fact_Ventas;
    PRINT 'Tabla Fact_Ventas eliminada.';
END
GO

-- Eliminar procedures
IF OBJECT_ID('[dbo].[Sp_Genera_Dim_Tiempo]') IS NOT NULL
BEGIN
    DROP PROCEDURE [dbo].[Sp_Genera_Dim_Tiempo];
    PRINT 'Stored Procedure Sp_Genera_Dim_Tiempo eliminado.';
END
GO

IF OBJECT_ID('[dbo].[usp_Poblar_Dimension_Tiempo]') IS NOT NULL
BEGIN
    DROP PROCEDURE [dbo].[usp_Poblar_Dimension_Tiempo];
    PRINT 'Stored Procedure usp_Poblar_Dimension_Tiempo eliminado.';
END
GO

-- Eliminar tablas de dimensión
IF OBJECT_ID('Dim_Almacen', 'U') IS NOT NULL
BEGIN
    DROP TABLE Dim_Almacen;
    PRINT 'Tabla Dim_Almacen eliminada.';
END
GO

IF OBJECT_ID('Dim_Proveedor', 'U') IS NOT NULL
BEGIN
    DROP TABLE Dim_Proveedor;
    PRINT 'Tabla Dim_Proveedor eliminada.';
END
GO

IF OBJECT_ID('Dim_EstadoPedido', 'U') IS NOT NULL
BEGIN
    DROP TABLE Dim_EstadoPedido;
    PRINT 'Tabla Dim_EstadoPedido eliminada.';
END
GO

IF OBJECT_ID('Dim_Tienda', 'U') IS NOT NULL
BEGIN
    DROP TABLE Dim_Tienda;
    PRINT 'Tabla Dim_Tienda eliminada.';
END
GO

IF OBJECT_ID('Dim_Cliente', 'U') IS NOT NULL
BEGIN
    DROP TABLE Dim_Cliente;
    PRINT 'Tabla Dim_Cliente eliminada.';
END
GO

IF OBJECT_ID('Dim_Producto', 'U') IS NOT NULL
BEGIN
    DROP TABLE Dim_Producto;
    PRINT 'Tabla Dim_Producto eliminada.';
END
GO

IF OBJECT_ID('Dim_Tiempo', 'U') IS NOT NULL
BEGIN
    DROP TABLE Dim_Tiempo;
    PRINT 'Tabla Dim_Tiempo eliminada.';
END
GO

-- CREACIÓN DE TABLAS DE DIMENSIÓN
PRINT '2. Creando tablas de dimensión...';
GO

-- Dimensión TIEMPO
CREATE TABLE Dim_Tiempo (
    -- Clave 
    Tiempo_Key INT NOT NULL PRIMARY KEY,   -- YYYYMMDD
    -- Fecha real
    Fecha DATE NOT NULL UNIQUE,
    -- Atributos básicos
    Anio SMALLINT NOT NULL,
    Mes TINYINT NOT NULL,
    Dia TINYINT NOT NULL,
    -- Atributos de mes
    Mes_Nombre VARCHAR(20) NOT NULL,
    Mes_Nombre_Corto CHAR(3) NOT NULL,
    Mes_Anio CHAR(7) NOT NULL,              -- '2025-01'
    -- Atributos de semana 
    Semana_ISO TINYINT NOT NULL,
    Anio_ISO SMALLINT NOT NULL,
    Dia_Semana_ISO TINYINT NOT NULL,        -- 1=Lunes … 7=Domingo
    Dia_Nombre VARCHAR(20) NOT NULL,
    Es_Fin_Semana BIT NOT NULL,
    -- Atributos de período
    Trimestre TINYINT NOT NULL,
    Trimestre_Nombre CHAR(2) NOT NULL,      -- Q1..Q4
    Semestre TINYINT NOT NULL,
    -- Flags útiles BI
    Es_Feriado BIT NOT NULL DEFAULT 0,
    Es_Dia_Laboral BIT NOT NULL
);
GO

-- Índices para BI
CREATE INDEX IX_DimTiempo_AnioMes ON Dim_Tiempo (Anio, Mes);
CREATE INDEX IX_DimTiempo_Fecha ON Dim_Tiempo (Fecha);
GO

-- Dimensión PRODUCTO
CREATE TABLE Dim_Producto (
    ID_Producto INT PRIMARY KEY IDENTITY(1,1),
    CodigoProducto VARCHAR(100) NOT NULL UNIQUE,
    Descripcion VARCHAR(255) NOT NULL,
    Categoria VARCHAR(100) NOT NULL,
    Marca VARCHAR(100) NOT NULL,
    PrecioCosto DECIMAL(18,2) NOT NULL DEFAULT 0,
    PrecioVentaSugerido DECIMAL(18,2) NOT NULL DEFAULT 0,
    FechaCreacion DATETIME DEFAULT GETDATE()
);

CREATE INDEX IDX_CodigoProducto ON Dim_Producto (CodigoProducto);
PRINT 'Tabla Dim_Producto creada.';
GO

-- Dimensión CLIENTE
CREATE TABLE Dim_Cliente (
    ID_Cliente INT PRIMARY KEY IDENTITY(1,1),
    CodigoCliente VARCHAR(50) NOT NULL UNIQUE,
    RazonSocial VARCHAR(255) NOT NULL,
    Telefono VARCHAR(50) NULL,
    Mail VARCHAR(255) NULL,
    Direccion VARCHAR(255) NULL,
    Localidad VARCHAR(100) NOT NULL,
    Provincia VARCHAR(100) NOT NULL,
    CP VARCHAR(20) NULL,
    FechaCreacion DATETIME DEFAULT GETDATE()
);

-- Índice
CREATE INDEX IDX_CodigoCliente ON Dim_Cliente (CodigoCliente);
PRINT 'Tabla Dim_Cliente creada.';
GO

-- Dimensión TIENDA
CREATE TABLE Dim_Tienda (
    ID_Tienda INT PRIMARY KEY IDENTITY(1,1),
    CodigoTienda VARCHAR(50) NOT NULL UNIQUE,
    Descripcion VARCHAR(255) NOT NULL,
    Direccion VARCHAR(255) NULL,
    Localidad VARCHAR(100) NOT NULL,
    Provincia VARCHAR(100) NOT NULL,
    CP VARCHAR(20) NULL,
    TipoTienda VARCHAR(50) NOT NULL,
    FechaCreacion DATETIME DEFAULT GETDATE()
);

CREATE INDEX IDX_CodigoTienda ON Dim_Tienda (CodigoTienda);
PRINT 'Tabla Dim_Tienda creada.';
GO

-- Dimensión ESTADO DEL PEDIDO
CREATE TABLE Dim_EstadoPedido (
    ID_Estado INT PRIMARY KEY IDENTITY(1,1),
    CodigoEstado VARCHAR(20) NOT NULL UNIQUE,
    Descripcion_Estado VARCHAR(100) NOT NULL,
    Tipo_Estado VARCHAR(50) NOT NULL, -- 'En Proceso', 'Completado', 'Cancelado'
    Orden_Secuencia INT NOT NULL, -- Para ordenar lógicamente los estados
    EsEstadoFinal BIT DEFAULT 0, -- Indica si es un estado terminal
    FechaCreacion DATETIME DEFAULT GETDATE(),
    FechaModificacion DATETIME DEFAULT GETDATE()
);

-- Índice 
CREATE INDEX IDX_CodigoEstado ON Dim_EstadoPedido (CodigoEstado);
CREATE INDEX IDX_TipoEstado ON Dim_EstadoPedido (Tipo_Estado);
PRINT '   - Dim_EstadoPedido creada.';
GO

-- Dimensión PROVEEDOR 
CREATE TABLE Dim_Proveedor (
    ID_Proveedor INT PRIMARY KEY IDENTITY(1,1),
    CodigoProveedor VARCHAR(50) NOT NULL UNIQUE,
    NombreProveedor VARCHAR(255) NOT NULL,
    TipoServicio VARCHAR(100) NULL, -- 'Express', 'Estándar', 'Económico'
    Telefono VARCHAR(50) NULL,
    Mail VARCHAR(255) NULL,
    CostoPromedioPorKm DECIMAL(10,4) NULL, -- Para cálculos de costo
    CalificacionPromedio DECIMAL(3,2) NULL, -- 0-5 estrellas
    TiempoPromedioEntrega INT NULL, -- En días
    Activo BIT DEFAULT 1,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    FechaModificacion DATETIME DEFAULT GETDATE()
);

CREATE INDEX IDX_CodigoProveedor ON Dim_Proveedor (CodigoProveedor);
CREATE INDEX IDX_TipoServicio ON Dim_Proveedor (TipoServicio);
CREATE INDEX IDX_Activo ON Dim_Proveedor (Activo);
PRINT '   - Dim_Proveedor creada.';
GO

-- Dimensión ALMACÉN
CREATE TABLE Dim_Almacen (
    ID_Almacen INT PRIMARY KEY IDENTITY(1,1),
    CodigoAlmacen VARCHAR(50) NOT NULL UNIQUE,
    NombreAlmacen VARCHAR(255) NOT NULL,
    Ubicacion VARCHAR(255) NOT NULL,
    Ciudad VARCHAR(100) NULL,
    Provincia VARCHAR(100) NULL,
    CodigoPostal VARCHAR(20) NULL,
    CapacidadM3 DECIMAL(12,2) NULL, -- Capacidad en metros cúbicos
    TipoAlmacen VARCHAR(50) NULL, -- 'Principal', 'Secundario', 'Temporal'
    Activo BIT DEFAULT 1,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    FechaModificacion DATETIME DEFAULT GETDATE()
);

CREATE INDEX IDX_CodigoAlmacen ON Dim_Almacen (CodigoAlmacen);
CREATE INDEX IDX_Ubicacion ON Dim_Almacen (Provincia, Ciudad);
CREATE INDEX IDX_TipoAlmacen ON Dim_Almacen (TipoAlmacen);
PRINT '   - Dim_Almacen creada.';
GO

-- CREACIÓN DE LA TABLA DE HECHOS
PRINT '3. Creando Fact_Ventas...';
GO

CREATE TABLE Fact_Ventas (
    ID_Venta BIGINT PRIMARY KEY IDENTITY(1,1),
    Tiempo_Key INT NOT NULL,
    ID_Producto INT NOT NULL,
    ID_Cliente INT NOT NULL,
    ID_Tienda INT NOT NULL,
    Cantidad INT NOT NULL CHECK (Cantidad > 0),
    PrecioVenta DECIMAL(18,2) NOT NULL CHECK (PrecioVenta >= 0),
    Total_IVA DECIMAL(18,2) NOT NULL,
    FechaCarga DATETIME DEFAULT GETDATE(),

    CONSTRAINT FK_Ventas_Tiempo FOREIGN KEY (Tiempo_Key) 
        REFERENCES Dim_Tiempo(Tiempo_Key),
    CONSTRAINT FK_Ventas_Producto FOREIGN KEY (ID_Producto) 
        REFERENCES Dim_Producto(ID_Producto),
    CONSTRAINT FK_Ventas_Cliente FOREIGN KEY (ID_Cliente) 
        REFERENCES Dim_Cliente(ID_Cliente),
    CONSTRAINT FK_Ventas_Tienda FOREIGN KEY (ID_Tienda) 
        REFERENCES Dim_Tienda(ID_Tienda)
);

CREATE INDEX IDX_Fecha ON Fact_Ventas (Tiempo_Key);
CREATE INDEX IDX_Producto ON Fact_Ventas (ID_Producto);
CREATE INDEX IDX_Cliente ON Fact_Ventas (ID_Cliente);
CREATE INDEX IDX_Tienda ON Fact_Ventas (ID_Tienda);
PRINT 'Tabla Fact_Ventas creada.';
GO

PRINT '4. Creando Fact_Entregas...';
GO

CREATE TABLE Fact_Entregas (
    ID_Entrega BIGINT PRIMARY KEY IDENTITY(1,1),
    
    -- Código natural de la entrega
    CodigoEntrega VARCHAR(50) NOT NULL UNIQUE,
    
    -- Relación con Fact_Ventas (permite múltiples entregas por venta)
    ID_Venta BIGINT NOT NULL,
    
    -- Tiempo_Key
    Tiempo_Key_Envio INT NOT NULL,
    Tiempo_Key_Entrega INT NULL,
    
    ID_Proveedor INT NOT NULL,
    ID_Almacen INT NOT NULL,
    ID_Estado INT NOT NULL,
    ID_Cliente INT NOT NULL,
    ID_Tienda INT NOT NULL,
    
    -- Métricas de la entrega
    CantidadProductos INT NOT NULL CHECK (CantidadProductos > 0),
    PesoKg DECIMAL(10,2) NULL,
    VolumenM3 DECIMAL(10,3) NULL,
    DistanciaKm DECIMAL(10,2) NULL,
    CostoEntrega DECIMAL(18,2) NOT NULL DEFAULT 0,
    
    -- Fecha estimada
    FechaEstimadaEntrega DATE NULL,
    
    -- Observaciones
    Observaciones VARCHAR(500) NULL,
    
    -- Auditoría
    FechaCarga DATETIME DEFAULT GETDATE(),
    FechaActualizacion DATETIME DEFAULT GETDATE(),
    
    -- Constraints de integridad referencial
    CONSTRAINT FK_Entregas_Ventas FOREIGN KEY (ID_Venta) 
        REFERENCES Fact_Ventas(ID_Venta),
    CONSTRAINT FK_Entregas_TiempoEnvio FOREIGN KEY (Tiempo_Key_Envio) 
        REFERENCES Dim_Tiempo(Tiempo_Key),
    CONSTRAINT FK_Entregas_TiempoEntrega FOREIGN KEY (Tiempo_Key_Entrega) 
        REFERENCES Dim_Tiempo(Tiempo_Key),
    CONSTRAINT FK_Entregas_Proveedor FOREIGN KEY (ID_Proveedor) 
        REFERENCES Dim_Proveedor(ID_Proveedor),
    CONSTRAINT FK_Entregas_Almacen FOREIGN KEY (ID_Almacen) 
        REFERENCES Dim_Almacen(ID_Almacen),
    CONSTRAINT FK_Entregas_Estado FOREIGN KEY (ID_Estado) 
        REFERENCES Dim_EstadoPedido(ID_Estado),
    CONSTRAINT FK_Entregas_Cliente FOREIGN KEY (ID_Cliente) 
        REFERENCES Dim_Cliente(ID_Cliente),
    CONSTRAINT FK_Entregas_Tienda FOREIGN KEY (ID_Tienda) 
        REFERENCES Dim_Tienda(ID_Tienda)
);

-- Índices para optimizar consultas
CREATE INDEX IDX_Entregas_Venta ON Fact_Entregas (ID_Venta);
CREATE INDEX IDX_Entregas_FechaEnvio ON Fact_Entregas (Tiempo_Key_Envio);
CREATE INDEX IDX_Entregas_FechaEntrega ON Fact_Entregas (Tiempo_Key_Entrega);
CREATE INDEX IDX_Entregas_Proveedor ON Fact_Entregas (ID_Proveedor);
CREATE INDEX IDX_Entregas_Almacen ON Fact_Entregas (ID_Almacen);
CREATE INDEX IDX_Entregas_Estado ON Fact_Entregas (ID_Estado);
CREATE INDEX IDX_Entregas_Cliente ON Fact_Entregas (ID_Cliente);

-- Índice compuesto para análisis por proveedor y fecha
CREATE INDEX IDX_Entregas_ProveedorFecha ON Fact_Entregas (ID_Proveedor, Tiempo_Key_Envio);

PRINT 'Tabla Fact_Entregas creada.';
GO

-- CREACIÓN DEL STORED PROCEDURE DIM TIEMPO CORREGIDO
PRINT '5. Creando Stored Procedure Sp_Genera_Dim_Tiempo...';
GO

IF OBJECT_ID('[dbo].[Sp_Genera_Dim_Tiempo]') IS NOT NULL
    DROP PROCEDURE [dbo].[Sp_Genera_Dim_Tiempo];
GO

CREATE PROCEDURE dbo.Sp_Genera_Dim_Tiempo
    @FechaInicio DATE,
    @FechaFin DATE
AS
BEGIN
    SET NOCOUNT ON;
    SET DATEFIRST 1;  -- Lunes

    ;WITH N AS (
        SELECT TOP (DATEDIFF(DAY, @FechaInicio, @FechaFin) + 1)
               ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM sys.objects
    ),
    Fechas AS (
        SELECT DATEADD(DAY, n, @FechaInicio) AS Fecha
        FROM N
    )
    INSERT INTO Dim_Tiempo (
        Tiempo_Key,
        Fecha,
        Anio,
        Mes,
        Dia,
        Mes_Nombre,
        Mes_Nombre_Corto,
        Mes_Anio,
        Semana_ISO,
        Anio_ISO,
        Dia_Semana_ISO,
        Dia_Nombre,
        Es_Fin_Semana,
        Trimestre,
        Trimestre_Nombre,
        Semestre,
        Es_Dia_Laboral
    )
    SELECT
        Tiempo_Key = YEAR(Fecha) * 10000 + MONTH(Fecha) * 100 + DAY(Fecha),
        Fecha,
        YEAR(Fecha),
        MONTH(Fecha),
        DAY(Fecha),
        DATENAME(MONTH, Fecha),
        LEFT(DATENAME(MONTH, Fecha), 3),
        CONVERT(CHAR(7), Fecha, 120),
        DATEPART(WEEK, Fecha),
        YEAR(Fecha),
        CASE 
            WHEN DATEPART(WEEKDAY, Fecha) = 1 THEN 7
            ELSE DATEPART(WEEKDAY, Fecha) - 1
        END,
        DATENAME(WEEKDAY, Fecha),
        CASE WHEN DATEPART(WEEKDAY, Fecha) IN (6,7) THEN 1 ELSE 0 END,
        DATEPART(QUARTER, Fecha),
        'Q' + CAST(DATEPART(QUARTER, Fecha) AS CHAR(1)),
        CASE WHEN MONTH(Fecha) <= 6 THEN 1 ELSE 2 END,
        CASE WHEN DATEPART(WEEKDAY, Fecha) IN (6,7) THEN 0 ELSE 1 END
    FROM Fechas
    WHERE NOT EXISTS (
        SELECT 1 FROM Dim_Tiempo d WHERE d.Fecha = Fechas.Fecha
    );
    
    PRINT 'Dimensión Tiempo poblada exitosamente.';
    PRINT 'Rango: ' + CAST(@FechaInicio AS VARCHAR(10)) + ' hasta ' + CAST(@FechaFin AS VARCHAR(10));
    PRINT 'Registros insertados: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
END;
GO
