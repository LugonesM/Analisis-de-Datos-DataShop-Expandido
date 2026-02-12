
-- CREACIÓN DE TABLAS INTERMEDIAS (INT)

PRINT '';
PRINT 'CREACIÓN DE TABLAS INT';
PRINT 'Capa Intermedia de Transformación';
PRINT '';
GO

-- INT_EstadoPedido
-- Transformación: STG_EstadoDelPedido -> Dim_EstadoPedido
IF OBJECT_ID('INT_EstadoPedido', 'U') IS NOT NULL
    DROP TABLE INT_EstadoPedido;
GO

CREATE TABLE INT_EstadoPedido (
    -- Clave natural (del sistema origen)
    CodEstado VARCHAR(20) NOT NULL,
    
    -- Atributos descriptivos limpios
    Descripcion_Estado VARCHAR(100) NOT NULL,
    Tipo_Estado VARCHAR(50) NOT NULL,
    Orden_Secuencia INT NOT NULL,
    EsEstadoFinal BIT NOT NULL DEFAULT 0,
    
    -- Auditoría de carga INT
    Fecha_Proceso DATETIME DEFAULT GETDATE(),
    ID_Proceso INT NULL,
    
    -- Constraint para evitar duplicados en INT
    CONSTRAINT PK_INT_EstadoPedido PRIMARY KEY (CodEstado)
);

CREATE INDEX IDX_INT_EstadoPedido_Tipo ON INT_EstadoPedido(Tipo_Estado);
PRINT ' Tabla INT_EstadoPedido creada';
GO



-- INT_Almacen
-- Transformación: STG_Almacenes -> Dim_Almacen
IF OBJECT_ID('INT_Almacen', 'U') IS NOT NULL
    DROP TABLE INT_Almacen;
GO

CREATE TABLE INT_Almacen (
    -- Clave natural
    CodAlmacen VARCHAR(50) NOT NULL,
    
    -- Atributos limpios
    NombreAlmacen VARCHAR(255) NOT NULL,
    Ubicacion VARCHAR(255) NOT NULL,
    Ciudad VARCHAR(100) NULL,
    Provincia VARCHAR(100) NULL,
    CodigoPostal VARCHAR(20) NULL,
    TipoAlmacen VARCHAR(50) NULL,
    Activo BIT DEFAULT 1,
    
    -- Auditoría
    Fecha_Proceso DATETIME DEFAULT GETDATE(),
    ID_Proceso INT NULL,
    
    CONSTRAINT PK_INT_Almacen PRIMARY KEY (CodAlmacen)
);

CREATE INDEX IDX_INT_Almacen_Provincia ON INT_Almacen(Provincia);
PRINT ' Tabla INT_Almacen creada';
GO



-- INT_Cliente
-- Transformación: STG_Clientes -> Dim_Cliente
IF OBJECT_ID('INT_Cliente', 'U') IS NOT NULL
    DROP TABLE INT_Cliente;
GO

CREATE TABLE INT_Cliente (
    -- Clave natural
    CodCliente VARCHAR(50) NOT NULL,
    
    -- Atributos limpios
    RazonSocial VARCHAR(255) NOT NULL,
    Telefono VARCHAR(50) NULL,
    Mail VARCHAR(255) NULL,
    Direccion VARCHAR(255) NULL,
    Localidad VARCHAR(100) NOT NULL,
    Provincia VARCHAR(100) NOT NULL,
    CP VARCHAR(20) NULL,
    
    -- Auditoría
    Fecha_Proceso DATETIME DEFAULT GETDATE(),
    ID_Proceso INT NULL,
    
    CONSTRAINT PK_INT_Cliente PRIMARY KEY (CodCliente)
);

CREATE INDEX IDX_INT_Cliente_Provincia ON INT_Cliente(Provincia);
PRINT ' Tabla INT_Cliente creada';
GO



-- INT_Producto
-- Transformación: STG_Productos -> Dim_Producto
IF OBJECT_ID('INT_Producto', 'U') IS NOT NULL
    DROP TABLE INT_Producto;
GO

CREATE TABLE INT_Producto (
    -- Clave natural
    CodigoProducto VARCHAR(100) NOT NULL,
    
    -- Atributos limpios y casteados
    Descripcion VARCHAR(255) NOT NULL,
    Categoria VARCHAR(100) NOT NULL,
    Marca VARCHAR(100) NOT NULL,
    PrecioCosto DECIMAL(18,2) NOT NULL DEFAULT 0,
    PrecioVentaSugerido DECIMAL(18,2) NOT NULL DEFAULT 0,
    
    -- Auditoría
    Fecha_Proceso DATETIME DEFAULT GETDATE(),
    ID_Proceso INT NULL,
    
    CONSTRAINT PK_INT_Producto PRIMARY KEY (CodigoProducto)
);

CREATE INDEX IDX_INT_Producto_Categoria ON INT_Producto(Categoria);
CREATE INDEX IDX_INT_Producto_Marca ON INT_Producto(Marca);
PRINT ' Tabla INT_Producto creada';
GO



-- INT_Tienda
-- Transformación: STG_Tiendas -> Dim_Tienda
IF OBJECT_ID('INT_Tienda', 'U') IS NOT NULL
    DROP TABLE INT_Tienda;
GO

CREATE TABLE INT_Tienda (
    -- Clave natural
    CodigoTienda VARCHAR(50) NOT NULL,
    
    -- Atributos limpios
    Descripcion VARCHAR(255) NOT NULL,
    Direccion VARCHAR(255) NULL,
    Localidad VARCHAR(100) NOT NULL,
    Provincia VARCHAR(100) NOT NULL,
    CP VARCHAR(20) NULL,
    TipoTienda VARCHAR(50) NOT NULL,
    
    -- Auditoría
    Fecha_Proceso DATETIME DEFAULT GETDATE(),
    ID_Proceso INT NULL,
    
    CONSTRAINT PK_INT_Tienda PRIMARY KEY (CodigoTienda)
);

CREATE INDEX IDX_INT_Tienda_Tipo ON INT_Tienda(TipoTienda);
PRINT ' Tabla INT_Tienda creada';
GO



-- INT_Proveedor
-- Transformación: STG_Entregas -> Dim_Proveedor
IF OBJECT_ID('INT_Proveedor', 'U') IS NOT NULL
    DROP TABLE INT_Proveedor;
GO

CREATE TABLE INT_Proveedor (
    -- Clave natural
    CodProveedor VARCHAR(50) NOT NULL,
    
    -- Atributos limpios
    NombreProveedor VARCHAR(255) NOT NULL,
    TipoServicio VARCHAR(100) NULL,
    Activo BIT DEFAULT 1,
    
    -- Auditoría
    Fecha_Proceso DATETIME DEFAULT GETDATE(),
    ID_Proceso INT NULL,
    
    CONSTRAINT PK_INT_Proveedor PRIMARY KEY (CodProveedor)
);

PRINT ' Tabla INT_Proveedor creada';
GO



-- INT_Ventas
-- Transformación: STG_Ventas + STG_Ventas_Add -> Fact_Ventas
IF OBJECT_ID('INT_Ventas', 'U') IS NOT NULL
    DROP TABLE INT_Ventas;
GO

CREATE TABLE INT_Ventas (
    -- ID secuencial de la tabla INT (no es la PK final)
    ID_INT BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Claves naturales 
    FechaVenta DATE NOT NULL,
    CodigoProducto VARCHAR(100) NOT NULL,
    CodigoCliente VARCHAR(50) NOT NULL,
    CodigoTienda VARCHAR(50) NOT NULL,
    
    -- Métricas limpias y casteadas
    Cantidad INT NOT NULL,
    PrecioVenta DECIMAL(18,2) NOT NULL,
    Total_IVA DECIMAL(18,2) NOT NULL,
    
    -- Auditoría
    Fecha_Proceso DATETIME DEFAULT GETDATE(),
    ID_Proceso INT NULL,
    
    -- Constraints de validación
    CONSTRAINT CHK_INT_Ventas_Cantidad CHECK (Cantidad > 0),
    CONSTRAINT CHK_INT_Ventas_Precio CHECK (PrecioVenta >= 0)
);

CREATE INDEX IDX_INT_Ventas_Fecha ON INT_Ventas(FechaVenta);
CREATE INDEX IDX_INT_Ventas_Producto ON INT_Ventas(CodigoProducto);
CREATE INDEX IDX_INT_Ventas_Cliente ON INT_Ventas(CodigoCliente);
CREATE INDEX IDX_INT_Ventas_Tienda ON INT_Ventas(CodigoTienda);
PRINT ' Tabla INT_Ventas creada';
GO



-- INT_Entregas
-- Transformación: STG_Entregas -> Fact_Entregas
IF OBJECT_ID('INT_Entregas', 'U') IS NOT NULL
    DROP TABLE INT_Entregas;
GO

CREATE TABLE INT_Entregas (
    -- ID secuencial de la tabla INT
    ID_INT BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Código natural de la entrega
    CodEntrega VARCHAR(50) NOT NULL UNIQUE,
    
    -- Claves naturales 
    CodVenta BIGINT NULL, -- Puede ser NULL si no se encuentra la venta
    CodProveedor VARCHAR(50) NOT NULL,
    CodAlmacen VARCHAR(50) NOT NULL,
    CodEstado VARCHAR(20) NOT NULL,
    
    -- Fechas casteadas
    Fecha_Envio DATE NOT NULL,
    Fecha_Entrega DATE NULL, -- Puede ser NULL si aún no se entregó
    FechaEstimadaEntrega DATE NULL,
    
    -- Métricas limpias
    CantidadProductos INT NOT NULL DEFAULT 1,
    PesoKg DECIMAL(10,2) NULL,
    VolumenM3 DECIMAL(10,3) NULL,
    DistanciaKm DECIMAL(10,2) NULL,
    CostoEntrega DECIMAL(18,2) NOT NULL DEFAULT 0,
    
    -- Auditoría
    Fecha_Proceso DATETIME DEFAULT GETDATE(),
    ID_Proceso INT NULL,
    
    -- Constraints de validación
    CONSTRAINT CHK_INT_Entregas_Cantidad CHECK (CantidadProductos > 0),
    CONSTRAINT CHK_INT_Entregas_Costo CHECK (CostoEntrega >= 0),
    CONSTRAINT CHK_INT_Entregas_Fechas CHECK (Fecha_Entrega IS NULL OR Fecha_Entrega >= Fecha_Envio)
);

CREATE INDEX IDX_INT_Entregas_CodVenta ON INT_Entregas(CodVenta);
CREATE INDEX IDX_INT_Entregas_Proveedor ON INT_Entregas(CodProveedor);
CREATE INDEX IDX_INT_Entregas_Almacen ON INT_Entregas(CodAlmacen);
CREATE INDEX IDX_INT_Entregas_Estado ON INT_Entregas(CodEstado);
CREATE INDEX IDX_INT_Entregas_FechaEnvio ON INT_Entregas(Fecha_Envio);
PRINT ' Tabla INT_Entregas creada';
GO


-- TABLA DE CONTROL DE PROCESOS ETL
IF OBJECT_ID('ETL_Control_Procesos', 'U') IS NOT NULL
    DROP TABLE ETL_Control_Procesos;
GO

CREATE TABLE ETL_Control_Procesos (
    ID_Proceso INT IDENTITY(1,1) PRIMARY KEY,
    Nombre_Proceso VARCHAR(100) NOT NULL,
    Fecha_Inicio DATETIME NOT NULL,
    Fecha_Fin DATETIME NULL,
    Estado VARCHAR(20) NOT NULL, -- 'EN_PROCESO', 'COMPLETADO', 'ERROR'
    Registros_Procesados INT NULL,
    Registros_Insertados INT NULL,
    Registros_Actualizados INT NULL,
    Registros_Rechazados INT NULL,
    Mensaje_Error VARCHAR(MAX) NULL,
    Usuario_Ejecucion VARCHAR(100) DEFAULT SYSTEM_USER
);

CREATE INDEX IDX_ETL_Control_Fecha ON ETL_Control_Procesos(Fecha_Inicio);
CREATE INDEX IDX_ETL_Control_Estado ON ETL_Control_Procesos(Estado);
PRINT ' Tabla ETL_Control_Procesos creada';
GO


-- TABLA DE REGISTROS RECHAZADOS
IF OBJECT_ID('ETL_Registros_Rechazados', 'U') IS NOT NULL
    DROP TABLE ETL_Registros_Rechazados;
GO

CREATE TABLE ETL_Registros_Rechazados (
    ID_Rechazo BIGINT IDENTITY(1,1) PRIMARY KEY,
    ID_Proceso INT NOT NULL,
    Tabla_Origen VARCHAR(100) NOT NULL,
    Registro_Original VARCHAR(MAX) NOT NULL,
    Motivo_Rechazo VARCHAR(500) NOT NULL,
    Fecha_Rechazo DATETIME DEFAULT GETDATE(),
    
    CONSTRAINT FK_Rechazos_Proceso FOREIGN KEY (ID_Proceso)
        REFERENCES ETL_Control_Procesos(ID_Proceso)
);

CREATE INDEX IDX_ETL_Rechazos_Proceso ON ETL_Registros_Rechazados(ID_Proceso);
CREATE INDEX IDX_ETL_Rechazos_Tabla ON ETL_Registros_Rechazados(Tabla_Origen);
PRINT ' Tabla ETL_Registros_Rechazados creada';
GO


