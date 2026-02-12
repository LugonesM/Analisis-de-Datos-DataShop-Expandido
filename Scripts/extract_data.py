import pandas as pd
import pyodbc
from configparser import ConfigParser
from datetime import datetime
import os


class CSVToSQLServer:
    """
    Clase para Extracción y Carga de datos CSV a tablas STAGING en SQL Server.
    """
    def __init__(self, config_file='config.ini'):
        self.config = ConfigParser()
        self.config.optionxform = str
        
        # --- Localizar config.ini dinámicamente ---
        # Obtiene la ruta de la carpeta donde está este script 
        base_dir = os.path.dirname(os.path.abspath(__file__))
        # Une la ruta de la carpeta con el nombre del archivo config.ini
        ruta_absoluta_config = os.path.join(base_dir, config_file)
        
        # Verificar que el archivo de configuración exista
        if not os.path.exists(ruta_absoluta_config) or not self.config.read(ruta_absoluta_config):
            print(f" ERROR: No se pudo leer el archivo de configuración en: {ruta_absoluta_config}")
            raise FileNotFoundError(f"Archivo '{config_file}' no encontrado.")

        self.connection = None
        
        # --- Localizar carpeta DATASET ---
        # Sube un nivel y entra en DATASET
        self.dataset_folder = os.path.abspath(os.path.join(base_dir, '..', 'DATASET'))
        
        # Definición de columnas 
        self.column_mapping = {
            'Clientes.csv': ['CodCliente', 'RazonSocial', 'Telefono', 'Mail', 'Direccion', 'Localidad', 'Provincia', 'CP'],
            'Productos.csv': ['CodigoProducto', 'Descripcion', 'Categoria', 'Marca', 'PrecioCosto', 'PrecioVentaSugerido'],
            'Tiendas.csv': ['CodigoTienda', 'Descripcion', 'Direccion', 'Localidad', 'Provincia', 'CP', 'TipoTienda'],
            'Ventas.csv': ['FechaVenta', 'CodigoProducto', 'Producto', 'Cantidad', 'PrecioVenta', 'CodigoCliente', 'Cliente', 'CodigoTienda', 'Tienda'],
            'Ventas_add.csv': ['FechaVenta', 'CodigoProducto', 'Producto', 'Cantidad', 'PrecioVenta', 'CodigoCliente', 'Cliente', 'CodigoTienda', 'Tienda'],
            'EstadoDelPedido.csv': ['CodEstado', 'Descripcion_Estado'],
            'Entregas.csv': ['CodEntrega', 'CodVenta','CodProveedor', 'Proveedor','CodAlmacen', 'Almacen','CodEstado', 'Estado', 'Fecha_Envio', 'Fecha_Entrega'],
            'Almacenes.csv': ['CodAlmacen', 'Nombre_Almacen', 'Ubicacion']
        }
   
    def connect_db(self):
        
        try:
            server = self.config.get('DATABASE', 'server', fallback='').strip()
            database = self.config.get('DATABASE', 'database', fallback='').strip()
            trusted_connection = self.config.get('DATABASE', 'trusted_connection', fallback='').strip()
            driver = self.config.get('DATABASE', 'driver', fallback='ODBC Driver 17 for SQL Server').strip()
            
            if not all([server, database, trusted_connection]):
                 raise ValueError("Faltan parámetros críticos en config.ini.")
            
            print(f" Conectando a: {server} | BD: {database}")
            connection_string = (
                f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};Trusted_Connection={trusted_connection};"
            )
            self.connection = pyodbc.connect(connection_string, timeout=10)
            self.connection.autocommit = False 
            print(" Conexión exitosa!")
        except Exception as e:
            print(f" Error de conexión: {e}")
            raise

    def get_csv_path(self, filename):
        """Obtener ruta completa del archivo CSV usando la ruta absoluta calculada"""
        return os.path.join(self.dataset_folder, filename)
    
    def run_etl(self):
        """Ejecutar proceso de extracción y carga."""
        print(f"\n INICIANDO PROCESO DE EXTRACCION Y CARGA")

        
        try:
            # 1. Verificar la carpeta de datos
            if not os.path.exists(self.dataset_folder):
                print(f" Carpeta '{self.dataset_folder}' no encontrada.")
                return
            
            print(f" Buscando archivos en: {self.dataset_folder}")
            
            # 2. Conectar a BD
            self.connect_db()
            cursor = self.connection.cursor()
            cursor.fast_executemany = True 
            
            # 3. Mapeo de archivos 
            csv_to_staging = {
                'Clientes.csv': 'STG_Clientes',
                'Productos.csv': 'STG_Productos', 
                'Tiendas.csv': 'STG_Tiendas',
                'Ventas.csv': 'STG_Ventas',
                'Ventas_add.csv': 'STG_Ventas_Add',
                'EstadoDelPedido.csv': 'STG_EstadoDelPedido',
                'Entregas.csv': 'STG_Entregas',
                'Almacenes.csv': 'STG_Almacenes'
            }
            
            for csv_file, table_name in csv_to_staging.items():
                csv_path = self.get_csv_path(csv_file)
                
                if os.path.exists(csv_path):
                    expected_columns = self.column_mapping[csv_file]
                    df = pd.read_csv(csv_path, usecols=expected_columns)

                    # Conversión de fechas para evitar problemas de formato
                    if csv_file in ['Ventas.csv', 'Ventas_add.csv']:
                        df['FechaVenta'] = pd.to_datetime(df['FechaVenta'], errors='coerce')

                    if csv_file == 'Entregas.csv':
                        df['Fecha_Envio'] = pd.to_datetime(df['Fecha_Envio'], errors='coerce')
                        df['Fecha_Entrega'] = pd.to_datetime(df['Fecha_Entrega'], errors='coerce')

                    # Normalización
                    df = df.fillna('').astype(str)
                    df['Fecha_Carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Truncar e Insertar
                    cursor.execute(f"TRUNCATE TABLE {table_name}")
                    columns = ', '.join(df.columns) 
                    placeholders = ', '.join(['?' for _ in df.columns])
                    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    
                    data_to_insert = [tuple(row) for row in df.values]
                    cursor.executemany(query, data_to_insert)
                    print(f" OK: {csv_file} → {table_name}")
                else:
                    print(f" ADVERTENCIA: {csv_file} no encontrado en {self.dataset_folder}")
            
            self.connection.commit()
            print("\n PROCESO DE EXTRACCION DE DATOS COMPLETADO")
            
        except Exception as e:
            print(f" ERROR FATAL: {e}")
            if self.connection: self.connection.rollback()
        finally:
            if self.connection: self.connection.close()

def main():
    etl = CSVToSQLServer()
    etl.run_etl()

if __name__ == "__main__":
    main()
