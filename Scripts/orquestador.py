
import os
import sys
import pyodbc
from configparser import ConfigParser
from datetime import datetime


class ELTDataWarehouseLoader:
    """
    Orquestador
    """

    def preparar_tablas(self):
        """Limpia las tablas de hechos y resetea contadores usando la conexión interna."""
        try:
            with self.connection.cursor() as cursor:
                sentencias = [
                    
                        "DELETE FROM Fact_Entregas;",
                        "DELETE FROM Fact_Ventas;",
                        "DELETE FROM ETL_Registros_Rechazados;",
                        "DELETE FROM ETL_Control_Procesos;",


                        "DELETE FROM Dim_Almacen;",
                        "DELETE FROM Dim_Cliente;",
                        "DELETE FROM Dim_EstadoPedido;",
                        "DELETE FROM Dim_Producto;",
                        "DELETE FROM Dim_Proveedor;",
                        "DELETE FROM Dim_Tiempo;",
                        "DELETE FROM Dim_Tienda;",

                        "DBCC CHECKIDENT ('Fact_Ventas', RESEED, 0);",
                        "DBCC CHECKIDENT ('Fact_Entregas', RESEED, 0);",
                        "DBCC CHECKIDENT ('Dim_Almacen', RESEED, 0);",
                        "DBCC CHECKIDENT ('Dim_Cliente', RESEED, 0);",
                        "DBCC CHECKIDENT ('Dim_EstadoPedido', RESEED, 0);",
                        "DBCC CHECKIDENT ('Dim_Producto', RESEED, 0);",
                        "DBCC CHECKIDENT ('Dim_Proveedor', RESEED, 0);",
                        "DBCC CHECKIDENT ('Dim_Tiempo', RESEED, 0);",
                        
                ]
                for sql in sentencias:
                    cursor.execute(sql)
                self.connection.commit()
                print("INFO: Tablas de hechos reseteadas. IDs volveran a 1.")
        except Exception as e:
            print(f"WARNING: No se pudo limpiar automaticamente: {e}")

    def __init__(self, config_file='config.ini'):
        self.config = ConfigParser()
        self.config.optionxform = str

        base_dir = os.path.dirname(os.path.abspath(__file__))
        ruta_absoluta_config = os.path.join(base_dir, config_file)

        if not os.path.exists(ruta_absoluta_config):
            print(f"ERROR: No se encontro '{config_file}' en: {base_dir}")
            raise FileNotFoundError(f"Archivo '{config_file}' no encontrado")

        if not self.config.read(ruta_absoluta_config):
            raise Exception(f"No se pudo leer '{config_file}'")

        self.connection = None
        self.sp_orquestador = 'dbo.SP_Orquestador_INT_to_DW'
        self.reprocesar = 0

    def log(self, message, level="INFO"):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] [{level}] {message}")

    def connect_db(self):
        """Conectar a SQL Server"""
        try:
            server = self.config.get('DATABASE', 'server').strip()
            database = self.config.get('DATABASE', 'database').strip()
            trusted_connection = self.config.get('DATABASE', 'trusted_connection', fallback='yes').strip()
            driver = self.config.get('DATABASE', 'driver', fallback='ODBC Driver 17 for SQL Server').strip()

            self.log(f"Conectando a: {server}\\{database}", "PROCESS")

            connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection={trusted_connection};"
            )

            self.connection = pyodbc.connect(connection_string, timeout=30)
            self.connection.autocommit = False
            self.log("Conexion establecida correctamente", "SUCCESS")

        except pyodbc.Error as e:
            self.log(f"Error de conexion: {str(e)}", "ERROR")
            raise

    def verify_prerequisites(self):
        """Verifica que el entorno este listo"""
        self.log("Verificando prerequisitos...", "PROCESS")

        cursor = self.connection.cursor()
        issues = []

        cursor.execute("SELECT COUNT(*), MIN(Fecha), MAX(Fecha) FROM Dim_Tiempo")
        count, min_fecha, max_fecha = cursor.fetchone()

        if count == 0:
            issues.append("Dim_Tiempo esta vacia")
            self.log("Dim_Tiempo esta VACIA", "ERROR")
        else:
            self.log(f"Dim_Tiempo: {count} registros ({min_fecha} a {max_fecha})", "SUCCESS")

        int_tables = [
            'INT_Cliente', 'INT_Producto', 'INT_Tienda',
            'INT_Ventas', 'INT_Entregas', 'INT_EstadoPedido',
            'INT_Almacen', 'INT_Proveedor'
        ]

        for table in int_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]

            if count == 0:
                issues.append(f"{table} esta vacia")
                self.log(f"{table}: VACIA", "WARNING")
            else:
                self.log(f"{table}: {count} registros", "INFO")

        cursor.execute(f"""
            SELECT COUNT(*)
            FROM sys.objects
            WHERE type = 'P'
              AND name = '{self.sp_orquestador.replace('dbo.', '')}'
        """)

        if cursor.fetchone()[0] == 0:
            issues.append(f"SP {self.sp_orquestador} no existe")
            self.log(f"{self.sp_orquestador} no encontrado", "ERROR")
        else:
            self.log(f"{self.sp_orquestador} encontrado", "SUCCESS")

        if issues:
            self.log("ADVERTENCIAS ENCONTRADAS:", "WARNING")
            for issue in issues:
                self.log(f"- {issue}", "WARNING")

            response = input("Desea continuar de todas formas? (s/n): ")
            if response.lower() != 's':
                self.log("Proceso cancelado por el usuario", "WARNING")
                return False

        return True

    def execute_orchestrator(self):
        if not self.connection:
            raise RuntimeError("La conexion a la BD no esta establecida")

        self.log("=" * 60)
        self.log(f"Ejecutando: {self.sp_orquestador}", "PROCESS")
        self.log(f"Parametro @Reprocesar = {self.reprocesar}")
        self.log("=" * 60)

        try:
            cursor = self.connection.cursor()
            cursor.execute("SET NOCOUNT OFF;")

            cursor.execute(
                f"EXEC {self.sp_orquestador} @Reprocesar = ?",
                (self.reprocesar,)
            )

            while True:
                try:
                    if not cursor.nextset():
                        break
                except pyodbc.ProgrammingError:
                    break

            for message in cursor.messages:
                print(message[1])

            self.connection.commit()
            self.log("SP orquestador ejecutado correctamente", "SUCCESS")
            self.log("Transaccion confirmada", "SUCCESS")

        except pyodbc.Error as ex:
            if self.connection:
                self.connection.rollback()

            self.log("ERROR AL EJECUTAR EL ORQUESTADOR", "ERROR")
            self.log(str(ex), "ERROR")
            raise

    def show_summary(self):
        self.log("=" * 60)
        self.log("RESUMEN DE CARGA")
        self.log("=" * 60)

        try:
            cursor = self.connection.cursor()

            cursor.execute("""
                SELECT TOP 1
                    Estado,
                    Registros_Procesados,
                    Registros_Rechazados,
                    DATEDIFF(SECOND, Fecha_Inicio, Fecha_Fin)
                FROM ETL_Control_Procesos
                WHERE Nombre_Proceso = 'INT_to_DW_Completo'
                ORDER BY ID_Proceso DESC
            """)

            row = cursor.fetchone()
            if row:
                estado, procesados, rechazados, duracion = row
                self.log(f"Estado: {estado}")
                self.log(f"Procesados: {procesados}")
                self.log(f"Rechazados: {rechazados}")
                self.log(f"Duracion: {duracion} segundos")

            cursor.execute("SELECT COUNT(*) FROM Fact_Ventas")
            ventas = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM Fact_Entregas")
            entregas = cursor.fetchone()[0]

            self.log("TABLAS DE HECHOS:")
            self.log(f"Fact_Ventas: {ventas} registros")
            self.log(f"Fact_Entregas: {entregas} registros")

        except Exception as e:
            self.log(f"No se pudo generar resumen: {e}", "WARNING")

    def run(self):
        print("=" * 60)
        print("DATA WAREHOUSE LOADER - DATASHOP")
        print("Proceso: INT -> DW (Tablas Finales)")
        print("=" * 60)

        try:
            self.connect_db()

            if not self.verify_prerequisites():
                return False

            self.execute_orchestrator()
            self.show_summary()

            self.log("PROCESO COMPLETADO EXITOSAMENTE", "SUCCESS")
            return True

        except Exception as e:
            self.log("PROCESO FINALIZADO CON ERRORES", "ERROR")
            self.log(str(e), "ERROR")
            return False

        finally:
            if self.connection:
                self.connection.close()
                self.log("Conexion cerrada")



# EJECUCION PRINCIPAL

if __name__ == "__main__":
    print("#" * 60)
    print("ETL DATA WAREHOUSE - DATASHOP")
    print("Modulo: Carga INT -> DW")
    print("#" * 60)

    try:
        loader = ELTDataWarehouseLoader(config_file='config.ini')
        loader.connect_db()
        loader.preparar_tablas()
        success = loader.run()
        sys.exit(0 if success else 1)

    except Exception as e:
        print(f"ERROR CRITICO: {e}")
        sys.exit(1)
