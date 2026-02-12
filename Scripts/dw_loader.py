
import os
import sys
import pyodbc
from configparser import ConfigParser
from datetime import datetime


class ELTDataWarehouseLoader:
    """
    Orquestador Python para ejecutar el proceso INT -> DW
    """

    def __init__(self, config_file='config.ini'):
        self.config = ConfigParser()
        self.config.optionxform = str

        base_dir = os.path.dirname(os.path.abspath(__file__))
        ruta_config = os.path.join(base_dir, config_file)

        if not os.path.exists(ruta_config):
            print(f"ERROR: No se encontró '{config_file}' en: {base_dir}")
            raise FileNotFoundError(f"Archivo '{config_file}' no encontrado")

        if not self.config.read(ruta_config, encoding='utf-8'):
            raise Exception(f"No se pudo leer '{config_file}'")

        self.connection = None
        self.sp_orquestador = 'SP_Orquestador_INT_to_DW'
        self.reprocesar = 0

 
    # ------------------------------------------------------------------
    def log(self, message, level="INFO"):
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] [{level}] {message}")

    # ------------------------------------------------------------------
    def connect_db(self):
        try:
            server = self.config.get('DATABASE', 'server').strip()
            database = self.config.get('DATABASE', 'database').strip()
            trusted = self.config.get('DATABASE', 'trusted_connection', fallback='yes').strip()
            driver = self.config.get('DATABASE', 'driver', fallback='ODBC Driver 17 for SQL Server').strip()

            self.log(f"Conectando a: {server}\\{database}")

            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection={trusted};"
            )

            self.connection = pyodbc.connect(conn_str, timeout=30)
            self.connection.autocommit = False
            self.log("Conexion establecida", "SUCCESS")
            return True

        except pyodbc.Error as e:
            self.log(f"Error de conexion: {str(e)}", "ERROR")
            self.log("Verifique config.ini y que SQL Server este corriendo", "WARNING")
            return False

    # ------------------------------------------------------------------
    def execute_orchestrator(self):
        if not self.connection:
            raise RuntimeError("Sin conexion a BD")

        print("\n" + "=" * 70)
        self.log(f"Ejecutando: {self.sp_orquestador}")
        self.log(f"Reprocesar: {self.reprocesar}")
        print("=" * 70 + "\n")

        try:
            cursor = self.connection.cursor()
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

            if hasattr(cursor, 'messages'):
                for msg in cursor.messages:
                    print(msg[1])

            self.connection.commit()

            print("\n" + "=" * 70)
            self.log("Orquestador ejecutado correctamente", "SUCCESS")
            print("=" * 70 + "\n")

            return True

        except pyodbc.Error as ex:
            if self.connection:
                self.connection.rollback()

            print("\n" + "=" * 70)
            self.log("ERROR AL EJECUTAR ORQUESTADOR", "ERROR")
            print("=" * 70)
            print(f"\n{str(ex)}\n")

            try:
                cursor = self.connection.cursor()
                cursor.execute("""
                    SELECT TOP 5
                        Tabla_Origen,
                        Motivo_Rechazo,
                        COUNT(*) as Cantidad
                    FROM ETL_Registros_Rechazados
                    WHERE ID_Proceso = (
                        SELECT MAX(ID_Proceso) FROM ETL_Control_Procesos
                    )
                    GROUP BY Tabla_Origen, Motivo_Rechazo
                    ORDER BY COUNT(*) DESC
                """)

                rechazados = cursor.fetchall()
                if rechazados:
                    print("REGISTROS RECHAZADOS:")
                    print("-" * 70)
                    for tabla, motivo, cant in rechazados:
                        print(f"  - {tabla}: {motivo} ({cant} registros)")
                    print("-" * 70 + "\n")
            except:
                pass

            return False

    # ------------------------------------------------------------------
    def show_summary(self):
        print("=" * 70)
        print("RESUMEN FINAL")
        print("=" * 70)

        try:
            cursor = self.connection.cursor()

            cursor.execute("""
                SELECT TOP 1
                    Estado,
                    Registros_Procesados,
                    Registros_Rechazados,
                    DATEDIFF(SECOND, Fecha_Inicio, Fecha_Fin) as Duracion
                FROM ETL_Control_Procesos
                WHERE Nombre_Proceso = 'INT_to_DW_Completo'
                ORDER BY ID_Proceso DESC
            """)

            row = cursor.fetchone()
            if row:
                estado, procesados, rechazados, duracion = row

                print(f"\nEstado     : {estado}")
                print(f"Tiempo     : {duracion} segundos")
                print(f"Procesados : {procesados}")
                print(f"Rechazados : {rechazados}")

            cursor.execute("SELECT COUNT(*) FROM Fact_Ventas")
            ventas = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM Fact_Entregas")
            entregas = cursor.fetchone()[0]

            print("\nTABLAS FACT:")
            print(f"Fact_Ventas   : {ventas} registros")
            print(f"Fact_Entregas : {entregas} registros")

            print("\n" + "=" * 70 + "\n")

        except Exception as e:
            self.log(f"No se pudo generar resumen: {e}", "WARNING")

    # ------------------------------------------------------------------
    def run(self):
        print("\n" + "=" * 70)
        print("DATA WAREHOUSE LOADER - DATASHOP")
        print("Proceso: INT -> DW (Final)")
        print("=" * 70)

        inicio = datetime.now()
        self.log(f"Inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            if not self.connect_db():
                return False

            if not self.execute_orchestrator():
                return False

            self.show_summary()

            fin = datetime.now()
            duracion = (fin - inicio).total_seconds()

            print("=" * 70)
            print("PROCESO COMPLETADO")
            print(f"Duracion total: {duracion:.1f} segundos")
            print("=" * 70 + "\n")

            return True

        except Exception as e:
            print("=" * 70)
            print("PROCESO CON ERRORES")
            print("=" * 70)
            self.log(f"Error: {str(e)}", "ERROR")
            return False

        finally:
            if self.connection:
                self.connection.close()
                self.log("Conexion cerrada")


# ----------------------------------------------------------------------
# EJECUCION
if __name__ == "__main__":
    try:
        loader = ELTDataWarehouseLoader()
        success = loader.run()
        sys.exit(0 if success else 1)

    except FileNotFoundError:
        print("\nArchivo config.ini no encontrado")
        print("""
[DATABASE]
server = localhost
database = DataShop
trusted_connection = yes
driver = ODBC Driver 17 for SQL Server
        """)
        sys.exit(1)
