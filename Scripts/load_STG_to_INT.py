import pyodbc
from configparser import ConfigParser
import os

class DWLoader:
    def __init__(self, config_file='config.ini'):
        self.config = ConfigParser()
        self.config.optionxform = str
        
        # --- Localizar config.ini ---
        # Obtiene la ruta de la carpeta donde esta este script
        base_dir = os.path.dirname(os.path.abspath(__file__))
        # Une la carpeta con el nombre del archivo config.ini
        ruta_absoluta_config = os.path.join(base_dir, config_file)
        
        # Verifica que el archivo de configuración exista y se lea correctamente
        if not os.path.exists(ruta_absoluta_config) or not self.config.read(ruta_absoluta_config):
            print(f" ERROR: No se encontró config.ini en: {ruta_absoluta_config}")
            raise FileNotFoundError(f"No se pudo leer el archivo de configuración: {config_file}")
        
        self.connection = None

    def connect_db(self):
        """Conectar a SQL Server"""
        try:
            server = self.config.get('DATABASE', 'server').strip()
            database = self.config.get('DATABASE', 'database').strip()
            trusted_connection = self.config.get('DATABASE', 'trusted_connection').strip()
            driver = self.config.get('DATABASE', 'driver', fallback='ODBC Driver 17 for SQL Server').strip()

            connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection={trusted_connection};"
            )

            print(f"Conectando a {server} | BD: {database}")
            self.connection = pyodbc.connect(connection_string, timeout=10)
            self.connection.autocommit = False
            print("Conexión exitosa")
        except Exception as e:
            print(f"Error de conexión: {e}")
            raise

    def run_orchestrator(self):
        """Ejecuta el SP orquestador STG -> INT"""
        cursor = self.connection.cursor()

        print("\nEjecutando SP_Orquestador_STG_to_INT...\n")

        try:
            cursor.execute("EXEC SP_Orquestador_STG_to_INT")

            # Consumir mensajes PRINT del SQL para que no queden pendientes
            while cursor.nextset():
                pass

            print("SP_Orquestador_STG_to_INT ejecutado correctamente")

        except Exception as e:
            print(f"Error al ejecutar el orquestador: {e}")
            raise

    def run(self):
        try:
            self.connect_db()
            self.run_orchestrator()

            self.connection.commit()
            print("\nCARGA STG → INT COMPLETADA EXITOSAMENTE")

        except Exception as e:
            print(f"\nERROR FATAL: {e}")
            if self.connection:
                self.connection.rollback()
                print("ROLLBACK ejecutado")

        finally:
            if self.connection:
                self.connection.close()
                print("Conexión cerrada")


# EJECUCIÓN

def main():
    loader = DWLoader()
    loader.run()

if __name__ == "__main__":
    main()
