import logging
import traceback
import pyodbc
import struct
from azure.identity import DefaultAzureCredential
import pandas as pd


class DatabaseConnectionError(Exception):
    """Custom exception for database connection errors."""
    pass

class DatabaseConnection:
    """
    Singleton class to manage a single database connection.
    """
    _instance = None
  
    def __init__(self, server, database, driver,localRun):
        """
        Private constructor to prevent direct object creation.
        """
        if not hasattr(self, '_initialized'):  # Prevent re-initialization
            self._initialized = True
            self._connection = None
            self._setup_connection(server, database, driver,localRun)

    
    def _setup_connection(self, server, database, driver,localRun):
        """
        Establishes a connection to Azure SQL Database using either Managed Identity (Azure) or
        Azure AD access token (local run).
        Args:
            server (str): The SQL server hostname.
            database (str): The database name.
            driver (str): The ODBC driver name.
            localRun (str): Flag to determine local vs Azure run.
        Raises:
        DatabaseConnectionError: If connection fails.
        """
        logging.info(f"Initializing connection with server={server}, database={database}, driver={driver}, localRun={localRun}")
        connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database}'
        try:
            # SQL Authentication Through MI As MSI_SECRET
            credential = DefaultAzureCredential()
            token = credential.get_token("https://database.windows.net/").token
            if localRun != 'LOCAL':
                logging.info('Connection Using Authentication=ActiveDirectoryMsi')
                self._connection = pyodbc.connect(connection_string+';Authentication=ActiveDirectoryMsi')
            else:
                logging.info('Connection Using SQL_COPT_SS_ACCESS_Token')
                SQL_COPT_SS_ACCESS_TOKEN = 1256
                exptoken = b''
                for i in bytes(token, "UTF-8"):
                    exptoken += bytes({i})
                    exptoken += bytes(1)
                tokenstruct = struct.pack("=i", len(exptoken)) + exptoken
                self._connection = pyodbc.connect(connection_string, attrs_before = { SQL_COPT_SS_ACCESS_TOKEN:tokenstruct })
        except Exception as err:
            logging.error(traceback.format_exc())
            logging.error(traceback.format_exc())
            raise DatabaseConnectionError(f"Database connection not established: {err}") from err

    def get_connection(self):
        if self._connection is None:
            raise DatabaseConnectionError("Database connection not established.")
        return self._connection
    
    def call_stored_procedure_with_select(self, procedure_name, parameters, temp_table_name):
        if not self._connection:
            raise DatabaseConnectionError("Database connection not established")

        try:
            cursor = self._connection.cursor()

            # Build the EXEC statement with parameters placeholders
            if isinstance(parameters, dict):
                param_placeholders = ", ".join(f"@{key} = ?" for key in parameters.keys())
                param_values = list(parameters.values())
            elif isinstance(parameters, list):
                param_placeholders = ", ".join(["?"] * len(parameters))
                param_values = parameters
            else:
                param_placeholders = ""
                param_values = []

            # Combine SP exec + select from temp table in one batch
            sql_query = f"""
            EXEC {procedure_name} {param_placeholders};
            SELECT * FROM {temp_table_name};
            """

            cursor.execute(sql_query, param_values)

            # Skip results until you get to the final SELECT results
            while True:
                try:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    df = pd.DataFrame.from_records(rows, columns=columns)
                    return df
                except Exception:
                    pass
                if not cursor.nextset():
                    break

            # If no resultset found
            logging.warning("No results returned from combined SP and select.")
            return pd.DataFrame()

        except Exception as e:
            logging.error(f"Error calling stored procedure with select: {e}")
            return None
        
    def run_script_and_get_last_result(self, script: str, params: tuple = None) -> pd.DataFrame:
        """
        Executes a multi-batch SQL script (split by GO) in one connection,
        and returns a DataFrame from the last batch's result set.
        Params is an optional tuple of parameters for the last batch.
        """
        if not self._connection:
            raise DatabaseConnectionError("Database connection not established")

        batches = [batch.strip() for batch in script.split("\nGO\n") if batch.strip()]

        cursor = self._connection.cursor()
        try:
            # Execute all but last batch without params
            for batch in batches[:-1]:
                cursor.execute(batch)
                while cursor.nextset():
                    pass

            # Execute last batch with params if provided
            if params:
                cursor.execute(batches[-1], params)
            else:
                cursor.execute(batches[-1])
                
            while cursor.description is None and cursor.nextset():
                pass

            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame.from_records(rows, columns=columns)

        finally:
            cursor.close()

    
    def read_sql(self,query):
        if not self._connection:
            raise DatabaseConnectionError("Database connection not established")
        try:
            df = pd.read_sql(query, self._connection)
        except Exception as err:
            raise DatabaseConnectionError(f"Error executing query: {err}") from err
        return df
    
    def execute_query(self, query, params=None):
        if not self._connection:
            raise DatabaseConnectionError("Database connection not established")

        cursor = None
        rows = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(query, params or [])
            # Fetch the Query result into a variable
            rows = cursor.fetchall()
        except Exception as err:
            raise DatabaseConnectionError(f"Error executing query: {err}") from err
        finally:
            if cursor:
                cursor.close()
        return rows
    
    def run_multistatement_script(self, script: str):
        if not self._connection:
            raise DatabaseConnectionError("No active DB connection.")
        cursor = self._connection.cursor()
        for stmt in script.strip().split(';'):
            if stmt.strip():
                cursor.execute(stmt)
        self._connection.commit()

    def close_connection(self):
      if self._connection is not None:
          self._connection.close()
          self._connection = None
          self._initialized = False

    def __enter__(self):
        logging.info("db __enter__")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        logging.info("db __exit__")
        self.close_connection()