import duckdb
import pyarrow as pa
from dagster import IOManager, io_manager, Field, String, get_dagster_logger
import os

class DuckDBBIExportIOManager(IOManager):
    def __init__(self, input_duckdb_path: str, sql_folder: str, export_duckdb_path: str):
        self.input_duckdb_path = input_duckdb_path
        self.sql_folder = sql_folder
        self.export_duckdb_path = export_duckdb_path

    def _parse_sql_files(self):
        """
        Parse SQL files in the specified folder to extract table names.
        """
        logger = get_dagster_logger()
        tables = {}
        
        for file_name in os.listdir(self.sql_folder):
            if file_name.endswith(".sql"):
                file_path = os.path.join(self.sql_folder, file_name)
                with open(file_path, "r") as sql_file:
                    sql_content = sql_file.read()
                    # Extract table name from the SQL file name (assuming table name matches file name)
                    table_name = file_name.replace(".sql", "")
                    tables[table_name] = sql_content
                    logger.info(f"Found SQL file for table: {table_name}")
                    
        return tables

    def _read_table_to_arrow(self, con, table_name):
        """
        Read the table from the input DuckDB file into an Arrow Table.
        """
        logger = get_dagster_logger()
        logger.info(f"Reading table {table_name} from {self.input_duckdb_path}...")
        return con.execute(f"SELECT * FROM {table_name}").fetch_arrow_table()

    def _write_arrow_to_duckdb(self, con, table_name, arrow_table):
        """
        Write the Arrow Table into the export DuckDB file.
        """
        logger = get_dagster_logger()
        logger.info(f"Writing table {table_name} to {self.export_duckdb_path}...")

        # Register the Arrow table as a temporary DuckDB table
        con.register('arrow_temp_table', arrow_table)
        
        # Use CREATE OR REPLACE to avoid the error if the table already exists
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM arrow_temp_table")


    def handle_output(self, context, obj):
        """
        Loop through the tables in the SQL folder, read from the input DuckDB, and write to the export DuckDB.
        """
        logger = get_dagster_logger()
        tables = self._parse_sql_files()

        # Connect to the input DuckDB file
        con_input = duckdb.connect(self.input_duckdb_path)
        
        # Connect to the export DuckDB file (output)
        con_output = duckdb.connect(self.export_duckdb_path)

        for table_name, _ in tables.items():
            arrow_table = self._read_table_to_arrow(con_input, table_name)
            self._write_arrow_to_duckdb(con_output, table_name, arrow_table)

        con_input.close()
        con_output.close()

        logger.info(f"Export completed successfully.")

    def load_input(self, context):
        """
        Optional: If you want to implement loading logic for this IO manager, 
        for example, reading tables from the new DuckDB path.
        """
        pass

@io_manager(
    config_schema={
        "input_duckdb_path": Field(String, is_required=True),
        "sql_folder": Field(String, is_required=True),
        "export_duckdb_path": Field(String, is_required=True),
    }
)
def duckdb_bi_export_io_manager(context):
    return DuckDBBIExportIOManager(
        input_duckdb_path=context.resource_config["input_duckdb_path"],
        sql_folder=context.resource_config["sql_folder"],
        export_duckdb_path=context.resource_config["export_duckdb_path"]
    )