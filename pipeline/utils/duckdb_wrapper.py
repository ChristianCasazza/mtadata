import os
import duckdb
from pathlib import Path
from dagster import get_dagster_logger
import glob

class DuckDBWrapper:
    def __init__(self, duckdb_path=None):
        """
        Initialize a DuckDB connection.
        If duckdb_path is provided, a persistent DuckDB database will be used.
        Otherwise, it creates an in-memory database.
        """
        if duckdb_path:
            self.con = duckdb.connect(str(duckdb_path), read_only=False)
        else:
            self.con = duckdb.connect(database=':memory:', read_only=False)
        self.registered_tables = []

        # Enable httpfs for remote paths if needed
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")

    def register_data(self, paths, table_names):
        """
        Registers local data files (Parquet, CSV, JSON) in DuckDB by creating views.
        Automatically detects the file type based on the file extension.
        """
        if len(paths) != len(table_names):
            raise ValueError("The number of paths must match the number of table names.")

        for path, table_name in zip(paths, table_names):
            path_str = str(path)
            file_extension = Path(path_str).suffix.lower()

            if file_extension == ".parquet":
                query = f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{path_str}')"
            elif file_extension == ".csv":
                query = f"CREATE VIEW {table_name} AS SELECT * FROM read_csv_auto('{path_str}')"
            elif file_extension == ".json":
                query = f"CREATE VIEW {table_name} AS SELECT * FROM read_json_auto('{path_str}')"
            else:
                raise ValueError(f"Unsupported file type '{file_extension}' for file: {path_str}")

            self.con.execute(query)
            self.registered_tables.append(table_name)

    def bulk_register_data(self, repo_root, base_path, table_names, wildcard="*.parquet"):
        """
        Constructs paths for each table based on a shared base path plus the table name,
        then appends a wildcard for file matching (e.g., '*.parquet'), and registers the data.
        """
        paths = []
        for table_name in table_names:
            path = Path(repo_root) / base_path / table_name / wildcard
            paths.append(path)

        self.register_data(paths, table_names)

    def run_query(self, sql_query, show_results=False):
        """
        Runs a SQL query on the registered tables in DuckDB and returns a Polars DataFrame.
        Optionally displays the result.
        """
        arrow_table = self.con.execute(sql_query).arrow()
        import polars as pl
        df = pl.DataFrame(arrow_table)
        if show_results:
            from IPython.display import display, HTML
            display(HTML(df.__repr__()))
        return df

    def print_query_results(self, df, title="Query Results"):
        """
        Prints a Polars DataFrame as a Rich table.
        """
        from rich.console import Console
        from rich.table import Table
        console = Console()
        table = Table(title=title, title_style="bold green", show_lines=True)
        for column in df.columns:
            table.add_column(str(column), style="bold cyan", overflow="fold")
        for row in df.iter_rows(named=True):
            values = [str(row[col]) for col in df.columns]
            table.add_row(*values, style="white on black")
        console.print(table)

    def _construct_path(self, path, base_path, file_name, extension):
        """
        Constructs the full file path based on input parameters.
        """
        if path:
            return Path(path)
        elif base_path and file_name:
            return Path(base_path) / f"{file_name}.{extension}"
        else:
            return Path(f"output.{extension}")

    def export(self, result, file_type, path=None, base_path=None, file_name=None, with_header=True):
        """
        Exports a Polars DataFrame (or anything convertible via .to_arrow()) to the specified file type.
        """
        file_type = file_type.lower()
        if file_type not in ["parquet", "csv", "json"]:
            raise ValueError("file_type must be one of 'parquet', 'csv', or 'json'.")
        full_path = self._construct_path(path, base_path, file_name, file_type)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        import polars as pl
        if isinstance(result, pl.DataFrame):
            df = result
        elif hasattr(result, "to_arrow"):
            df = pl.DataFrame(result.to_arrow())
        else:
            raise ValueError("Unsupported result type.")
        if file_type == "parquet":
            df.write_parquet(str(full_path))
        elif file_type == "csv":
            df.write_csv(str(full_path), separator=",", include_header=with_header)
        elif file_type == "json":
            df.write_ndjson(str(full_path))
        print(f"File written to: {full_path}")

    def show_tables(self):
        """
        Displays the table names and types currently registered in DuckDB.
        """
        query = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema='main'
        """
        df = self.run_query(query)
        from rich.console import Console
        from rich.table import Table
        console = Console()
        table = Table(title="Registered Tables", title_style="bold green", show_lines=True)
        table.add_column("Table Name", justify="left", style="bold yellow")
        table.add_column("Table Type", justify="left", style="bold cyan")
        for row in df.to_dicts():
            table.add_row(row["table_name"], row["table_type"], style="white on black")
        console.print(table)

    def show_schema(self, table_name):
        """
        Displays the schema of the specified DuckDB table or view.
        """
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        """
        df = self.run_query(query)
        from rich.console import Console
        from rich.table import Table
        console = Console()
        schema_table = Table(title=f"Schema for '{table_name}'", title_style="bold green")
        schema_table.add_column("Column Name", justify="left", style="bold yellow", no_wrap=True)
        schema_table.add_column("Data Type", justify="left", style="bold cyan")
        for row in df.to_dicts():
            schema_table.add_row(row["column_name"], str(row["data_type"]), style="white on black")
        console.print(schema_table)

    def show_parquet_schema(self, file_path):
        """
        Reads a Parquet file using Polars and prints its schema.
        """
        import polars as pl
        df = pl.read_parquet(file_path)
        from rich.console import Console
        from rich.table import Table
        console = Console()
        schema_table = Table(title="Parquet Schema", title_style="bold green")
        schema_table.add_column("Column Name", justify="left", style="bold yellow", no_wrap=True)
        schema_table.add_column("Data Type", justify="left", style="bold cyan")
        for col_name, col_dtype in df.schema.items():
            schema_table.add_row(col_name, str(col_dtype), style="white on black")
        console.print(schema_table)
        console.print(f"[bold magenta]\nNumber of rows:[/] [bold white]{df.height}[/]")

    def register_partitioned_data(self, base_path, table_name, wildcard="*/*/*.parquet"):
        """
        Registers partitioned Parquet data using Hive partitioning by creating a view.
        """
        path_str = str(Path(base_path) / wildcard)
        query = f"""
        CREATE OR REPLACE VIEW {table_name} AS 
        SELECT * FROM read_parquet('{path_str}', hive_partitioning=true, union_by_name=true)
        """
        self.con.execute(query)
        self.registered_tables.append(table_name)
        print(f"Partitioned view '{table_name}' created for files at '{path_str}'.")

    def register_local_data_skip_errors(self, base_path, table_name, wildcard="*.parquet"):
        """
        Registers non-partitioned data from base_path (appending a wildcard)
        and skips registration if the directory or matching files are missing.
        """
        p = Path(base_path)
        if not p.exists():
            print(f"Skipping local {table_name}: directory does not exist => {base_path}")
            return
        pattern = str(p / wildcard)
        if not glob.glob(pattern):
            print(f"Skipping local {table_name}: no .parquet files found with pattern => {pattern}")
            return
        try:
            self.register_data([p / wildcard], [table_name])
        except Exception as e:
            print(f"Skipping local {table_name}: encountered error => {e}")

    def register_partitioned_data_skip_errors(self, base_path, table_name, wildcard="*/*/*.parquet"):
        """
        Registers partitioned data from base_path (using hive partitioning and a wildcard).
        Skips registration if the directory or matching files are missing.
        """
        p = Path(base_path)
        if not p.exists():
            print(f"Skipping partitioned {table_name}: directory does not exist => {base_path}")
            return
        pattern = str(p / wildcard)
        if not glob.glob(pattern):
            print(f"Skipping partitioned {table_name}: no .parquet files found with pattern => {pattern}")
            return
        try:
            self.register_partitioned_data(base_path, table_name, wildcard=wildcard)
        except Exception as e:
            print(f"Skipping partitioned {table_name}: encountered error => {e}")
