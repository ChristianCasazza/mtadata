import duckdb
from pathlib import Path
import polars as pl
import polars.selectors as cs
import glob

try:
    from IPython.display import display, HTML
    IN_NOTEBOOK = True
except ImportError:
    IN_NOTEBOOK = False

try:
    from great_tables import loc, style
    HAS_GREAT_TABLES = True
except ImportError:
    HAS_GREAT_TABLES = False

from rich.console import Console
from rich.table import Table

class DuckDBWrapper:
    def __init__(self, duckdb_path=None):
        if duckdb_path:
            self.con = duckdb.connect(str(duckdb_path), read_only=False)
        else:
            self.con = duckdb.connect(database=':memory:', read_only=False)
        self.registered_tables = []
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")

    def register_data_view(self, paths, table_names):
        """
        Registers local data files (Parquet, CSV, JSON) as views in DuckDB with error skipping.
        Uses CREATE OR REPLACE to overwrite existing views.
        """
        if len(paths) != len(table_names):
            raise ValueError("The number of paths must match the number of table names.")

        for path, table_name in zip(paths, table_names):
            path_str = str(path)
            p = Path(path_str)
            if not glob.glob(path_str):
                print(f"Skipping {table_name}: no files found with pattern => {path_str}")
                continue

            file_extension = p.suffix.lower()
            try:
                if file_extension == ".parquet":
                    query = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{path_str}')"
                elif file_extension == ".csv":
                    query = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv_auto('{path_str}')"
                elif file_extension == ".json":
                    query = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_json_auto('{path_str}')"
                else:
                    raise ValueError(f"Unsupported file type '{file_extension}' for file: {path_str}")

                self.con.execute(query)
                self.registered_tables.append(table_name)
                print(f"View '{table_name}' created for files at '{path_str}'.")
            except Exception as e:
                print(f"Skipping {table_name}: encountered error => {e}")

    def register_data_table(self, paths, table_names):
        """
        Registers local data files (Parquet, CSV, JSON) as tables in DuckDB with error skipping.
        Uses CREATE OR REPLACE to overwrite existing tables.
        """
        if len(paths) != len(table_names):
            raise ValueError("The number of paths must match the number of table names.")

        for path, table_name in zip(paths, table_names):
            path_str = str(path)
            p = Path(path_str)
            if not glob.glob(path_str):
                print(f"Skipping {table_name}: no files found with pattern => {path_str}")
                continue

            file_extension = p.suffix.lower()
            try:
                if file_extension == ".parquet":
                    query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{path_str}')"
                elif file_extension == ".csv":
                    query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{path_str}')"
                elif file_extension == ".json":
                    query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{path_str}')"
                else:
                    raise ValueError(f"Unsupported file type '{file_extension}' for file: {path_str}")

                self.con.execute(query)
                self.registered_tables.append(table_name)
                print(f"Table '{table_name}' created for files at '{path_str}'.")
            except Exception as e:
                print(f"Skipping {table_name}: encountered error => {e}")

    def bulk_register_data(self, repo_root, base_path, table_names, wildcard="*.parquet", as_table=False, show_tables=False):
        paths = [Path(repo_root) / base_path / table_name / wildcard for table_name in table_names]
        
        if as_table:
            self.register_data_table(paths, table_names)
        else:
            self.register_data_view(paths, table_names)

        if show_tables:
            self.show_tables()

    def register_partitioned_data_view(self, base_path, table_name, wildcard="*/*/*.parquet"):
        """
        Registers partitioned Parquet data as a view using Hive partitioning with error skipping.
        Uses CREATE OR REPLACE to overwrite existing views.
        """
        partition_path = Path(base_path)
        if not partition_path.exists():
            print(f"Skipping partitioned {table_name}: directory does not exist => {partition_path}")
            return

        path_str = str(partition_path / wildcard)
        if not glob.glob(path_str):
            print(f"Skipping partitioned {table_name}: no .parquet files found with pattern => {path_str}")
            return

        try:
            query = f"""
            CREATE OR REPLACE VIEW {table_name} AS 
            SELECT * FROM read_parquet('{path_str}', hive_partitioning=true, union_by_name=true)
            """
            self.con.execute(query)
            self.registered_tables.append(table_name)
            print(f"Partitioned view '{table_name}' created for files at '{path_str}'.")
        except Exception as e:
            print(f"Skipping partitioned {table_name}: encountered error => {e}")

    def register_partitioned_data_table(self, base_path, table_name, wildcard="*/*/*.parquet"):
        """
        Registers partitioned Parquet data as a table using Hive partitioning with error skipping.
        Uses CREATE OR REPLACE to overwrite existing tables.
        """
        partition_path = Path(base_path)
        if not partition_path.exists():
            print(f"Skipping partitioned {table_name}: directory does not exist => {partition_path}")
            return

        path_str = str(partition_path / wildcard)
        if not glob.glob(path_str):
            print(f"Skipping partitioned {table_name}: no .parquet files found with pattern => {path_str}")
            return

        try:
            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{path_str}', hive_partitioning=true, union_by_name=true)
            """
            self.con.execute(query)
            self.registered_tables.append(table_name)
            print(f"Partitioned table '{table_name}' created for files at '{path_str}'.")
        except Exception as e:
            print(f"Skipping partitioned {table_name}: encountered error => {e}")

    def bulk_register_partitioned_data(self, repo_root, base_path, table_names, wildcard="*/*/*.parquet", as_table=False, show_tables=False):
        for table_name in table_names:
            partition_path = Path(repo_root) / base_path / table_name
            if as_table:
                self.register_partitioned_data_table(
                    base_path=partition_path,
                    table_name=table_name,
                    wildcard=wildcard
                )
            else:
                self.register_partitioned_data_view(
                    base_path=partition_path,
                    table_name=table_name,
                    wildcard=wildcard
                )

        if show_tables:
            self.show_tables()

    def run_query(self, sql_query, show_results=False):
        arrow_table = self.con.execute(sql_query).arrow()
        df = pl.DataFrame(arrow_table)

        if show_results:
            if IN_NOTEBOOK and HAS_GREAT_TABLES:
                styled = (
                    df.style
                    .tab_header(
                        title="DuckDB Query Results",
                        subtitle=f"{sql_query[:50]}..."
                    )
                    .fmt_number(cs.numeric(), decimals=3)
                )
                styled_html = styled._repr_html_()
                scrollable_html = f"""
                <div style="max-width:100%; overflow-x:auto; white-space:nowrap;">
                    {styled_html}
                </div>
                """
                display(HTML(scrollable_html))
            else:
                self.print_query_results(df, title=f"Query: {sql_query[:50]}...")

        return df

    def print_query_results(self, df, title="Query Results"):
        console = Console()
        with console.pager(styles=True):
            table = Table(title=title, title_style="bold green", show_lines=True)
            for column in df.columns:
                table.add_column(str(column), style="bold cyan", overflow="fold")
            for row in df.iter_rows(named=True):
                values = [str(row[col]) for col in df.columns]
                table.add_row(*values, style="white on black")
            console.print(table)

    def show_tables(self):
        query = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema='main'
        """
        df = self.run_query(query)
        console = Console()
        table = Table(title="Registered Tables", title_style="bold green", show_lines=True)
        table.add_column("Table Name", justify="left", style="bold yellow")
        table.add_column("Table Type", justify="left", style="bold cyan")
        for row in df.to_dicts():
            table.add_row(row["table_name"], row["table_type"], style="white on black")
        console.print(table)

    def show_schema(self, table_name):
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        """
        df = self.run_query(query)
        console = Console()
        schema_table = Table(title=f"Schema for '{table_name}'", title_style="bold green")
        schema_table.add_column("Column Name", justify="left", style="bold yellow", no_wrap=True)
        schema_table.add_column("Data Type", justify="left", style="bold cyan")
        for row in df.to_dicts():
            schema_table.add_row(row["column_name"], str(row["data_type"]), style="white on black")
        console.print(schema_table)

    def export(self, result, file_type, path=None, base_path=None, file_name=None, with_header=True):
        file_type = file_type.lower()
        if file_type not in ["parquet", "csv", "json"]:
            raise ValueError("file_type must be one of 'parquet', 'csv', or 'json'.")

        if path:
            full_path = Path(path)
        elif base_path and file_name:
            full_path = Path(base_path) / f"{file_name}.{file_type}"
        else:
            full_path = Path(f"output.{file_type}")
        full_path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(result, pl.DataFrame):
            df = result
        elif hasattr(result, "to_arrow"):
            df = pl.DataFrame(result.to_arrow())
        else:
            raise ValueError("Unsupported result type. Must be a Polars DataFrame or have a 'to_arrow()' method.")

        if file_type == "parquet":
            df.write_parquet(str(full_path))
        elif file_type == "csv":
            df.write_csv(str(full_path), separator=",", include_header=with_header)
        elif file_type == "json":
            df.write_ndjson(str(full_path))

        print(f"File written to: {full_path}")