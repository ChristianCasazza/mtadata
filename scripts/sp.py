# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "polars",
#     "rich",
# ]
# ///

import sys
import polars as pl
from rich.console import Console
from rich.table import Table

def show_parquet_schema(file_path):
    """
    Reads a Parquet file using Polars and prints its schema 
    and row count using Rich formatting.
    """
    try:
        df = pl.read_parquet(file_path)

        console = Console()
        schema_table = Table(title="Parquet Schema", title_style="bold green")
        schema_table.add_column("Column Name", justify="left", style="bold yellow", no_wrap=True)
        schema_table.add_column("Data Type", justify="left", style="bold cyan")

        for col_name, col_dtype in df.schema.items():
            schema_table.add_row(col_name, str(col_dtype), style="white on black")

        console.print(schema_table)
        console.print(f"[bold magenta]\nNumber of rows:[/] [bold white]{df.height}[/]")
    
    except Exception as e:
        console = Console()
        console.print(f"[bold red]Error:[/] {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python show_parquet_schema.py <path_to_parquet_file>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    show_parquet_schema(file_path)
