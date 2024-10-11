from mta.resources.polars_parquet_io_manager import PolarsParquetIOManager

# Custom IO manager for hourly subway data
hourly_subway_io_manager = PolarsParquetIOManager(
    base_dir="../../data/mta/new/hourly_subway_data"
)

# Custom IO manager for daily subway data
daily_subway_io_manager = PolarsParquetIOManager(
    base_dir="../../data/mta/new/daily_subway_data"
)

# Custom IO manager for bus speeds data
bus_speeds_io_manager = PolarsParquetIOManager(
    base_dir="../../data/mta/new/bus_speeds_data"
)

# Define the IO manager for the mta_bus_wait_time asset
bus_wait_time_io_manager = PolarsParquetIOManager(
    base_dir="../../data/mta/new/bus_wait_time_data"
)

# Define the IO manager for the mta_operations_statement asset
operations_statement_io_manager = PolarsParquetIOManager(
    base_dir="../../data/mta/new/operations_statement_data"
)
