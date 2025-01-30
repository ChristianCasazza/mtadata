# mta/assets/ingestion/sf_assets.py

import gc
import polars as pl
from datetime import datetime
from dagster import asset
from mta.utils.socrata_api import SocrataAPI, SocrataAPIConfig
from mta.assets.ingestion.mta_assets import get_io_manager  

# 1. A custom config class that extends SocrataAPIConfig
class SFAirTrafficCargoConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.sfgov.org/resource/u397-j8nr.json"
    order: str = "Activity_Period_Start_Date ASC"
    limit: int = 500000
    offset: int = 0

# 2. A helper function to process the DataFrame: rename columns, cast types, parse dates, etc.
def process_sf_air_traffic_cargo_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    to_cast = []
    # For each column, only cast if it exists in the data
    if "activity_period" in df.columns:
        to_cast.append(pl.col("activity_period").cast(pl.Int64))
    if "activity_period_start_date" in df.columns:
        to_cast.append(pl.col("activity_period_start_date").str.strptime(pl.Datetime, strict=False))
    if "cargo_weight_lbs" in df.columns:
        to_cast.append(pl.col("cargo_weight_lbs").cast(pl.Float64))
    if "cargo_metric_tons" in df.columns:
        to_cast.append(pl.col("cargo_metric_tons").cast(pl.Float64))
    if "data_as_of" in df.columns:
        to_cast.append(pl.col("data_as_of").str.strptime(pl.Datetime, strict=False))
    if "data_loaded_at" in df.columns:
        to_cast.append(pl.col("data_loaded_at").str.strptime(pl.Datetime, strict=False))

    if to_cast:
        df = df.with_columns(to_cast)

    return df

# 3. Define the asset function
@asset(
    name="sf_air_traffic_cargo",
    io_manager_key="sf_air_traffic_cargo_polars_parquet_io_manager",  # We'll define this IO manager in constants.py
    compute_kind="Polars",
)
def sf_air_traffic_cargo(context):
    config = SFAirTrafficCargoConfig()
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching SF Air Cargo data with offset: {offset} ...")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more SF Air Cargo data to fetch at offset={offset}.")
            return  # End asset execution

        # Convert raw list-of-dicts to Polars
        raw_df = pl.DataFrame(data)
        processed_df = process_sf_air_traffic_cargo_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)  # Reuse the existing get_io_manager approach
            io_manager.handle_output(context, processed_df, batch_number=batch_number)
        
        # Clean up memory between batches
        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1


class SFAirTrafficPassengerStatsConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.sfgov.org/resource/rkru-6vcg.json"
    order: str = "Activity_Period_Start_Date ASC"
    limit: int = 500000
    offset: int = 0

def process_sf_air_traffic_passenger_stats_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    # Conditionally rename if original columns are present
    rename_map = {}
    if "Activity Period" in df.columns:
        rename_map["Activity Period"] = "activity_period"
    if "Activity Period Start Date" in df.columns:
        rename_map["Activity Period Start Date"] = "activity_period_start_date"
    if "Operating Airline" in df.columns:
        rename_map["Operating Airline"] = "operating_airline"
    if "Operating Airline IATA Code" in df.columns:
        rename_map["Operating Airline IATA Code"] = "operating_airline_iata_code"
    if "Published Airline" in df.columns:
        rename_map["Published Airline"] = "published_airline"
    if "Published Airline IATA Code" in df.columns:
        rename_map["Published Airline IATA Code"] = "published_airline_iata_code"
    if "GEO Summary" in df.columns:
        rename_map["GEO Summary"] = "geo_summary"
    if "GEO Region" in df.columns:
        rename_map["GEO Region"] = "geo_region"
    if "Activity Type Code" in df.columns:
        rename_map["Activity Type Code"] = "activity_type_code"
    if "Price Category Code" in df.columns:
        rename_map["Price Category Code"] = "price_category_code"
    if "Terminal" in df.columns:
        rename_map["Terminal"] = "terminal"
    if "Boarding Area" in df.columns:
        rename_map["Boarding Area"] = "boarding_area"
    if "Passenger Count" in df.columns:
        rename_map["Passenger Count"] = "passenger_count"
    # data_as_of, data_loaded_at typically are already in snake or lower, but rename if needed
    if "data_as_of" in df.columns:
        rename_map["data_as_of"] = "data_as_of"
    if "data_loaded_at" in df.columns:
        rename_map["data_loaded_at"] = "data_loaded_at"

    if rename_map:
        df = df.rename(rename_map)

    # Cast columns if they exist
    casts = []
    if "activity_period" in df.columns:
        casts.append(pl.col("activity_period").cast(pl.Int64))
    if "activity_period_start_date" in df.columns:
        casts.append(
            pl.col("activity_period_start_date").str.strptime(pl.Datetime, strict=False)
        )
    if "passenger_count" in df.columns:
        casts.append(pl.col("passenger_count").cast(pl.Float64))
    if "data_as_of" in df.columns:
        casts.append(pl.col("data_as_of").str.strptime(pl.Datetime, strict=False))
    if "data_loaded_at" in df.columns:
        casts.append(pl.col("data_loaded_at").str.strptime(pl.Datetime, strict=False))

    if casts:
        df = df.with_columns(casts)

    return df


@asset(
    name="sf_air_traffic_passenger_stats",
    io_manager_key="sf_air_traffic_passenger_stats_polars_parquet_io_manager",
    compute_kind="Polars",
)
def sf_air_traffic_passenger_stats(context):
    config = SFAirTrafficPassengerStatsConfig()
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching SF Air Passenger Stats with offset: {offset} ...")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more passenger stats data at offset={offset}.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_sf_air_traffic_passenger_stats_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number=batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1

class SFAirTrafficLandingsConfig(SocrataAPIConfig):
    SOCRATA_ENDPOINT: str = "https://data.sfgov.org/resource/fpux-q53t.json"
    order: str = "Activity_Period_Start_Date ASC"
    limit: int = 500000
    offset: int = 0

def process_sf_air_traffic_landings_df(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    # Conditionally rename if old columns are present
    rename_map = {}
    if "Activity Period" in df.columns:
        rename_map["Activity Period"] = "activity_period"
    if "Activity Period Start Date" in df.columns:
        rename_map["Activity Period Start Date"] = "activity_period_start_date"
    if "Operating Airline" in df.columns:
        rename_map["Operating Airline"] = "operating_airline"
    if "Operating Airline IATA Code" in df.columns:
        rename_map["Operating Airline IATA Code"] = "operating_airline_iata_code"
    if "Published Airline" in df.columns:
        rename_map["Published Airline"] = "published_airline"
    if "Published Airline IATA Code" in df.columns:
        rename_map["Published Airline IATA Code"] = "published_airline_iata_code"
    if "GEO Summary" in df.columns:
        rename_map["GEO Summary"] = "geo_summary"
    if "GEO Region" in df.columns:
        rename_map["GEO Region"] = "geo_region"
    if "Landing Aircraft Type" in df.columns:
        rename_map["Landing Aircraft Type"] = "landing_aircraft_type"
    if "Aircraft Body Type" in df.columns:
        rename_map["Aircraft Body Type"] = "aircraft_body_type"
    if "Aircraft Manufacturer" in df.columns:
        rename_map["Aircraft Manufacturer"] = "aircraft_manufacturer"
    if "Aircraft Model" in df.columns:
        rename_map["Aircraft Model"] = "aircraft_model"
    if "Aircraft Version" in df.columns:
        rename_map["Aircraft Version"] = "aircraft_version"
    if "Landing Count" in df.columns:
        rename_map["Landing Count"] = "landing_count"
    if "Total Landed Weight" in df.columns:
        rename_map["Total Landed Weight"] = "total_landed_weight"

    if rename_map:
        df = df.rename(rename_map)

    # Cast columns if they exist
    casts = []
    if "activity_period" in df.columns:
        casts.append(pl.col("activity_period").cast(pl.Int64))
    if "activity_period_start_date" in df.columns:
        casts.append(
            pl.col("activity_period_start_date").str.strptime(pl.Datetime, strict=False)
        )
    if "landing_count" in df.columns:
        casts.append(pl.col("landing_count").cast(pl.Float64))
    if "total_landed_weight" in df.columns:
        casts.append(pl.col("total_landed_weight").cast(pl.Float64))

    if casts:
        df = df.with_columns(casts)

    return df


@asset(
    name="sf_air_traffic_landings",
    io_manager_key="sf_air_traffic_landings_polars_parquet_io_manager",
    compute_kind="Polars",
)
def sf_air_traffic_landings(context):
    config = SFAirTrafficLandingsConfig()
    api_client = SocrataAPI(config)
    offset = config.offset
    batch_number = 1

    while True:
        context.log.info(f"Fetching SF Air Landings with offset: {offset} ...")
        updated_config = config.copy(update={"offset": offset})
        api_client.config = updated_config
        data = api_client.fetch_data()

        if not data:
            context.log.info(f"No more landings data at offset={offset}.")
            return

        raw_df = pl.DataFrame(data)
        processed_df = process_sf_air_traffic_landings_df(raw_df)

        if not processed_df.is_empty():
            io_manager = get_io_manager(context)
            io_manager.handle_output(context, processed_df, batch_number=batch_number)

        del raw_df, processed_df, data
        gc.collect()

        offset += config.limit
        batch_number += 1
