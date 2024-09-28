import io
import zipfile

import httpx
import polars as pl
from dagster import asset
from slugify import slugify


@asset(
    compute_kind="Polars",  # Specify the compute kind
)
def nfl_pbp_2024() -> pl.DataFrame:
    """
    NFL pbp data from NFLFastR
    """
    nfl_pbp_2024_url = (
        "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2024.parquet"
    )

    return pl.read_parquet(nfl_pbp_2024_url)

@asset(
    group_name="nfl_seeds",
    compute_kind="Polars",  # Specify the compute kind
)
def nfl_players() -> pl.DataFrame:
    """
    NFL player ids from NFLFastR
    """
    nfl_pbp_2024_url = (
        "https://github.com/nflverse/nflverse-data/releases/download/players/players.parquet"
    )

    return pl.read_parquet(nfl_pbp_2024_url)

@asset(
    compute_kind="Polars",
    group_name="nfl_seeds",  # Specify the compute kind
)
def nfl_weekly_rosters_2024() -> pl.DataFrame:
    """
    NFL weekly rosters for the 2024 season
    """
    weekly_rosters_url = (
        "https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_2024.parquet"
    )

    return pl.read_parquet(weekly_rosters_url)

@asset(
    group_name="nfl_seeds",
    compute_kind="Polars",  # Specify the compute kind
)
def nfl_officials() -> pl.DataFrame:
    """
    NFL officials' data
    """
    officials_url = (
        "https://github.com/nflverse/nflverse-data/releases/download/officials/officials.parquet"
    )

    return pl.read_parquet(officials_url)