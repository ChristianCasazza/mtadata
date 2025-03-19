import os
import requests
import polars as pl
from dagster import ConfigurableIOManager, OutputContext, InputContext

# (NEW) For concurrency
from concurrent.futures import ThreadPoolExecutor, as_completed


class FastOpenDataPartitionedParquetIOManager(ConfigurableIOManager):
    base_dir: str

    def handle_output(self, context: OutputContext, obj):
        """
        Download month-by-month partitioned data for the asset.
        We now parallelize the month-based downloads to reduce overall runtime.
        """
        asset_name = context.asset_key.to_python_identifier()
        context.log.info(f"[FastOpenDataPartitionedParquetIOManager] handle_output for '{asset_name}'")

        # -- 1) Parse the date range from the returned object (dict) --
        if not isinstance(obj, dict):
            context.log.warn(
                f"Asset '{asset_name}' did not return a dict with start/end date. Nothing to do."
            )
            return

        start_year = obj.get("start_year")
        start_month = obj.get("start_month")
        end_year = obj.get("end_year")
        end_month = obj.get("end_month")

        if not all([start_year, start_month, end_year, end_month]):
            context.log.warn(
                f"Asset '{asset_name}' returned incomplete date range. Nothing to do."
            )
            return

        # -- 2) Generate all (year, month) partitions --
        partitions = []
        current_year = start_year
        current_month = start_month

        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            partitions.append((current_year, current_month))
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

        # -- 3) Download partitions in parallel --
        downloaded_files_total = []
        max_workers = 8  # Adjust concurrency based on your environment

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_partition = {}
            for (yr, mo) in partitions:
                # Submit each partition download as a separate task
                future = executor.submit(self._download_partition, asset_name, yr, mo, context)
                future_to_partition[future] = (yr, mo)

            # Collect results as they complete
            for future in as_completed(future_to_partition):
                result = future.result()  # list of downloaded file paths
                downloaded_files_total.extend(result)

        context.log.info(
            f"[FastOpenDataPartitionedParquetIOManager] Downloaded {len(downloaded_files_total)} files "
            f"total for asset '{asset_name}'."
        )

    def _download_partition(self, asset_name: str, year: int, month: int, context):
        """
        Download all batch files for a single year-month partition of a given asset.
        """
        # Remote partition URL: e.g. https://fastopendata.org/<asset_name>/year=YYYY/month=MM/
        remote_dir = f"https://fastopendata.org/{asset_name}/year={year:04d}/month={month:02d}/"

        # Local partition dir: base_dir/asset_name/year=YYYY/month=MM
        local_dir = os.path.join(self.base_dir, asset_name, f"year={year:04d}", f"month={month:02d}")
        os.makedirs(local_dir, exist_ok=True)

        downloaded = []
        batch_num = 1
        while True:
            file_name = f"{asset_name}_{year:04d}{month:02d}_{batch_num}.parquet"
            file_url = remote_dir + file_name

            # HEAD check to see if file exists
            resp_head = requests.head(file_url)
            if resp_head.status_code == 404:
                break  # no more
            if resp_head.status_code != 200:
                context.log.warn(f"HTTP {resp_head.status_code} on {file_url}, aborting.")
                break

            local_file_path = os.path.join(local_dir, file_name)
            self._download_file(file_url, local_file_path)
            downloaded.append(local_file_path)

            batch_num += 1

        if downloaded:
            context.log.info(
                f"  - Downloaded {len(downloaded)} files for {asset_name} year={year}, month={month}"
            )
        return downloaded

    def _download_file(self, file_url: str, local_path: str):
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    def load_input(self, context: InputContext):
        """
        If you actually need to load the data. This example will load everything
        the handle_output just downloaded.
        """
        asset_name = context.asset_key.to_python_identifier()
        local_asset_dir = os.path.join(self.base_dir, asset_name)
        if not os.path.exists(local_asset_dir):
            return None

        # Recursively find all .parquet
        parquet_paths = []
        for root, dirs, files in os.walk(local_asset_dir):
            for file in files:
                if file.endswith(".parquet"):
                    parquet_paths.append(os.path.join(root, file))

        if not parquet_paths:
            return None

        dfs = [pl.read_parquet(p) for p in sorted(parquet_paths)]
        return pl.concat(dfs) if dfs else None
