from pathlib import Path
from dagster_dbt import DbtProject

dbt_project = DbtProject(
    project_dir=Path(__file__).parent.joinpath("dbt").resolve(),  # Correctly reference the dbt directory
)

dbt_project.prepare_if_dev()
