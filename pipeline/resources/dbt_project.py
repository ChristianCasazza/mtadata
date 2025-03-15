from pathlib import Path
from dagster_dbt import DbtProject

# Get the root of the project (two levels up from this script)
ROOT_DIR = Path(__file__).parent.parent.parent.resolve()

# Set the path to MTA's DBT project folder
dbt_project = DbtProject(
    project_dir=ROOT_DIR.joinpath("transformations", "dbt"),  # The DBT project is in your_project_root/transformations/dbt
)

dbt_project.prepare_if_dev()  # Prepare if in development mode
