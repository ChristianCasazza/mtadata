from pathlib import Path
from dagster_dbt import DbtProject

# Set the path to MTA's DBT project folder
dbt_project = DbtProject(
    project_dir=Path(__file__).parent.parent.joinpath("transformations", "dbt").resolve(),  # Adjust the path to point to the correct MTA DBT folder
)

dbt_project.prepare_if_dev()  # Prepare if in development mode
