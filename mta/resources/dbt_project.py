from pathlib import Path
from dagster_dbt import DbtProject

# Adjust the path to point to nfl/transformations/dbt
dbt_project = DbtProject(
    project_dir=Path(__file__).parent.parent.joinpath("transformations", "dbt").resolve(),  # Correct relative path
)

dbt_project.prepare_if_dev()
