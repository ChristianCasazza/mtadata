from pathlib import Path
from dagster_dbt import DbtCliResource

# Correct the path to point directly to the dbt directory and the profiles.yml location
dbt_project = DbtCliResource(
    project_dir=Path(__file__).parent.parent.joinpath("transformations", "dbt").resolve(),  # Correct relative path to the dbt project
    profiles_dir=Path(__file__).parent.parent.joinpath("transformations", "dbt").resolve()  # Point to the correct path for profiles.yml
)
