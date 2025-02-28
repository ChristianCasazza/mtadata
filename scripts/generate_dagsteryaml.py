#!/usr/bin/env python
import sys

def main():
    if len(sys.argv) < 2:
        print("Usage: generate_dagsteryaml.py <DAGSTER_HOME>", file=sys.stderr)
        sys.exit(1)

    dagster_home = sys.argv[1].replace("\\", "/")

    # We'll store an absolute path for base_dir
    # The concurrency is just an example (max_concurrent_runs: 5)
    print(f"""storage:
  sqlite:
    base_dir: "{dagster_home}"
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 5
""")

if __name__ == "__main__":
    main()
