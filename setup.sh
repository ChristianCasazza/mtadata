#!/bin/bash

# Step 1: Create the virtual environment
uv venv

# Step 2: Activate the virtual environment
if [ -f .venv/bin/activate ]; then
    source .venv/bin/activate
else
    echo "Error: Virtual environment not found at .venv/bin/activate"
    exit 1
fi

# Step 3: Install dependencies
uv sync || { echo "Failed to sync dependencies"; exit 1; }

# Step 4: Check existing .env file and its contents
if [ -f .env ]; then
    # Source the .env file to get variables
    source .env
    
    # Check if all required variables exist and CREATE_NEW_ENV is FALSE
    if [ "$CREATE_NEW_ENV" = "FALSE" ] && \
       [ -n "$DAGSTER_HOME" ] && \
       [ -n "$WAREHOUSE_PATH" ] && \
       [ -n "$SOCRATA_API_TOKEN" ]; then
        echo "Existing valid .env configuration found, skipping setup..."
        # Jump straight to starting dagster
        echo "Starting Dagster development server..."
        dagster dev
        exit 0
    fi
    
    # If CREATE_NEW_ENV is TRUE, delete the existing .env
    if [ "$CREATE_NEW_ENV" = "TRUE" ]; then
        rm -f .env
    fi
fi

# Step 5: Ask user for SOCRATA_API_TOKEN
echo "Please enter your SOCRATA_API_TOKEN (press Enter to use the default community token):"
read -r SOCRATA_API_TOKEN
SOCRATA_API_TOKEN=${SOCRATA_API_TOKEN:-uHoP8dT0q1BTcacXLCcxrDp8z}

echo "Using SOCRATA_API_TOKEN: $SOCRATA_API_TOKEN"
if [ "$SOCRATA_API_TOKEN" == "uHoP8dT0q1BTcacXLCcxrDp8z" ]; then
    echo "Note: This is the default community token. Please use your own token if possible."
fi

# Step 6: Create new .env file if it doesn't exist or was deleted
touch .env

# Step 7: Run exportpathlinux.py to retrieve WAREHOUSE_PATH and DAGSTER_HOME
BASH_VERSION_MAJOR=$(echo "$BASH_VERSION" | cut -d. -f1)

if [ "$BASH_VERSION_MAJOR" -ge 4 ]; then
    # Use readarray (Bash 4+)
    readarray -t PATHS < <(uv run scripts/exportpathlinux.py)
else
    # Fallback for older Bash versions
    PATHS=()
    while IFS= read -r line; do
        PATHS+=("$line")
    done < <(uv run scripts/exportpathlinux.py)
fi

WAREHOUSE_PATH="${PATHS[0]}"
DAGSTER_HOME="${PATHS[1]}"

if [ -z "$WAREHOUSE_PATH" ] || [ -z "$DAGSTER_HOME" ]; then
    echo "Error: Failed to retrieve WAREHOUSE_PATH or DAGSTER_HOME"
    exit 1
fi

# Step 8: Append env vars to .env including CREATE_NEW_ENV=FALSE
{
    echo "SOCRATA_API_TOKEN=$SOCRATA_API_TOKEN"
    echo "WAREHOUSE_PATH=$WAREHOUSE_PATH"
    echo "DAGSTER_HOME=$DAGSTER_HOME"
    echo "CREATE_NEW_ENV=FALSE"
} >> .env

# Step 9: Generate dagster.yaml in DAGSTER_HOME
mkdir -p "$DAGSTER_HOME"
uv run scripts/generate_dagsteryaml.py "$DAGSTER_HOME" > "$DAGSTER_HOME/dagster.yaml"

# Step 10: Launch Dagster
echo "Starting Dagster development server..."
dagster dev