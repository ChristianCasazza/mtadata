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

# Step 4: Ask user for NYC_API_KEY
echo "Please enter your NYC_API_KEY (press Enter to use the default community key):"
read -r NYC_API_KEY
NYC_API_KEY=${NYC_API_KEY:-uHoP8dT0q1BTcacXLCcxrDp8z}

echo "Using NYC_API_KEY: $NYC_API_KEY"
if [ "$NYC_API_KEY" == "uHoP8dT0q1BTcacXLCcxrDp8z" ]; then
    echo "Note: This is the default community key. Please use your own key if possible."
fi

# Step 5: Copy .env.example to .env
cp .env.example .env || { echo "Failed to copy .env.example to .env"; exit 1; }

# Step 6: Run exportpath.py to generate LAKE_PATH
LAKE_PATH=$(uv run scripts/exportpath.py)
if [ -z "$LAKE_PATH" ]; then
    echo "Error: Failed to generate LAKE_PATH"
    exit 1
fi

# Step 7: Add NYC_API_KEY and LAKE_PATH to .env
echo "NYC_API_KEY=$NYC_API_KEY" >> .env
echo "LAKE_PATH=$LAKE_PATH" >> .env

# Step 8: Run Dagster development server
echo "Starting Dagster development server..."
dagster dev
