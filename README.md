
 Project Setup Guide

## 1. Install `uv`

Before proceeding, you will need to install `uv`. Please follow the instructions here: [Install UV](https://docs.astral.sh/uv/getting-started/installation/#installation-methods).

Once `uv` is installed, proceed to clone the repository.

## 2. Clone the Repository

To clone the repository, run the following command:

```bash
git clone <your-repo-url>
```

Then, navigate into the repository directory:

```bash
cd <your-repo-folder>
```

## 3. Create a Virtual Environment with `uv`

With `uv` installed, you can now create the virtual environment. Run the following command:

```bash
uv venv
```

After running this command, `uv` will automatically create a virtual environment for you and display instructions on how to activate it.

### Mac/Linux:
```bash
source .venv/bin/activate
```

### Windows:
```bash
.venv\Scripts\activate
```

## 4. Install Dependencies

With the virtual environment activated, install the required packages:

```bash
pip install -r requirements.txt
```

## 5. Configure Environment Variables

1. Copy the `.env.example` file and rename it to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Open the `.env` file and add your Socrata App Token next to the key `NYC_API_KEY`.

## 6. Start Dagster Development Server

Start the Dagster server by running the following command:

```bash
dagster dev
```

Once the server is running, you will see a URL in your terminal. Click on the URL or paste it into your browser to access the Dagster web UI, which will be running locally.

## 7. Materialize Assets

1. In the Dagster web UI, click on the **Assets** tab in the top-left corner.
2. Then, in the top-right corner, click on **View Global Asset Lineage**.
3. In the top-right corner, click **Materialize All** to start downloading and processing all of the data.



# Explaining the Scripts Folder

The scripts folder contain some python and node scripts to automate running some key repetitive tasks

### 1. `constants.py`
This file contains the paths for key data storage files:
- **`LAKE_PATH`**: The path to the DuckDB file, located inside the `mta/mtastats` folder. This DuckDB file interacts with a Svelte-based Evidence project.
- **`SQLITE_PATH`**: The path to the SQLite file used for metadata management.

### 2. `createlake.py`
This script ingests MTA and weather assets from the `mta/assets` folder and creates views on top of the parquet files for each asset in the DuckDB file. It reads the asset paths and constructs SQL queries to create these views, allowing the DuckDB file to point to the external parquet files without actually storing the data.

### 3. `createmetadata.py`
This script is responsible for creating a SQLite database that stores metadata about the DuckDB tables. It queries the DuckDB file for each table’s PRAGMA information (such as columns, types, etc.) and stores that information in the SQLite database.

### 4. `metadatadescriptions.py`
This script adds column-level descriptions to the assets in the DuckDB file. It references a file called `mta/assets/assets_descriptions.py`, which contains a data dictionary (descriptions for each table). The script loops through each table and its corresponding descriptions and updates the SQLite database with the appropriate metadata for each DuckDB table.

### 5. `rundbt.py`
This script runs the dbt project, which is located in `mta/transformations/dbt`. It changes directories into this folder, then runs `dbt run` to build all dbt models. The dbt project interacts with the DuckDB file, where:
- **Raw files**: Views on the parquet files.
- **dbt tables**: dbt creates materialized DuckDB tables by running SQL queries against the views.

### 6. `app.py`
This script creates a local Flask-based interface for exploring the metadata stored in the SQLite database. The app has two modes:
- **Human**: Displays easy-to-read table information.
- **LLM**: Displays table schemas in a compact format optimized for use with a language model like ChatGPT.
It relies on `templates/index.html` for the app’s user interface and makes API calls to the SQLite database to retrieve the table metadata.

### 7. `create.py`
This is an aggregation script that runs the other scripts in sequence. It supports optional parameters:
- **`uv run scripts/create.py`**: Runs `createlake.py`, `createmetadata.py`, and `metadatadescriptions.py`.
- **`uv run scripts/create.py dbt`**: Runs the same scripts as above, followed by `rundbt.py`.
- **`uv run scripts/create.py app`**: Runs the same scripts as above, followed by `app.py`.
- **`uv run scripts/create.py full`**: Runs all of the above scripts in sequence.

### 8. `create_dbt_sources.py`
This helper script generates a `sources.yml` file for dbt. It creates the required sources structure by scanning the assets from the MTA and weather datasets and formatting them as dbt sources for use in Dagster.

### 9. `run.js`
A Node.js script that automates running the data application locally. It performs the following steps:
1. Changes directory to `mta/mtastats`.
2. Runs `npm install` to ensure all dependencies are installed.
3. Runs `npm run sources` to create the dbt sources file.
4. Launches the app (`npm run dev`).

---

## Usage Instructions

### Setting Up the Environment
1. Ensure that you have all necessary dependencies installed, including Python, Node.js, and dbt.
2. The `mta/mtastats` folder contains the Svelte-based Evidence project, so you’ll need to navigate there and install the required dependencies using `npm install`.

### Running the Scripts

#### Default Data Pipeline
To run the basic data pipeline, execute:
```bash
uv run scripts/create.py
```
This will:
- Ingest the MTA and weather assets (parquet files) and create views in the DuckDB file.
- Extract metadata from DuckDB and store it in a SQLite database.
- Add descriptions for each table's columns in the SQLite database.

#### Running with Additional Options
- To also run the dbt project after completing the basic steps:
  ```bash
  uv run scripts/create.py dbt
  ```
- To launch the Flask-based app after completing the basic steps:
  ```bash
  uv run scripts/create.py app
  ```
- To run both dbt and the app after the basic steps:
  ```bash
  uv run scripts/create.py full
  ```

#### Running the Node.js Application
To run the Svelte-based Evidence app with Node.js:
```bash
node scripts/run.js
```
This will:
1. Change directory to `mta/mtastats`.
2. Install all dependencies (`npm install`).
3. Create dbt sources (`npm run sources`).
4. Launch the app (`npm run dev`).

### Key Points to Remember
- The DuckDB file does not store raw data. Instead, the raw files are views that point to external parquet files.
- When dbt runs, it materializes the views into actual tables within the DuckDB file, executing SQL queries against the views.


---

### Note:
Using `uv run` allows you to execute the Python scripts without manually activating the virtual environment. However, if you prefer, you can activate your virtual environment and run the scripts using `python filename.py` instead.

# Querying the data with the Harlequin SQL Editor

### Step 1: Activating Harlequin

Harlequin is a terminal based local SQL editor.

To start it, open a new terminal, then, run the following command to install the Harlequin SQL editor:


```bash
pip install harlequin
```
Then use it to connect to the duckdb file we created with scripts/create.py

```bash
harlequin mta/mtastats/sources/mta/mtastats.duckdb
```

### Step 2: Query the Data

The duckdb file will already have the views to the tables to query. it can be queried like

```sql
SELECT 
    COUNT(*) AS total_rows,
    MIN(transit_timestamp) AS min_transit_timestamp,
    MAX(transit_timestamp) AS max_transit_timestamp
FROM mta_hourly_subway_socrata
```

This query will return the total number of rows, the earliest timestamp, and the latest timestamp in the dataset.


# How to Run the Data App UI

## Step 1: Open a New Terminal

## Step 2: Check if Node.js is Installed

Before running the app, check if you have Node.js installed by running the following command:

```bash
node -v
```

If Node.js is installed, this will display the current version (e.g., `v16.0.0` or higher). If you see a version number, you're ready to proceed to the next step.

### If Node.js is NOT installed:

1. Go to the [Node.js download page](https://nodejs.org/).
2. Download the appropriate installer for your operating system (Windows, macOS, or Linux).
3. Follow the instructions to install Node.js.

Once installed, verify the installation by running the `node -v` command again to ensure it displays the version number.

## Step 3: Navigate to the `mtastats` Directory

Change to the `mtastats` directory where the app is located by running the following command:

```bash
cd mtastats
```

## Step 4: Install Dependencies

With Node.js installed, run the following command to install the necessary dependencies:

```bash
npm install
```

## Step 5: Start the Data Sources

After installing the dependencies, start the data sources by running:

```bash
npm run sources
```

## Step 6: Run the Data App

Now, run the following command to start the Data App UI locally:

```bash
npm run dev
```

This will open up the Data App UI, and it will be running on your local machine. You should be able to access it by visiting the address shown in your terminal, typically `http://localhost:3000`.
