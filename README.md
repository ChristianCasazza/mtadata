# What is this Repo?

This repository serves as a local data platform for working with MTA data. It encompasses the following key functionalities:

- **Data Ingestion**: Fetches data from the Socrata API.
- **Data Cleaning**: Performs necessary cleaning and preprocessing of the ingested data.
- **SQL Transformation Pipeline**: Executes a series of SQL transformations to prepare the data for analysis.
- **Data Visualization**: Generates insights and visualizes them through a data application.

This end-to-end workflow enables efficient data processing and insight generation from MTA datasets.

What does this repo use
 
# Project Setup Guide

This project assumes you are using a code IDE, either locally such as with [VSCode](https://code.visualstudio.com/docs/setup/setup-overview) or with [Github Codespaces](
https://docs.github.com/en/codespaces/getting-started/quickstart). Codespaces can be run by first making a free Github account, clicking the green **Code** button at the top of this repo, and then selecting **Codespaces**.

If coding locally, [this guide](https://github.com/theopendatastackofficial/get-started-coding-locally) will help you get setup with VSCode(free code interface), WSL(for windows users), uv(for python) bun(for js/ts) and git/Github(for saving and sharing)

## 1. Install uv for python code

**uv** is an extremely fast Python package manager that simplifies creating and managing Python projects. We’ll install it first to ensure our Python environment is ready to go. uv makes working with python both faster and simpler.

### Install uv

In VSCode, at the top of the screen, you will see an option that says **Terminal**. Then, you should click the button, and then select **New Terminal**. Then, copy and paste the following commands to install uv. You can double check they are the correct scripts by going to the official uv site, maintained by the company astral.

[Install uv](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer)

```macOS, WSL, and Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

If you are using Windows and also not using WSL.

Windows Powershell:
```sh
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Test your install by running in the terminal:
```sh
uv
```

#### Expected output:
If uv was installed correctly, you should see a help message that starts with:

```sh
An extremely fast Python package manager.

Usage: uv [OPTIONS] <COMMAND>

Commands:
  run      Run a command or script
  init     Create a new project
  ...
  help     Display documentation for a command
```

#### If you get an error:
- Copy and paste the exact error message into ChatGPT and explain you’re having problems using uv.
- ChatGPT can help troubleshoot your specific error message.

If uv displays its usage information without an error, congratulations! You’re all set to work with Python in your local environment.

## 2. Clone the Repository

To clone the repository, run the following command:

```bash
git clone https://github.com/ChristianCasazza/mtadata 
```

You can also make the repo have a custom name by adding it at the end:

```bash
git clone https://github.com/ChristianCasazza/mtadata custom_name
```


In VSCode, on the left side of the screen, there will be a vertical bar, and the icon with two pieces of paper will be called **Explorer**. By clicking on this, you can open click **open folder** and then it will be called mtadata or your custom name. 

## 3. Setup the Project

You will need a Socrata API key. Please use your own key if possible. You can obtain one in two minutes by signing up [here](https://evergreen.data.socrata.com/signup) and following [these instructions](https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys). You want to obtain your Socrata App token, at the bottom. Keep this handy to copy and paste in the rest of this step.

This repository includes two setup scripts:
- **`setup.sh`**: For Linux/macOS
- **`setup.bat`**: For Windows

These scripts automate the following tasks:
1. Create and activate a virtual environment using `uv`.
2. Install project dependencies.
3. Ask for your Socrata App Token (`SOCRATA_API_TOKEN`). If no key is provided, the script will use the community key: `uHoP8dT0q1BTcacXLCcxrDp8z`.
   - **Important**: The community key is shared and rate-limited, please get your own token.
4. Creates a `.env` and appends `SOCRATA_API_TOKEN`, `WAREHOUSE_PATH`(to tell DBT where your DuckDB file is) and `DAGSTER_HOME`(for your logs) to the .env file.
- **Note**: Your personal .env file won't appear in Github because of its presence in the .gitignore file
65 Start the Dagster development server.

### Run the Setup Script

In your VSCode window, click **Terminal** at the top, and then **New Terminal**. Then, copy and paste the following command: 

#### On Linux/macOS:
```bash
./setup.sh
```

If you encounter a `Permission denied` error, ensure the script is executable by running:
```bash
chmod +x setup.sh
```

#### On Windows:
```cmd
setup.bat
```

If PowerShell does not recognize the script, ensure you're in the correct directory and use `.\` before the script name:
```powershell
.\setup.bat
```

The script will guide you through the setup interactively. Once complete, your `.env` file will be configured, and the Dagster server will be running.

## 4. Access Dagster

After the setup script finishes, you can access the Dagster web UI. The script will display a URL in the terminal. Click on the URL or paste it into your browser to access the Dagster interface.

## 5. Materialize Assets

1. In the Dagster web UI, click on the **Assets** tab in the top-left corner.
2. Then, in the top-right corner, click on **View Global Asset Lineage**.
3. In the top-right corner, click **Materialize All** to start downloading and processing all of the data.

This will execute the following pipeline:

1. Ingest MTA data from the Socrata API, weather data from the Open Mateo API, and the 70M rows mta_subway_hourly_ridsership  dataset from R2 as partitioned parquet files.
2. Create a DuckDB file with views on each raw dataset's parquet files.
3. Execute a SQL transformation pipeline with DBT on the raw datasets.

The entire pipeline should take 1-10 minutes, depending on your machine's strenth and internet download speed. Most of the time spent is ingesting the large hourly dataset.


# Running the Data Dictionary UI

After your pipeline run finishes, you can run a local UI to view the datasets we have downloaded and view their schema. Setting the toggle to LLM mode makes it easier to copy and paste a table to an LLM.

## Running the UI

To run the UI, open a new terminal from your existing Dagster instance. Then, run the command:

```
uv run scripts/create.py app
```

# Querying the data for Ad-hoc analysis

## Querying the data with the Harlequin SQL Editor

### Step 1: Activating Harlequin

Harlequin is a terminal based local SQL editor.

To start it, open a new terminal, then, run the following command to use the Harlequin SQL editor with uv:


```bash
uvx harlequin
```
Then use it to connect to the duckdb file we created during our pipeline.

```bash
harlequin app/sources/app/data.duckdb
```

### Step 2: Query the Data

The duckdb file will already have the views to the tables to query. it can be queried like

```sql
SELECT 
    COUNT(*) AS total_rows,
    MIN(transit_timestamp) AS min_transit_timestamp,
    MAX(transit_timestamp) AS max_transit_timestamp
FROM mta_hourly_subway_ridership
```

This query will return the total number of rows, the earliest timestamp, and the latest timestamp in the dataset.

## Working in a notebook

### Overview


The `DuckDBWrapper` class provides a simple interface to interact with DuckDB, allowing you to register data files (Parquet, CSV, JSON), execute queries, and export results in multiple formats.

---

### Installation and Initialization

In the top right corner of your notebook, select your .venv in python enviornments. If using VScode, it may suggest to install Jupyter and python extensions.

Then, in the notebook, you just need to run the first two cells. The first cell will load the DuckDBWrapper Class. Then, you can initialize a `DuckDBWrapper` instance in the second cell with:

#### Initialize an in-memory DuckDB instance

```bash
con = DuckDBWrapper()
```
#### Initialize a persistent DuckDB database

```

```bash
con = DuckDBWrapper("my_database.duckdb")
```

You can run the rest of the cells to learn how to utilize the class.


# How to Run the Data App UI

## Step 1: Open a New Terminal

## Step 2: Ensure Bun is installed

[Bun](https://bun.sh/docs) is an extremely fast runtime and package manager that simplifies creating and managing JavaScript (JS) or TypeScript (TS) projects. We’ll install it first to ensure our Js/TS environment is ready to go. Bun makes working with JS and TS code both faster and simpler.

[Install Bun](https://bun.sh/docs/installation)

```macOS, WSL, and Linux
curl -fsSL https://bun.sh/install | bash 
```

If you are using Windows and also not using WSL.

Windows Powershell:
```sh
powershell -c "irm bun.sh/install.ps1|iex"
```


### Test Your Bun Installation

#### Open the Terminal in VSCode
- In VSCode, look at the top menu and select **Terminal > New Terminal**

#### Run the command:
```sh
bun
```

#### Expected output:
If Bun was installed correctly, you should see a help message that starts with something like this:

```sh
Bun is a fast JavaScript runtime, package manager, bundler, and test runner. (1.2.4+fd9a5ea66)

Usage: bun <command> [...flags] [...args]

Commands:
  run       ./my-script.ts       Execute a file with Bun
            lint                 Run a package.json script
  test                           Run unit tests with Bun
  x         prettier             Execute a package binary (CLI), installing if needed (bunx)
  repl                           Start a REPL session with Bun
  exec                           Run a shell script directly with Bun

  install                        Install dependencies for a package.json (bun i)
```

#### Again, if you get an error:
- Copy and paste the exact error message into ChatGPT and explain you’re having problems installing and using bun.
- ChatGPT can help troubleshoot your specific error message.

---

## Step 3: Run the script

```
bun scripts/run.js
```

This script will do the following

```bash
cd app
```

```bash
bun install
```

```bash
bun run sources
```

```bash
bun run dev
```

This will open up the Data App UI, and it will be running on your local machine. You should be able to access it by visiting the address shown in your terminal, typically `http://localhost:3000`.
