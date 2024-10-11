
# Project Setup Guide

## 1. Create a Virtual Environment

To begin, create a virtual environment for this project.

### Mac/Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Windows:
```bash
python -m venv .venv
.venv\Scripts\activate
```

Once activated, you should see something like:

```
Using CPython 3.10.15
Creating virtual environment at: .venv
Activate with:
```

## 2. Install Dependencies

With the virtual environment activated, install the required packages:

```bash
pip install -r requirements.txt
```

## 3. Configure Environment Variables

1. Copy the `.env.example` file and rename it to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Open the `.env` file and add your Socrata App Token next to the key `NYC_API_KEY`.

## 4. Start Dagster Development Server

Start the Dagster server by running the following command:

```bash
dagster dev
```

Once the server is running, you will see a URL in your terminal. Click on the URL or paste it into your browser to access the Dagster web UI, which will be running locally.

## 5. Materialize Assets

1. In the Dagster web UI, click on the **Assets** tab in the top-left corner.
2. Then, in the top-right corner, click on **View Global Asset Lineage**.
3. In the top-right corner, click **Materialize All** to start downloading and processing all of the data.

## 6. Run Harlequin SQL Editor

Open a new terminal, reactivate your virtual environment, and run:

```bash
source .venv/bin/activate  # Mac/Linux
.venv\Scripts\activate  # Windows
```

Then, run the following command to open the Harlequin SQL editor:

```bash
uv tool run harlequin
```

This will open up the SQL editor for you to interact with the downloaded data.

## 7. Query the Data with DuckDB

After launching the Harlequin SQL editor (as described in the previous step), you can now query the Parquet files using DuckDB.

### Step 1: Create a View for MTA Hourly Data

First, create a view that reads all the Parquet files in the specified directory. Make sure to replace the path in the query with the correct location of your data files.

For example, if your Parquet files are stored in the `data/mta/new/hourly_subway_data/` folder, you can create a view like this:

```sql
CREATE VIEW mta_hourly AS 
SELECT * 
FROM read_parquet('data/mta/new/hourly_subway_data/*.parquet');
```

This will load all the Parquet files from that directory into the view `mta_hourly`.

### Step 2: Query the Data

Once the view is created, you can run queries against it. For example, to get the total number of rows and the range of timestamps in the dataset, you can run the following query:

```sql
SELECT 
    COUNT(*) AS total_rows,
    MIN(transit_timestamp) AS min_transit_timestamp,
    MAX(transit_timestamp) AS max_transit_timestamp
FROM mta_hourly;
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
