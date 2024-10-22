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
