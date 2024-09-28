const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

// Function to run terminal commands
function runCommand(command, workingDirectory = '.') {
  return new Promise((resolve, reject) => {
    exec(command, { cwd: workingDirectory }, (error, stdout, stderr) => {
      if (error) {
        console.error(`Error: ${error.message}`);
        return reject(error);
      }
      if (stderr) {
        console.warn(`Warning: ${stderr}`);
      }
      resolve(stdout);
    });
  });
}

// Main function to generate the project
async function createProject(userProjectName, newFolderName) {
  try {
    // Step 1: Clone the Evidence template with the specified project name
    console.log(`Cloning the project as ${userProjectName}...`);
    await runCommand(`npx degit evidence-dev/template ${userProjectName}`);

    // Step 2: Navigate to the new project directory and install npm dependencies
    console.log('Installing npm dependencies...');
    await runCommand('npm install', `./${userProjectName}`);

    // Step 3: Create the required "sources" and "needful_things" subfolders inside the "sources" folder
    const sourcesDir = path.join(userProjectName, 'sources');
    const needfulThingsDir = path.join(sourcesDir, 'needful_things');
    
    // Check if directories already exist, if not, create them
    if (!fs.existsSync(sourcesDir)) {
      fs.mkdirSync(sourcesDir);
      console.log('Created "sources" directory.');
    } else {
      console.log('"sources" directory already exists.');
    }

    if (!fs.existsSync(needfulThingsDir)) {
      fs.mkdirSync(needfulThingsDir);
      console.log('Created "needful_things" directory inside sources.');
    } else {
      console.log('"needful_things" directory already exists inside sources.');
    }

    // Step 4: Delete the "needful_things.duckdb" file inside the "sources/needful_things" folder
    const duckdbFile = path.join(needfulThingsDir, 'needful_things.duckdb');
    if (fs.existsSync(duckdbFile)) {
      fs.unlinkSync(duckdbFile);
      console.log('Deleted "needful_things.duckdb" inside sources/needful_things.');
    } else {
      console.log('File "needful_things.duckdb" does not exist inside sources/needful_things.');
    }

    // Step 5: Rename the folder "needful_things" to the new folder name inside sources
    const newSubfolderPath = path.join(sourcesDir, newFolderName);
    if (fs.existsSync(needfulThingsDir)) {
      fs.renameSync(needfulThingsDir, newSubfolderPath);
      console.log(`Renamed the "needful_things" folder to ${newFolderName} inside sources.`);
    } else {
      console.error(`Folder "needful_things" does not exist to rename inside sources.`);
    }

    // Step 6: Rename the top-level project folder based on the first input
    const newPath = path.join('.', newFolderName);
    if (fs.existsSync(`./${userProjectName}`)) {
      fs.renameSync(`./${userProjectName}`, newPath);
      console.log(`Renamed the top-level folder to ${newFolderName}`);
    } else {
      console.error(`Folder ${userProjectName} does not exist to rename.`);
    }

    console.log('Project setup completed successfully!');
  } catch (error) {
    console.error(`An error occurred: ${error.message}`);
  }
}

// Taking inputs from the command line arguments
const userProjectName = process.argv[2]; // User input for the project folder name
const newFolderName = process.argv[3];   // User input for the renamed folder

if (!userProjectName || !newFolderName) {
  console.error('Please provide both a project name and a new folder name.');
  process.exit(1);
}

// Execute the function with the provided inputs
createProject(userProjectName, newFolderName);
