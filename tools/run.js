const { spawn } = require('child_process');
const path = require('path');

function runCommand(command, args = [], options = {}) {
    return new Promise((resolve, reject) => {
        const process = spawn(command, args, { ...options, stdio: 'inherit' });

        process.on('close', (code) => {
            if (code !== 0) {
                reject(`Command failed with exit code ${code}`);
                return;
            }
            resolve();
        });

        process.on('error', (err) => {
            reject(`Failed to start process: ${err.message}`);
        });
    });
}

async function runScripts() {
    try {
        // Change to the 'mta/mtastats' directory relative to the scripts folder
        console.log('Navigating to app...');
        process.chdir(path.join(__dirname, '..', 'app'));

        // Run 'npm install' to install dependencies
        console.log('Running bun install...');
        await runCommand('bun', ['install']);

        // Run 'npm run sources'
        console.log('Running bun run sources...');
        await runCommand('bun', ['run', 'sources']);

        // Run 'npm run dev'
        console.log('Running bun run dev...');
        await runCommand('bun', ['run', 'dev']);

        console.log('All commands executed successfully.');
    } catch (error) {
        console.error(`An error occurred: ${error}`);
    }
}

runScripts();
