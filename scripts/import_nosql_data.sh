#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Change to the parent directory of the script
cd "$SCRIPT_DIR"/..

python3 -m app.services.database.nosql.data_importer.data_importer dataset/ign.csv