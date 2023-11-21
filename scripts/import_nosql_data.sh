#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Change to the parent directory of the script
cd "$SCRIPT_DIR"/..

python3 import_data.py dataset/airline_delay.json