# JSONL File Monitor for MongoDB

This Python script monitors a JSONL (JSON Lines) file for changes and inserts new entries into a MongoDB database in real-time.

## Features

- Real-time monitoring of JSONL file changes
- Handles file rotation and truncation
- Resumes from last known position after restarts
- Periodic full file scans to ensure data integrity
- Robust error handling for invalid JSON entries

## Requirements

- Python 3.6+
- pymongo
- watchdog

## Installation

1. Clone this repository
2. Install dependencies: `pip install pymongo watchdog`

## Usage

1. Update the MongoDB connection details in the script
2. Set the `jsonl_file` variable to your JSONL file path
3. Run the script

## Configuration

Adjust these variables in the script:
- MongoDB connection string
- Database and collection names
- JSONL file path
- Periodic scan interval

## Limitations

- May miss updates during very rapid file changes
- Assumes JSONL format for all entries
