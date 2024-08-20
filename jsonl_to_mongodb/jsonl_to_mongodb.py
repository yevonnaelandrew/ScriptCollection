import json
import time
import os
from pymongo import MongoClient
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class JSONLHandler(FileSystemEventHandler):
    def __init__(self, filename):
        self.filename = filename
        self.db = MongoClient('mongodb://localhost:27017/')['database']
        self.collection = self.db['collection']
        self.position_collection = self.db['file_positions']
        self.last_position, self.last_inode = self.load_position()

    def load_position(self):
        record = self.position_collection.find_one({'filename': self.filename})
        if record:
            return record['position'], record['inode']
        return 0, None

    def save_position(self, position, inode):
        self.position_collection.update_one(
            {'filename': self.filename},
            {'$set': {'position': position, 'inode': inode}},
            upsert=True
        )

    def process_file(self):
        try:
            with open(self.filename, 'r') as file:
                current_inode = os.fstat(file.fileno()).st_ino
                
                if self.last_inode != current_inode:
                    print(f"File has been rotated. Processing from the beginning.")
                    self.last_position = 0
                
                file.seek(0, 2)  # Go to the end of the file
                file_size = file.tell()
                
                if file_size < self.last_position:
                    print("File has been truncated. Processing from the beginning.")
                    self.last_position = 0
                
                file.seek(self.last_position)
                
                while True:
                    line = file.readline()
                    if not line:
                        break  # We've reached the end of the file
                    
                    try:
                        data = json.loads(line.strip())
                        self.collection.insert_one(data)
                        print(f"Inserted: {data}")
                    except json.JSONDecodeError:
                        print(f"Invalid JSON: {line}")
                    
                    self.last_position = file.tell()
                
                self.save_position(self.last_position, current_inode)
        except IOError as e:
            print(f"Error processing file: {e}")

    def on_modified(self, event):
        if event.src_path == self.filename:
            self.process_file()

    def periodic_full_scan(self):
        print("Performing periodic full file scan...")
        self.last_position = 0
        self.process_file()

if __name__ == "__main__":
    jsonl_file = 'file.jsonl'
    event_handler = JSONLHandler(jsonl_file)
    observer = Observer()
    observer.schedule(event_handler, path='.', recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(60)  # Wait for 1 minute
            event_handler.periodic_full_scan()
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
