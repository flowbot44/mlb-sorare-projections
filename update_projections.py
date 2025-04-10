import subprocess
import time
import os

# Step 1: Run fangraph_fetcher to download CSVs
print("Running fangraph_fetcher.py...")
subprocess.run(["python3", os.path.join(os.path.dirname(os.path.abspath(__file__)),"fangraph_fetcher.py")], check=True)

# Optional delay if needed (not strictly required if fangraph_fetcher handles .crdownload wait)
time.sleep(5)

# Step 2: Run depth_projection to process CSVs into SQLite DB
print("Running depth_projection.py...")
subprocess.run(["python3", os.path.join(os.path.dirname(os.path.abspath(__file__)),"depth_projection.py")], check=True)

print("All scripts completed successfully.")
