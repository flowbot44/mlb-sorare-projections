import subprocess
import time
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Step 1: Run fangraph_fetcher to download CSVs
print("Running fangraph_fetcher.py...")
subprocess.run(["python3", os.path.join(script_dir, "fangraph_fetcher.py")], check=True)

# Optional delay if needed (not strictly required if fangraph_fetcher handles .crdownload wait)
time.sleep(5)

# Step 2: Run park_factor_fetcher to download ballpark data
print("Running park_factor_fetcher.py...")
subprocess.run(["python3", os.path.join(script_dir, "park_factor_fetcher.py")], check=True)

# Optional delay
time.sleep(2)

# Step 3: Run depth_projection to process CSVs into SQLite DB
print("Running depth_projection.py...")
subprocess.run(["python3", os.path.join(script_dir, "depth_projection.py")], check=True)

# Step 4: Run update_stadiums to ensure stadium data is current
print("Running update_stadiums.py...")
subprocess.run(["python3", os.path.join(script_dir, "update_stadiums.py")], check=True)

print("All scripts completed successfully.")