import time
import os
import shutil
import glob
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def create_headless_driver(download_dir: str) -> webdriver.Chrome:
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-webgl") # comment out for screenshots on exceptions
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")

    # Set download preferences
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    return webdriver.Chrome(options=chrome_options)

def create_pi_driver(download_dir):
    options = Options()
    options.binary_location = "/usr/bin/chromium-browser"
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-webgl")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    # Might be located here depending on your OS version
    service = Service("/usr/lib/chromium-browser/chromedriver")
    return webdriver.Chrome(service=service, options=options)

def login_to_fangraphs(driver, username, password):
    """Handle the login process and return success status"""
    print("Navigating to login page...")
    driver.get("https://blogs.fangraphs.com/wp-login.php")
    
    # Wait for the login form to load
    try:
        print("Waiting for login form...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "user_login"))
        )
    except TimeoutException:
        print("Login form not found. Taking screenshot...")
        driver.save_screenshot("login_page_error.png")
        return False
    
    # Fill in login credentials
    print("Entering credentials...")
    driver.find_element(By.ID, "user_login").send_keys(username)
    driver.find_element(By.ID, "user_pass").send_keys(password)
    print("Clicking login button...")
    driver.find_element(By.ID, "wp-submit").click()
    
    # Wait for login to complete
    print("Waiting for login to complete...")
    time.sleep(5)  # Give it some time to process the login
    
    # Check if we're logged in
    print(f"Current URL after login attempt: {driver.current_url}")
    #driver.save_screenshot("after_login.png")
    
    return True

def download_projection_data(driver, download_dir, output_dir, stat_type="bat"):
    """Download specific projection data type using existing driver session"""
    friendly_name = "batter" if stat_type == "bat" else "pitcher"
    
    dest_file = os.path.join(output_dir, f"{friendly_name}.csv")

    # Clean up any preexisting .csv and .crdownload files
    for f in glob.glob(os.path.join(download_dir, "*.csv")):
        os.remove(f)
    for f in glob.glob(os.path.join(download_dir, "*.crdownload")):
        os.remove(f)

    # Navigate to projections page
    projections_url = f"https://www.fangraphs.com/projections?type=rfangraphsdc&stats={stat_type}&pos=all&team=0&players=0"
    print(f"Navigating to {friendly_name} projections page: {projections_url}")
    driver.get(projections_url)

    print("Waiting for projections page to load...")
    time.sleep(10)
    #driver.save_screenshot(os.path.join(download_dir, f"projections_page_{friendly_name}.png"))

    try:
        print("Looking for data grid...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".fg-data-grid"))
        )
        print("Data grid found!")
    except TimeoutException:
        print("Data grid not found. Taking screenshot...")
        driver.save_screenshot(os.path.join(download_dir, f"no_data_grid_{friendly_name}.png"))

    # Click the CSV link
    print("Looking for CSV link...")
    csv_links = driver.find_elements(By.XPATH, "//a[contains(text(), 'CSV') or contains(@href, 'csv')]")
    if csv_links:
        print(f"Found {len(csv_links)} CSV links")
        csv_links[0].click()
        print("Clicked CSV link!")
    else:
        print("No CSV link found.")
        return None

    # Wait for .crdownload file to appear and disappear
    print("Waiting for download to start...")
    for _ in range(10):
        cr_files = glob.glob(os.path.join(download_dir, "*.crdownload"))
        if cr_files:
            print(f"Detected {len(cr_files)} active download(s)...")
            break
        time.sleep(1)

    # Wait for all .crdownload files to finish
    for _ in range(30):
        cr_files = glob.glob(os.path.join(download_dir, "*.crdownload"))
        if not cr_files:
            print("Download finished. Waiting briefly for final write...")
            time.sleep(2)  # extra wait to ensure file is flushed
            break
        time.sleep(1)

    # Find the most recent .csv
    csv_files = glob.glob(os.path.join(download_dir, "*.csv"))
    if not csv_files:
        print("CSV file was not downloaded.")
        return None

    latest_file = max(csv_files, key=os.path.getctime)
    print(f"Downloaded file found: {latest_file}")

    os.makedirs(output_dir, exist_ok=True)
    shutil.move(latest_file, dest_file)
    print(f"File moved and renamed to: {dest_file}")
    return dest_file

def main():
    # Replace with your FanGraphs username and password
    username = os.getenv("FANGRAPHS_USERNAME")
    password = os.getenv("FANGRAPHS_PASSWORD")
    
    # Check if credentials are available
    if not username or not password:
        print("Error: FanGraphs credentials not found in .env file")
        print("Please create a .env file with FANGRAPHS_USERNAME and FANGRAPHS_PASSWORD")
        return
    
    
   
    # Set the directories
    download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    
    # Ensure download directory exists
    os.makedirs(download_dir, exist_ok=True)
    
    # Initialize the driver
    print("Initializing Chrome...")
    driver = create_pi_driver(download_dir)

    try:
        # Log in once
        if not login_to_fangraphs(driver, username, password):
            print("Login failed. Exiting.")
            return
        
        # Download batter projections
        print("\n=== DOWNLOADING BATTER PROJECTIONS ===\n")
        batter_file = download_projection_data(driver, download_dir, output_dir, stat_type="bat")
        if batter_file:
            print(f"Batter projections downloaded to: {batter_file}")
        else:
            print("Batter projections download failed.")
        
        # Download pitcher projections
        print("\n=== DOWNLOADING PITCHER PROJECTIONS ===\n")
        pitcher_file = download_projection_data(driver, download_dir, output_dir, stat_type="pit")
        if pitcher_file:
            print(f"Pitcher projections downloaded to: {pitcher_file}")
        else:
            print("Pitcher projections download failed.")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Close the browser
        print("Closing browser...")
        driver.quit()
        print("Browser closed.")

if __name__ == "__main__":
    main()