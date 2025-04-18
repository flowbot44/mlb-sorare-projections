import time
import os
import shutil
import glob
import logging
import tempfile
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fangraphs_fetcher.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file or environment
load_dotenv()

def create_headless_driver(download_dir: str) -> webdriver.Chrome:
    """Create a headless Chrome driver using pre-installed Chromium"""
    logger.info("Setting up headless Chromium driver")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Use the pre-installed Chromium from the Docker image
    chromium_path = os.environ.get('CHROME_BIN', '/usr/bin/chromium')
    chromedriver_path = os.environ.get('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')
    
    logger.info(f"Using Chromium at: {chromium_path}")
    logger.info(f"Using ChromeDriver at: {chromedriver_path}")
    
    chrome_options.binary_location = chromium_path
    
    # Set download preferences
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    try:
        # Use the pre-installed ChromeDriver
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    except Exception as e:
        logger.error(f"Failed to create Chrome driver: {str(e)}")
        raise

def login_to_fangraphs(driver, username, password, max_retries=3):
    """Handle the login process with retries and return success status"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Login attempt {attempt+1}/{max_retries}")
            driver.get("https://blogs.fangraphs.com/wp-login.php")
            
            # Wait for the login form to load
            logger.info("Waiting for login form...")
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "user_login"))
            )
            
            # Fill in login credentials
            logger.info("Entering credentials...")
            driver.find_element(By.ID, "user_login").send_keys(username)
            driver.find_element(By.ID, "user_pass").send_keys(password)
            logger.info("Clicking login button...")
            driver.find_element(By.ID, "wp-submit").click()
            
            # Wait for login to complete
            logger.info("Waiting for login to complete...")
            time.sleep(5)  # Give it some time to process the login
            
            # Check if we're logged in - look for a typical element present after login
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "avatar"))
                )
                logger.info("Login successful")
                return True
            except TimeoutException:
                if "wp-login.php" in driver.current_url:
                    logger.warning("Still on login page, login may have failed")
                    if attempt == max_retries - 1:
                        screenshot_path = f"login_failure_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                        driver.save_screenshot(screenshot_path)
                        logger.error(f"Login failed after {max_retries} attempts. Screenshot saved: {screenshot_path}")
                        return False
                else:
                    logger.info(f"Current URL after login: {driver.current_url}")
                    return True
                    
        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            if attempt == max_retries - 1:
                return False
            time.sleep(5)  # Wait before retrying
    
    return False

def download_projection_data(driver, download_dir, output_dir, stat_type="bat", max_retries=3):
    """Download specific projection data type with retries"""
    friendly_name = "batter" if stat_type == "bat" else "pitcher"
    dest_file = os.path.join(output_dir, f"{friendly_name}.csv")
    
    # Clean up any preexisting .csv and .crdownload files
    for f in glob.glob(os.path.join(download_dir, "*.csv")):
        os.remove(f)
    for f in glob.glob(os.path.join(download_dir, "*.crdownload")):
        os.remove(f)
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Download attempt {attempt+1}/{max_retries} for {friendly_name} projections")
            
            # Navigate to projections page
            projections_url = f"https://www.fangraphs.com/projections?type=rfangraphsdc&stats={stat_type}&pos=all&team=0&players=0"
            logger.info(f"Navigating to {friendly_name} projections page: {projections_url}")
            driver.get(projections_url)
            
            logger.info("Waiting for projections page to load...")
            time.sleep(10)
            
            try:
                logger.info("Looking for data grid...")
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".fg-data-grid"))
                )
                logger.info("Data grid found!")
            except TimeoutException:
                screenshot_path = f"no_data_grid_{friendly_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                driver.save_screenshot(screenshot_path)
                logger.error(f"Data grid not found. Screenshot saved: {screenshot_path}")
                if attempt == max_retries - 1:
                    return None
                continue
            
            # Click the CSV link
            logger.info("Looking for CSV link...")
            csv_links = driver.find_elements(By.XPATH, "//a[contains(text(), 'CSV') or contains(@href, 'csv')]")
            if csv_links:
                logger.info(f"Found {len(csv_links)} CSV links")
                csv_links[0].click()
                logger.info("Clicked CSV link!")
            else:
                logger.error("No CSV link found.")
                if attempt == max_retries - 1:
                    return None
                continue
            
            # Wait for download to complete
            logger.info("Waiting for download to start...")
            download_started = False
            for _ in range(20):  # Wait up to 20 seconds for download to start
                cr_files = glob.glob(os.path.join(download_dir, "*.crdownload"))
                if cr_files:
                    logger.info(f"Detected {len(cr_files)} active download(s)...")
                    download_started = True
                    break
                time.sleep(1)
            
            if not download_started:
                logger.warning("No .crdownload file detected. Download may not have started.")
            
            # Wait for all .crdownload files to finish
            for _ in range(60):  # Wait up to 60 seconds for download to complete
                cr_files = glob.glob(os.path.join(download_dir, "*.crdownload"))
                if not cr_files:
                    logger.info("Download finished. Waiting briefly for final write...")
                    time.sleep(3)  # extra wait to ensure file is flushed
                    break
                time.sleep(1)
            
            # Find the most recent .csv
            csv_files = glob.glob(os.path.join(download_dir, "*.csv"))
            if not csv_files:
                logger.error("CSV file was not downloaded.")
                if attempt == max_retries - 1:
                    return None
                continue
            
            latest_file = max(csv_files, key=os.path.getctime)
            logger.info(f"Downloaded file found: {latest_file}")
            
            # Make sure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Move file to destination
            shutil.move(latest_file, dest_file)
            logger.info(f"File moved and renamed to: {dest_file}")
            return dest_file
            
        except Exception as e:
            logger.error(f"Error downloading {friendly_name} projections: {str(e)}")
            if attempt == max_retries - 1:
                return None
            time.sleep(5)  # Wait before retrying
    
    return None

def main():
    start_time = datetime.now()
    logger.info(f"Script started at {start_time}")
    
    # Get FanGraphs credentials
    username = os.getenv("FANGRAPHS_USERNAME")
    password = os.getenv("FANGRAPHS_PASSWORD")
    
    # Check if credentials are available
    if not username or not password:
        logger.error("Error: FanGraphs credentials not found in environment variables")
        logger.error("Please set FANGRAPHS_USERNAME and FANGRAPHS_PASSWORD")
        return 1
    
    # Set the directories - use script directory for output and temp dir for downloads
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "data")
    
    # Create a temporary download directory that will be cleaned up automatically
    with tempfile.TemporaryDirectory() as download_dir:
        logger.info(f"Using temporary download directory: {download_dir}")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize the driver
        driver = None
        try:
            logger.info("Initializing Chrome...")
            driver = create_headless_driver(download_dir)
            
            # Log in once
            if not login_to_fangraphs(driver, username, password):
                logger.error("Login failed. Exiting.")
                return 1
            
            # Download batter projections
            logger.info("\n=== DOWNLOADING BATTER PROJECTIONS ===\n")
            batter_file = download_projection_data(driver, download_dir, output_dir, stat_type="bat")
            if batter_file:
                logger.info(f"Batter projections downloaded to: {batter_file}")
            else:
                logger.error("Batter projections download failed.")
            
            # Download pitcher projections
            logger.info("\n=== DOWNLOADING PITCHER PROJECTIONS ===\n")
            pitcher_file = download_projection_data(driver, download_dir, output_dir, stat_type="pit")
            if pitcher_file:
                logger.info(f"Pitcher projections downloaded to: {pitcher_file}")
            else:
                logger.error("Pitcher projections download failed.")
            
        except Exception as e:
            logger.error(f"An unexpected error occurred: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return 1
        
        finally:
            # Close the browser
            if driver:
                logger.info("Closing browser...")
                try:
                    driver.quit()
                    logger.info("Browser closed.")
                except Exception as e:
                    logger.error(f"Error closing browser: {str(e)}")
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Script completed at {end_time} (Duration: {duration})")
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)