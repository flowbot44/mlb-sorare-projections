import os
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from utils import DATA_DIR
import pandas as pd

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# URLs
LOGIN_URL = "https://blogs.fangraphs.com/wp-login.php"

# Replace with the CSV links you want
CSV_LINKS = {
    "batter": "https://www.fangraphs.com/api/projections?type=rfangraphsdc&stats=bat&pos=all&team=0&players=0&lg=all&download=1",
    "pitcher": "https://www.fangraphs.com/api/projections?type=rfangraphsdc&stats=pit&pos=all&team=0&players=0&lg=all&download=1",
    "batter_vs_rhp": "https://www.fangraphs.com/api/projections?type=rsteamer_vr_0&stats=bat&pos=all&team=0&players=0&lg=all&download=1",
    "batter_vs_lhp": "https://www.fangraphs.com/api/projections?type=rsteamer_vl_0&stats=bat&pos=all&team=0&players=0&lg=all&download=1"
}

def login_to_fangraphs(session: requests.Session, username: str, password: str) -> bool:
    logger.info("Attempting to log in to FanGraphs...")

    # Simulate visiting the login page to set initial cookies
    session.get("https://blogs.fangraphs.com/wp-login.php")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://blogs.fangraphs.com/wp-login.php"
    }

    login_data = {
        "log": username,
        "pwd": password,
        "rememberme": "forever",
        "wp-submit": "Log In",
        "redirect_to": "https://blogs.fangraphs.com/wp-admin/",
        "testcookie": "1"
    }

    response = session.post(LOGIN_URL, data=login_data, headers=headers)

    # Check for presence of login cookies
    cookies = session.cookies.get_dict()
    logged_in = any("wordpress_logged_in" in k for k in cookies)

    if logged_in:
        logger.info("Login successful.")
        return True
    else:
        logger.error("Login failed. FanGraphs may have changed their form or blocked bots.")
        with open("login_response.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        logger.error("Saved response HTML to login_response.html for inspection.")
        return False


def download_csv(session: requests.Session, url: str, filename: str):
    try:
        logger.info(f"Downloading: {filename} from {url}")
        response = session.get(url)
        response.raise_for_status()

        os.makedirs(DATA_DIR, exist_ok=True)
        filepath = os.path.join(DATA_DIR, filename)

        content_type = response.headers.get("Content-Type", "")

        # If response is JSON, convert to CSV
        if "application/json" in content_type:
            logger.info("Detected JSON response, converting to CSV.")
            data = response.json()
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False)
        else:
            logger.info("Saving raw response as CSV.")
            with open(filepath, "wb") as f:
                f.write(response.content)
    except Exception as e:
        logger.error(f"Failed to download {filename}: {str(e)}")

def main():
    start_time = datetime.now()
    logger.info(f"Script started at {start_time}")

    username = os.getenv("FANGRAPHS_USERNAME")
    password = os.getenv("FANGRAPHS_PASSWORD")

    if not username or not password:
        logger.error("Missing FanGraphs credentials in environment variables.")
        return 1

    with requests.Session() as session:
        if not login_to_fangraphs(session, username, password):
            return 1

        for key, url in CSV_LINKS.items():
            filename = f"{key}.csv"
            download_csv(session, url, filename)

    end_time = datetime.now()
    logger.info(f"Script completed at {end_time} (Duration: {end_time - start_time})")
    return 0

if __name__ == "__main__":
    exit(main())
