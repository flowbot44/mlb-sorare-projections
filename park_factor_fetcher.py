import os
import pandas as pd
import requests

from dotenv import load_dotenv
import logging
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("park_factor_fetcher")

# Load environment variables from .env file or environment
load_dotenv()

def download_from_google_sheets(sheet_url, output_dir="data", output_filename="park_data.csv"):
    """
    Download data from a Google Sheets document as CSV
    
    Args:
        sheet_url (str): URL of the Google Sheet
        output_dir (str): Directory to save the CSV file
        output_filename (str): Filename to save as
        
    Returns:
        str: Path to the saved CSV file or None if download failed
    """
    logger.info(f"Downloading data from Google Sheets: {sheet_url}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Check if it's a valid Google Sheets URL
    if "docs.google.com/spreadsheets" not in sheet_url:
        logger.error("Not a valid Google Sheets URL")
        return None
    
    try:
        # Extract the document ID from the URL
        if "/d/" in sheet_url and "/edit" in sheet_url:
            doc_id = sheet_url.split("/d/")[1].split("/edit")[0]
        else:
            # Try to find the ID another way
            doc_id = sheet_url.split("spreadsheets/d/")[1].split("/")[0]
        
        # Form the export URL for public docs (using the public web publishing approach)
        export_url = f"https://docs.google.com/spreadsheets/d/{doc_id}/gviz/tq?tqx=out:csv"
        
        # Create session with a user agent
        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
        }
        
        # Download the CSV
        response = session.get(export_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Save the CSV
        output_file = os.path.join(output_dir, output_filename)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        logger.info(f"Data saved to {output_file}")
        return output_file
        
    except Exception as e:
        logger.error(f"Error downloading from Google Sheets: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def create_fallback_data(output_dir):
    """Create a fallback park_data.csv with minimal data if everything else fails"""
    logger.info("Creating fallback minimal park data")
    # Sample data based on your provided park_data.csv
    data = {
        "Rk.": list(range(1, 31)),
        "Team": ["Rockies", "Red Sox", "Reds", "Royals", "Twins", "D-backs", "Marlins", "Pirates", "Rangers", "Nationals",
                 "Phillies", "Braves", "Angels", "Dodgers", "Blue Jays", "Astros", "Cardinals", "Yankees", "Orioles", "White Sox",
                 "Tigers", "Athletics", "Giants", "Cubs", "Brewers", "Mets", "Guardians", "Padres", "Rays", "Mariners"],
        "Venue": ["Coors Field", "Fenway Park", "Great American Ball Park", "Kauffman Stadium", "Target Field", "Chase Field",
                  "loanDepot park", "PNC Park", "Globe Life Field", "Nationals Park", "Citizens Bank Park", "Truist Park",
                  "Angel Stadium", "Dodger Stadium", "Rogers Centre", "Minute Maid Park", "Busch Stadium", "Yankee Stadium",
                  "Oriole Park at Camden Yards", "Guaranteed Rate Field", "Comerica Park", "Oakland Coliseum", "Oracle Park",
                  "Wrigley Field", "American Family Field", "Citi Field", "Progressive Field", "Petco Park", "Tropicana Field", "T-Mobile Park"],
        "Year": ["2022-2024"] * 30,
        "Park Factor": [112, 107, 105, 104, 102, 101, 101, 101, 101, 101, 101, 100, 100, 100, 100, 100, 100, 100, 99, 99, 98, 97, 97, 97, 97, 97, 97, 96, 96, 92]
    }
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Save as CSV
    output_file = os.path.join(output_dir, "park_data.csv")
    df.to_csv(output_file, index=False)
    logger.info(f"Fallback park data saved to {output_file}")
    return output_file

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data")
    
    # Google Sheets URL
    google_sheets_url = os.getenv("BALL_PARK_GOOGLE_DOC")
    
    # Try to download data from Google Sheets
    result = download_from_google_sheets(google_sheets_url, data_dir)
    
    # If download fails, create fallback data
    if not result:
        logger.warning("Failed to download from Google Sheets, creating fallback data...")
        create_fallback_data(data_dir)