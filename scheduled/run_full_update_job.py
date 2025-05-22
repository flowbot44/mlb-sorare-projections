import sys
import os
import logging

# Ensure app folder is in the path so imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from flask_app import run_full_update

# Configure logging to stdout for container logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("schedule_job")

if __name__ == "__main__":
    logger.info("🚀 Starting scheduled full update job...")
    success = run_full_update()
    if success:
        logger.info("✅ Full update completed successfully.")
        sys.exit(0)
    else:
        logger.error("❌ Full update failed.")
        sys.exit(1)
