import os
from datetime import datetime
import logging

log_directory = '/Users/arpansahu/projects/profile/talent_bridge_cron/scraper/logs'
os.makedirs(log_directory, exist_ok=True)

current_date = datetime.now().strftime('%Y%m%d')
log_file_name = os.path.join(log_directory, f"test_log_{current_date}.log")

logging.basicConfig(
    filename=log_file_name,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("test_logger")
logger.info("This is a test log entry.")