"""
Yahoo Finance News Scraper - Simple function-based approach
Scrapes news articles from Yahoo Finance with infinite scroll until reaching target days ago.
"""

import time
import re
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from scraper import get_article_text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_driver(headless: bool = True) -> webdriver.Chrome:
    """Create and return Chrome driver with optimal settings."""
    logger.info(f"Creating Chrome driver (headless={headless})")
    options = Options()
    if headless:
        options.add_argument("--headless")
    
    # Performance and stability options
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-images")
    options.add_argument("--disable-javascript")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)  # Increased timeout
        driver.implicitly_wait(10)
        logger.info("Chrome driver created successfully")
        return driver
    except Exception as e:
        logger.error(f"Failed to create driver: {e}")
        raise


def check_target_days(html_content: str, target_days: int) -> bool:
    """Check if target days ago pattern is found in content."""
    if target_days == 0:
        # For today's news, scroll until we see 1d ago content
        patterns = [
            r'1d ago',
            r'1 day ago',
            r'1\s*d\s*ago',
            r'1\s*day\s*ago'
        ]
        found = any(re.search(pattern, html_content, re.IGNORECASE) for pattern in patterns)
        if found:
            logger.debug("Found 1d ago content for target_days=0")
        return found
    
    # For specific days ago
    patterns = [
        rf'{target_days}d ago',
        rf'{target_days} days ago',
        rf'{target_days}\s*d\s*ago',
        rf'{target_days}\s*days?\s*ago'
    ]
    
    found = any(re.search(pattern, html_content, re.IGNORECASE) for pattern in patterns)
    if found:
        logger.debug(f"Found {target_days}d ago content")
    return found


def parse_relative_time_to_date(time_text: str) -> str:
    """
    Convert relative time string to yyyy-mm-dd date format.
    
    Args:
        time_text: Relative time like "3d ago", "2 hrs ago", "1 day ago"
        
    Returns:
        Date string in yyyy-mm-dd format
    """
    try:
        current_date = datetime.now()
        time_text = time_text.lower().strip()
        
        # Patterns for different time formats
        day_patterns = [
            r'(\d+)d ago',
            r'(\d+) days? ago',
            r'(\d+) day ago'
        ]
        
        hour_patterns = [
            r'(\d+) hrs? ago',
            r'(\d+) hours? ago',
            r'(\d+) hour ago'
        ]
        
        minute_patterns = [
            r'(\d+) mins? ago',
            r'(\d+) minutes? ago',
            r'(\d+) minute ago'
        ]
        
        # Check for days
        for pattern in day_patterns:
            match = re.search(pattern, time_text)
            if match:
                days = int(match.group(1))
                target_date = current_date - timedelta(days=days)
                return target_date.strftime('%Y-%m-%d')
        
        # Check for hours
        for pattern in hour_patterns:
            match = re.search(pattern, time_text)
            if match:
                hours = int(match.group(1))
                target_date = current_date - timedelta(hours=hours)
                return target_date.strftime('%Y-%m-%d')
        
        # Check for minutes  
        for pattern in minute_patterns:
            match = re.search(pattern, time_text)
            if match:
                minutes = int(match.group(1))
                target_date = current_date - timedelta(minutes=minutes)
                return target_date.strftime('%Y-%m-%d')
        
        # If no pattern matches, return today's date
        return current_date.strftime('%Y-%m-%d')
        
    except Exception as e:
        print(f"Error parsing time '{time_text}': {e}")
        return datetime.now().strftime('%Y-%m-%d')

def scroll_and_wait(driver: webdriver.Chrome) -> None:
    """Scroll to bottom and wait for content to load."""
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)


def extract_news_item(item, symbol, target_days: int = None, exact_day_only: bool = False) -> Optional[Dict]:
    """Extract data from a single news item."""
    try:
        data = {}
        
        # Headline
        headline = item.find('h3', class_=re.compile(r'clamp.*yf-'))
        if headline:
            data['title'] = headline.get_text(strip=True)
        
        # URL
        link = item.find('a', {'data-ylk': re.compile(r'.*hdln.*')})
        if link:
            url = link.get('href')
            data['url'] = url if url.startswith('http') else f'https://ca.finance.yahoo.com{url}'
        
        # Timestamp
        time_elem = item.find('div', class_=re.compile(r'publishing.*yf-'))
        if time_elem:
            time_text = time_elem.get_text(strip=True)
            timestamp_text = time_text.split('•')[-1].strip() if '•' in time_text else time_text
            data['timestamp'] = timestamp_text
            # Convert to actual date
            data['date'] = parse_relative_time_to_date(timestamp_text)
            
            if target_days is not None:
                article_date = datetime.strptime(data['date'], '%Y-%m-%d').date()
                current_date = datetime.now().date()
                delta_days = (current_date - article_date).days
                
                if exact_day_only and delta_days != target_days:
                    return None
                elif not exact_day_only and delta_days >= target_days:
                    return None
        
        # Body
        # TODO: Uncomment to fetch full article text
        # if 'url' in data:
        #     article_text = get_article_text(data['url'])
        #     data['body'] = article_text

        # Tickers
        data['tickers'] = symbol
        
        return data if data.get('title') else None
    except:
        return None


def extract_all_news(soup: BeautifulSoup, symbol: str, target_days: int = None, exact_day_only: bool = False) -> List[Dict]:
    """Extract all news items from page, excluding ads."""
    container = soup.find('ul', class_='stream-items yf-9xydx9')
    if not container:
        return []
    
    items = container.find_all('li', class_='stream-item')
    news_items = []
    
    for item in tqdm(items, desc="Processing news items", leave=False):
        # Skip ads
        if any(cls in item.get('class', []) for cls in ['ad-item', 'native-ad']):
            continue
        
        # Check if item contains the symbol in raw HTML
        item_html = str(item)
        if symbol not in item_html:
            continue
        
        news_item = extract_news_item(item, symbol, target_days, exact_day_only)
        if news_item:
            news_items.append(news_item)

        # Sleep responsibly between requests
        time.sleep(0.5)
    
    return news_items


def scrape_yahoo_finance_news(symbol: str = "AAPL", target_days: int = 1, max_scrolls: int = 50, headless: bool = True, exact_day_only: bool = False) -> List[Dict]:
    """
    Scrape Yahoo Finance news until reaching target days ago content.
    
    Args:
        symbol: Stock symbol (default: AAPL)
        target_days: Number of days to scroll back to (default: 1)
        max_scrolls: Maximum scrolls to prevent infinite loops (default: 50)
        headless: Run browser in background (default: True)
        exact_day_only: If True, return only news from exactly target_days ago (default: False)
    
    Returns:
        List of news item dictionaries
    """
    url = f"https://ca.finance.yahoo.com/quote/{symbol}/news/"
    driver = None
    
    try:
        logger.info(f"Starting scrape for {symbol} (target_days={target_days})")
        logger.info(f"Loading {url}...")
        driver = create_driver(headless)
        
        # Load page with retry logic
        for attempt in tqdm(range(3), desc="Loading page", leave=False):
            try:
                driver.get(url)
                logger.info("Page loaded, waiting for content...")
                WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.CLASS_NAME, "stream-items")))
                logger.info("Found news container!")
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt == 2:
                    logger.error("All retry attempts failed")
                    raise
                time.sleep(5)
        
        target_found = False
        for scroll in tqdm(range(max_scrolls), desc=f"Scrolling for {target_days}d ago content"):
            if check_target_days(driver.page_source, 0 if target_days==0 else target_days+1):
                logger.info(f"Found {target_days if target_days > 0 else '1'}d ago content after {scroll} scrolls")
                target_found = True
                break
            
            scroll_and_wait(driver)
            time.sleep(1)

        if not target_found:
            logger.warning(f"Target {target_days}d ago content not found after {max_scrolls} scrolls")
        
        logger.info("Parsing page content with BeautifulSoup")
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        news_items = extract_all_news(soup, symbol, target_days, exact_day_only)
        logger.info(f"Successfully scraped {len(news_items)} news items for {symbol}")
        return news_items
        
    except Exception as e:
        logger.error(f"Error during scraping {symbol}: {e}")
        return []
    finally:
        if driver:
            try:
                driver.quit()
                logger.debug("Chrome driver closed successfully")
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")


def save_news_to_file(news_items: List[Dict], filename: str = "news_data.json") -> None:
    """Save news items to JSON file."""
    logger.info(f"Saving {len(news_items)} items to {filename}")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(news_items, f, indent=2, ensure_ascii=False)
    logger.info(f"Successfully saved {len(news_items)} items to {filename}")

def scrape_multiple_yahoo_tickers(tickers: List[str], target_days: int = 0, ) -> Dict[str, List[Dict]]:
    """Scrape Yahoo Finance news for multiple tickers."""
    logger.info(f"Starting bulk scrape for {len(tickers)} tickers")
    all_news = {}
    
    for ticker in tqdm(tickers, desc="Scraping tickers"):
        try:
            news = scrape_yahoo_finance_news(ticker, target_days, headless=True)
            all_news[ticker] = news
            logger.info(f"Scraped {len(news)} news items for {ticker}")
        except Exception as e:
            logger.error(f"Error scraping {ticker}: {e}")
            all_news[ticker] = []  # Add empty list for failed scrapes
    
    total_items = sum(len(items) for items in all_news.values())
    logger.info(f"Bulk scrape completed: {total_items} total items from {len(tickers)} tickers")
    return all_news

if __name__ == "__main__":
    # Example usage
    news_data = scrape_yahoo_finance_news("AMZN", 0, exact_day_only=True)
    save_news_to_file(news_data, f"yahoo_finance_news+{datetime.now().strftime('%Y%m%d')}.json")
    # print(parse_relative_time_to_date("22 hrs ago"))