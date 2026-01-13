"""
Yahoo Finance News Scraper - Simple function-based approach
Scrapes news articles from Yahoo Finance with infinite scroll until reaching target days ago.
"""

import time
import re
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


def create_driver(headless: bool = True) -> webdriver.Chrome:
    """Create and return Chrome driver with optimal settings."""
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
        return driver
    except Exception as e:
        print(f"Failed to create driver: {e}")
        raise


def check_target_days(html_content: str, target_days: int) -> bool:
    """Check if target days ago pattern is found in content."""
    if target_days == 0:
        target_days = 1  # Treat 0 as 1 day ago

    patterns = [
        rf'{target_days}d ago',
        rf'{target_days} days ago',
        rf'{target_days}\s*d\s*ago',
        rf'{target_days}\s*days?\s*ago'
    ]
    
    return any(re.search(pattern, html_content, re.IGNORECASE) for pattern in patterns)


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


def extract_news_item(item, symbol) -> Optional[Dict]:
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
        
        # Tickers
        data['tickers'] = symbol
        
        return data if data.get('title') else None
    except:
        return None


def extract_all_news(soup: BeautifulSoup, symbol: str) -> List[Dict]:
    """Extract all news items from page, excluding ads."""
    container = soup.find('ul', class_='stream-items yf-9xydx9')
    if not container:
        return []
    
    items = container.find_all('li', class_='stream-item')
    news_items = []
    
    for item in items:
        # Skip ads
        if any(cls in item.get('class', []) for cls in ['ad-item', 'native-ad']):
            continue
        
        # Check if item contains the symbol in raw HTML
        item_html = str(item)
        if symbol not in item_html:
            continue
        
        news_item = extract_news_item(item, symbol)
        if news_item:
            news_items.append(news_item)
    
    return news_items


def scrape_yahoo_finance_news(symbol: str = "AAPL", target_days: int = 1, max_scrolls: int = 50, headless: bool = True) -> List[Dict]:
    """
    Scrape Yahoo Finance news until reaching target days ago content.
    
    Args:
        symbol: Stock symbol (default: AAPL)
        target_days: Number of days to scroll back to (default: 5)
        max_scrolls: Maximum scrolls to prevent infinite loops (default: 50)
        headless: Run browser in background (default: True)
    
    Returns:
        List of news item dictionaries
    """
    url = f"https://ca.finance.yahoo.com/quote/{symbol}/news/"
    driver = None
    
    try:
        print(f"Loading {url}...")
        driver = create_driver(headless)
        
        # Load page with retry logic
        for attempt in range(3):
            try:
                driver.get(url)
                print("Page loaded, waiting for content...")
                WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.CLASS_NAME, "stream-items")))
                print("Found news container!")
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt == 2:
                    raise
                time.sleep(5)
        
        for scroll in range(max_scrolls):
            if check_target_days(driver.page_source, target_days):
                print(f"Found {target_days}d ago content after {scroll} scrolls")
                break
            
            print(f"Scrolling... ({scroll + 1}/{max_scrolls})")
            scroll_and_wait(driver)
            time.sleep(1)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        news_items = extract_all_news(soup, symbol)
        print(f"Scraped {len(news_items)} news items")
        return news_items
        
    except Exception as e:
        print(f"Error during scraping: {e}")
        return []
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


def save_news_to_file(news_items: List[Dict], filename: str = "news_data.json") -> None:
    """Save news items to JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(news_items, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(news_items)} items to {filename}")


def scrape_and_save(symbol: str = "AAPL", target_days: int = 5, filename: str = "news_data.json", **kwargs) -> List[Dict]:
    """Scrape news and save to file in one call."""
    news_items = scrape_yahoo_finance_news(symbol, target_days, **kwargs)
    save_news_to_file(news_items, filename)
    return news_items


if __name__ == "__main__":
    # Example usage
    print("Scraping AAPL news until 1 day ago...")
    news = scrape_yahoo_finance_news("AAPL", target_days=1, max_scrolls=20)
    
    print(f"\nFound {len(news)} news items")
    for i, item in enumerate(news[:3], 1):
        print(f"{i}. {item.get('headline', 'No headline')}")
        print(f"   Time: {item.get('timestamp', 'No time')}")
        print(f"   Tickers: {', '.join(item.get('tickers', []))}")
    
    # Save to file
    save_news_to_file(news, "aapl_news.json")
