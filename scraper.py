import requests
from bs4 import BeautifulSoup
import time
import logging
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from newspaper import Article

from mongodb import create_document, create_embedding

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DELAY = 2
BASE_URL = "https://www.marketwatch.com"

USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]


def create_session():
    session = requests.Session()
    
    retry = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1)
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
    })
    
    return session


def fetch_page(session, url, max_retries=3):
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                time.sleep(DELAY * attempt)
            
            response = session.get(url, timeout=30)
            
            if response.status_code == 200:
                return response.content
            
            if response.status_code in [401, 403, 429]:
                logger.warning(f"HTTP {response.status_code} on attempt {attempt + 1}")
                continue
            
            response.raise_for_status()
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed (attempt {attempt + 1}): {str(e)[:50]}")
            if attempt == max_retries - 1:
                return None
    
    return None


def extract_article_content(url):
    try:
        article = Article(url)
        article.download()
        article.parse()
        
        if article.text and len(article.text.strip()) > 50:
            return article.text
        
        return None
        
    except Exception as e:
        logger.debug(f"Content extraction failed for {url}: {str(e)[:50]}")
        return None


def parse_article_element(element, ticker):
    # Find headline and URL
    headline = None
    url = None
    
    selectors = ['h3.article__headline a', 'h3 a', 'h2 a', 'h4 a']
    
    for selector in selectors:
        elem = element.select_one(selector)
        if elem:
            headline = elem.get_text(strip=True)
            url = elem.get('href')
            if headline and len(headline) > 15:
                break
    
    if not headline or len(headline.strip()) < 10:
        return None
    
    # Build article data
    article = {
        'ticker': ticker.upper(),
        'title': headline.strip(),
    }
    
    # Fix URL
    if url:
        if not url.startswith('http'):
            article['url'] = f"{BASE_URL}{url if url.startswith('/') else '/' + url}"
        else:
            article['url'] = url
    
    # Extract author
    author = element.select_one('span.article__author')
    if author:
        article['author'] = author.get_text(strip=True).replace('by ', '').replace('By ', '')
    
    # Extract timestamp
    timestamp = element.get('data-timestamp')
    if timestamp:
        article['data_timestamp'] = timestamp
    
    return article


def scrape_page(session, url, ticker):
    content = fetch_page(session, url)
    if not content:
        return []
    
    soup = BeautifulSoup(content, 'html.parser')
    container = soup.find('div', class_='collection__elements j-scrollElement')
    
    if not container:
        return []
    
    articles = []
    elements = container.find_all('div', class_=lambda x: x and 'element--article' in x)
    
    for element in elements:
        article = parse_article_element(element, ticker)
        if not article or 'url' not in article:
            continue
        
        # Extract full content
        content_text = extract_article_content(article['url'])
        article['summary'] = content_text if content_text else 'Content unavailable'
        
        articles.append(article)
        time.sleep(0.5)
    
    return articles


def scrape_ticker(ticker, max_pages=20):
    ticker = ticker.lower()
    session = create_session()
    all_articles = []
    
    logger.info(f"Scraping {ticker.upper()} (max {max_pages} pages)")
    
    # Base URL without page number
    base_url = f"{BASE_URL}/investing/stock/{ticker}/moreheadlines?channel=AllDowJones&source=ChartingSymbol"
    
    articles = scrape_page(session, base_url, ticker)
    all_articles.extend(articles)
    logger.info(f"Base page: {len(articles)} articles")
    
    # Paginated URLs
    for page in range(1, max_pages + 1):
        url = f"{base_url}&pageNumber={page}"
        articles = scrape_page(session, url, ticker)
        
        if not articles:
            logger.info(f"No articles on page {page}, stopping")
            break
        
        all_articles.extend(articles)
        logger.info(f"Page {page}: {len(articles)} articles")
        time.sleep(DELAY)
    
    logger.info(f"Total: {len(all_articles)} articles for {ticker.upper()}")
    return all_articles


def scrape_multiple_tickers(tickers, max_pages=10):
    results = {}
    
    for i, ticker in enumerate(tickers, 1):
        logger.info(f"\n[{i}/{len(tickers)}] Processing {ticker.upper()}")
        
        try:
            articles = scrape_ticker(ticker, max_pages)
            results[ticker.upper()] = articles
            
            if i < len(tickers):
                time.sleep(DELAY)
                
        except Exception as e:
            logger.error(f"Error processing {ticker.upper()}: {e}")
            results[ticker.upper()] = []
    
    return results


def main():
    ticker = "amzn"
    max_pages = 1
    
    articles = scrape_ticker(ticker, max_pages)
    
    if articles:
        logger.info(f"Saving {len(articles)} articles to database")
        for article in articles:
            if 'summary' in article and article['summary'] != 'Content unavailable':
                article['embedding'] = create_embedding(article['summary'])
            else:
                article['embedding'] = create_embedding(article['title'])
            create_document('meet_news', article)
        logger.info("Complete")
    else:
        logger.info("No articles found")


if __name__ == "__main__":
    main()