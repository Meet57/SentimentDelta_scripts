"""
Web scraping utilities for stock market news and sentiment data.
Provides clean, reusable functions for scraping financial news articles.
"""

import time
import random
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from newspaper import Article

from ..utils.logger import get_logger, log_operation_start, log_operation_end, log_error

logger = get_logger(__name__)

# Constants
BASE_URL = "https://www.marketwatch.com"
DEFAULT_DELAY = 2
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_PAGES = 20

USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]


def create_session() -> requests.Session:
    """
    Create a configured requests session with retry strategy.
    
    Returns:
        Configured requests session
    """
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3, 
        status_forcelist=[429, 500, 502, 503, 504], 
        backoff_factor=1
    )
    
    session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
    })
    
    return session


def fetch_page_content(session: requests.Session, url: str, max_retries: int = 3) -> Optional[bytes]:
    """
    Fetch page content with retry logic.
    
    Args:
        session: Requests session
        url: URL to fetch
        max_retries: Maximum number of retry attempts
    
    Returns:
        Page content as bytes or None if failed
    """
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                time.sleep(DEFAULT_DELAY * attempt)
            
            response = session.get(url, timeout=DEFAULT_TIMEOUT)
            
            if response.status_code == 200:
                return response.content
            
            if response.status_code in [401, 403, 429]:
                logger.warning(f"HTTP {response.status_code} for {url} (attempt {attempt + 1})")
                continue
            
            response.raise_for_status()
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed for {url} (attempt {attempt + 1}): {str(e)[:100]}")
            if attempt == max_retries - 1:
                return None
    
    return None


def extract_article_text(url: str) -> Optional[str]:
    """
    Extract article text content using newspaper library.
    
    Args:
        url: Article URL
    
    Returns:
        Article text or None if extraction failed
    """
    try:
        article = Article(url)
        article.download()
        article.parse()
        
        if article.text and len(article.text.strip()) > 50:
            return article.text.strip()
        
        return None
        
    except Exception as e:
        logger.debug(f"Article extraction failed for {url}: {str(e)[:100]}")
        return None


def parse_article_element(element, ticker: str) -> Optional[Dict[str, Any]]:
    """
    Parse an article element from the page.
    
    Args:
        element: BeautifulSoup element containing article data
        ticker: Stock ticker symbol
    
    Returns:
        Article data dictionary or None if parsing failed
    """
    # Try different selectors for headline and URL
    headline_selectors = ['h3.article__headline a', 'h3 a', 'h2 a', 'h4 a']
    headline = None
    url = None
    
    for selector in headline_selectors:
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
    
    # Process URL
    if url:
        if not url.startswith('http'):
            article['url'] = urljoin(BASE_URL, url)
        else:
            article['url'] = url
    
    # Extract author
    author_elem = element.select_one('span.article__author')
    if author_elem:
        author_text = author_elem.get_text(strip=True)
        article['author'] = author_text.replace('by ', '').replace('By ', '')
    
    # Extract timestamp
    timestamp = element.get('data-timestamp')
    if timestamp:
        article['data_timestamp'] = timestamp
    
    return article


def scrape_ticker_page(session: requests.Session, url: str, ticker: str) -> List[Dict[str, Any]]:
    """
    Scrape articles from a single page for a ticker.
    
    Args:
        session: Requests session
        url: Page URL to scrape
        ticker: Stock ticker symbol
    
    Returns:
        List of article dictionaries
    """
    content = fetch_page_content(session, url)
    if not content:
        return []
    
    soup = BeautifulSoup(content, 'html.parser')
    container = soup.find('div', class_='collection__elements j-scrollElement')
    
    if not container:
        logger.warning(f"No article container found on page: {url}")
        return []
    
    articles = []
    elements = container.find_all('div', class_=lambda x: x and 'element--article' in x)
    
    for element in elements:
        article = parse_article_element(element, ticker)
        if not article or 'url' not in article:
            continue
        
        # Extract full article content
        article_text = extract_article_text(article['url'])
        article['summary'] = article_text if article_text else 'Content unavailable'
        
        articles.append(article)
        time.sleep(0.5)  # Rate limiting
    
    return articles


def build_ticker_url(ticker: str, page: Optional[int] = None) -> str:
    """
    Build URL for ticker news page.
    
    Args:
        ticker: Stock ticker symbol
        page: Page number (optional)
    
    Returns:
        Complete URL for the ticker page
    """
    base_url = f"{BASE_URL}/investing/stock/{ticker.lower()}/moreheadlines"
    params = "?channel=AllDowJones&source=ChartingSymbol"
    
    if page is not None:
        params += f"&pageNumber={page}"
    
    return base_url + params


def scrape_ticker_news(ticker: str, max_pages: int = DEFAULT_MAX_PAGES) -> List[Dict[str, Any]]:
    """
    Scrape news articles for a specific ticker.
    
    Args:
        ticker: Stock ticker symbol
        max_pages: Maximum number of pages to scrape
    
    Returns:
        List of article dictionaries
    """
    log_operation_start(logger, f"scraping news for {ticker.upper()}", max_pages=max_pages)
    
    session = create_session()
    all_articles = []
    
    # Scrape base page (no page number)
    base_url = build_ticker_url(ticker)
    articles = scrape_ticker_page(session, base_url, ticker)
    all_articles.extend(articles)
    logger.info(f"Base page for {ticker.upper()}: {len(articles)} articles")
    
    # Scrape paginated pages
    for page in range(1, max_pages + 1):
        url = build_ticker_url(ticker, page)
        articles = scrape_ticker_page(session, url, ticker)
        
        if not articles:
            logger.info(f"No articles found on page {page} for {ticker.upper()}, stopping")
            break
        
        all_articles.extend(articles)
        logger.info(f"Page {page} for {ticker.upper()}: {len(articles)} articles")
        time.sleep(DEFAULT_DELAY)
    
    log_operation_end(logger, f"scraping news for {ticker.upper()}", total_articles=len(all_articles))
    return all_articles


def scrape_multiple_tickers(tickers: List[str], max_pages: int = 10) -> Dict[str, List[Dict[str, Any]]]:
    """
    Scrape news articles for multiple tickers.
    
    Args:
        tickers: List of stock ticker symbols
        max_pages: Maximum number of pages per ticker
    
    Returns:
        Dictionary mapping ticker to list of articles
    """
    log_operation_start(logger, "scraping multiple tickers", count=len(tickers), max_pages=max_pages)
    
    results = {}
    
    for i, ticker in enumerate(tickers, 1):
        logger.info(f"Processing ticker {i}/{len(tickers)}: {ticker.upper()}")
        
        try:
            articles = scrape_ticker_news(ticker, max_pages)
            results[ticker.upper()] = articles
            
            if i < len(tickers):
                time.sleep(DEFAULT_DELAY)
                
        except Exception as e:
            log_error(logger, f"scraping {ticker.upper()}", e)
            results[ticker.upper()] = []
    
    total_articles = sum(len(articles) for articles in results.values())
    log_operation_end(logger, "scraping multiple tickers", 
                     total_articles=total_articles, tickers_processed=len(results))
    
    return results


def prepare_article_for_storage(article: Dict[str, Any], embedding_function) -> Dict[str, Any]:
    """
    Prepare article data for database storage by adding embeddings.
    
    Args:
        article: Article data dictionary
        embedding_function: Function to create embeddings
    
    Returns:
        Article data with embeddings added
    """
    article_copy = article.copy()
    
    # Create embedding from summary or title
    text_for_embedding = article_copy.get('summary', '')
    if not text_for_embedding or text_for_embedding == 'Content unavailable':
        text_for_embedding = article_copy.get('title', '')
    
    if text_for_embedding:
        article_copy['embedding'] = embedding_function(text_for_embedding)
    
    return article_copy