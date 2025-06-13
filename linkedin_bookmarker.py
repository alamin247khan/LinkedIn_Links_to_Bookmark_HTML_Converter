#!/usr/bin/env python3
"""
LinkedIn Profile Bookmark Generator with Anti-Bot Protection
Converts LinkedIn profile URLs into organized browser bookmarks HTML file.
Includes proxy rotation, cookie handling, and CAPTCHA solving.
"""

import os
import sys
import time
import json
import random
import argparse
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
import logging
from typing import Dict, List, Tuple, Optional, Union

# --- CONFIGURATION ---
DEFAULT_DELAY = 10  # Conservative delay between requests (seconds)
DEFAULT_OUTPUT = f'linkedin_bookmarks_{datetime.now().strftime("%Y%m%d_%H%M")}.html'
DEFAULT_CONFIG = os.path.join(os.path.expanduser("~"), '.linkedin_bookmarker_config.json')
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30
MAX_URLS = 500  # Safety limit for input URLs
MAX_CONSECUTIVE_FAILURES = 5  # Stop after this many consecutive failures

# Proxy configuration (replace with your own)
PROXY_LIST = [
    '123.123.123.123:8000',
    '111.222.111.222:8080',
    # Add more proxies here
]

# User-Agent rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
]

# LinkedIn credentials (for cookie refresh)
LINKEDIN_EMAIL = os.getenv('LINKEDIN_EMAIL', '')
LINKEDIN_PASSWORD = os.getenv('LINKEDIN_PASSWORD', '')

# 2Captcha API key (optional)
CAPTCHA_API_KEY = os.getenv('CAPTCHA_API_KEY', '')

# --- CUSTOM EXCEPTIONS ---
class LinkedInScraperError(Exception):
    """Base exception for scraper-related errors"""
    pass

class InvalidURLException(LinkedInScraperError):
    """Exception for invalid LinkedIn URLs"""
    pass

class RateLimitException(LinkedInScraperError):
    """Exception for rate limiting"""
    pass

class BlockedException(LinkedInScraperError):
    """Exception for when blocked by LinkedIn"""
    pass

class CaptchaException(LinkedInScraperError):
    """Exception for CAPTCHA requirements"""
    pass

# --- LOGGING SETUP ---
def setup_logging() -> logging.Logger:
    """Configure logging for the application"""
    logger = logging.getLogger('linkedin_bookmarker')
    logger.setLevel(logging.INFO)
    
    # Create file handler
    log_file = os.path.join(os.path.dirname(__file__), 'linkedin_bookmarker.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    
    # Create formatter and add to handlers
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# --- PROXY MANAGEMENT ---
class ProxyManager:
    """Manages proxy rotation and health checks"""
    def __init__(self, proxy_list: List[str]):
        self.proxies = proxy_list
        self.current_proxy = None
        self.blacklisted = set()
        
    def get_proxy(self) -> Dict[str, str]:
        """Get a random working proxy"""
        available = [p for p in self.proxies if p not in self.blacklisted]
        if not available:
            raise LinkedInScraperError("No available proxies")
            
        self.current_proxy = random.choice(available)
        return {
            'http': f'http://{self.current_proxy}',
            'https': f'http://{self.current_proxy}'
        }
        
    def mark_bad(self, proxy: str):
        """Mark a proxy as bad"""
        self.blacklisted.add(proxy)
        logger.warning(f"Proxy blacklisted: {proxy}")

proxy_manager = ProxyManager(PROXY_LIST)

# --- COOKIE MANAGEMENT ---
class CookieManager:
    """Manages LinkedIn session cookies"""
    def __init__(self):
        self.cookies = {}
        self.last_refresh = 0
        
    def get_cookies(self) -> Dict[str, str]:
        """Get current cookies or refresh if needed"""
        if not self.cookies or time.time() - self.last_refresh > 3600:  # Refresh every hour
            self.refresh_cookies()
        return self.cookies
        
    def refresh_cookies(self):
        """Refresh LinkedIn session cookies using Selenium"""
        if not LINKEDIN_EMAIL or not LINKEDIN_PASSWORD:
            raise LinkedInScraperError("LinkedIn credentials not configured")
            
        logger.info("Refreshing LinkedIn cookies...")
        
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.get("https://www.linkedin.com/login")
            
            # Fill login form
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "username"))
            ).send_keys(LINKEDIN_EMAIL)
            
            driver.find_element(By.ID, "password").send_keys(LINKEDIN_PASSWORD)
            driver.find_element(By.XPATH, "//button[@type='submit']").click()
            
            # Wait for login to complete
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//input[@role='combobox' and @aria-label='Search']"))
            )
            
            # Get cookies
            self.cookies = {c['name']: c['value'] for c in driver.get_cookies()}
            self.last_refresh = time.time()
            logger.info("Successfully refreshed cookies")
            
        except Exception as e:
            logger.error(f"Failed to refresh cookies: {str(e)}")
            raise LinkedInScraperError(f"Cookie refresh failed: {str(e)}")
            
        finally:
            if 'driver' in locals():
                driver.quit()

cookie_manager = CookieManager()

# --- CAPTCHA SOLVING ---
class CaptchaSolver:
    """Handles CAPTCHA solving using 2Captcha service"""
    def __init__(self, api_key: str):
        self.api_key = api_key
        
    def solve_recaptcha(self, site_key: str, page_url: str) -> Optional[str]:
        """Solve reCAPTCHA using 2Captcha API"""
        if not self.api_key:
            return None
            
        submit_url = "http://2captcha.com/in.php"
        data = {
            'key': self.api_key,
            'method': 'userrecaptcha',
            'googlekey': site_key,
            'pageurl': page_url,
            'json': 1
        }
        
        try:
            # Submit CAPTCHA for solving
            response = requests.post(submit_url, data=data, timeout=60).json()
            if response.get('status') != 1:
                logger.error(f"CAPTCHA submission failed: {response.get('error_text')}")
                return None
                
            captcha_id = response['request']
            logger.info(f"CAPTCHA submitted, ID: {captcha_id}")
            
            # Check for solution
            result_url = f"http://2captcha.com/res.php?key={self.api_key}&action=get&id={captcha_id}&json=1"
            for _ in range(30):  # Wait up to 3 minutes
                time.sleep(6)
                result = requests.get(result_url, timeout=30).json()
                if result.get('status') == 1:
                    return result['request']
                elif result.get('request') == 'CAPCHA_NOT_READY':
                    continue
                else:
                    logger.error(f"CAPTCHA solving failed: {result.get('error_text')}")
                    return None
                    
        except Exception as e:
            logger.error(f"CAPTCHA solving error: {str(e)}")
            
        return None

captcha_solver = CaptchaSolver(CAPTCHA_API_KEY)

# --- REQUEST MANAGEMENT ---
def make_request(url: str) -> requests.Response:
    """
    Make a request with anti-bot protection measures
    Args:
        url: URL to request
    Returns:
        Response object
    """
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.google.com/',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            proxy = proxy_manager.get_proxy()
            cookies = cookie_manager.get_cookies()
            
            # Random delay to mimic human behavior
            time.sleep(random.uniform(DEFAULT_DELAY, DEFAULT_DELAY * 1.5))
            
            response = requests.get(
                url,
                headers=headers,
                cookies=cookies,
                proxies=proxy,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True
            )
            
            # Check for blocking
            if response.status_code == 999:
                proxy_manager.mark_bad(proxy_manager.current_proxy)
                raise RateLimitException("LinkedIn rate limit (HTTP 999)")
                
            if "security check" in response.text.lower():
                raise BlockedException("LinkedIn security check detected")
                
            if "captcha" in response.text.lower():
                raise CaptchaException("CAPTCHA required")
                
            if response.status_code != 200:
                raise LinkedInScraperError(f"HTTP {response.status_code}")
                
            return response
            
        except (requests.exceptions.ProxyError, requests.exceptions.ConnectionError):
            proxy_manager.mark_bad(proxy_manager.current_proxy)
            logger.warning(f"Proxy failed, trying another... (attempt {attempt + 1})")
            
        except (RateLimitException, BlockedException, CaptchaException) as e:
            logger.warning(f"Blocked detected: {str(e)}")
            time.sleep(random.uniform(30, 60))  # Longer delay when blocked
            if attempt == MAX_RETRIES - 1:
                raise
                
        except Exception as e:
            logger.warning(f"Request failed (attempt {attempt + 1}): {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise LinkedInScraperError(f"Request failed after {MAX_RETRIES} attempts: {str(e)}")
    
    raise LinkedInScraperError("Max retries exceeded")

# --- URL HANDLING ---
def validate_linkedin_url(url: str) -> bool:
    """
    Validate if a URL is a proper LinkedIn profile URL
    Args:
        url: URL to validate
    Returns:
        bool: True if valid LinkedIn profile URL
    """
    try:
        parsed = urlparse(url)
        if not all([parsed.scheme, parsed.netloc]):
            return False
        
        path = parsed.path.lower()
        valid_path = any(
            p in path for p in ['/in/', '/pub/', '/profile/', '/title/']
        )
        
        return (
            parsed.netloc.endswith(('linkedin.com', 'www.linkedin.com')) and 
            valid_path and 
            len(path.split('/')) > 2  # Must have username part
        )
    except Exception as e:
        logger.warning(f"URL validation error for {url}: {str(e)}")
        return False

def clean_url(url: str) -> str:
    """
    Normalize LinkedIn URLs to consistent format
    Args:
        url: URL to normalize
    Returns:
        str: Normalized URL
    """
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    parsed = urlparse(url)
    path = '/'.join(part for part in parsed.path.split('/') if part)
    
    # Remove tracking parameters
    clean_params = {}
    for param in parsed.query.split('&'):
        if '=' in param:
            key, val = param.split('=', 1)
            if key.lower() not in ['trk', 'ref', 'original_referer']:
                clean_params[key] = val
    
    # Reconstruct URL without tracking params
    query = '&'.join(f"{k}={v}" for k, v in clean_params.items())
    return urljoin(
        f"{parsed.scheme}://{parsed.netloc}", 
        f"{path}?{query}" if query else path
    )

# --- INPUT HANDLING ---
def get_urls_from_source(source: str, csv_column: str = 'url') -> List[str]:
    """
    Get URLs from various input sources
    Args:
        source: Input source (file path or 'clipboard')
        csv_column: Column name for CSV files
    Returns:
        List[str]: List of cleaned URLs
    """
    if source.lower() == 'clipboard':
        try:
            import pyperclip
            text = pyperclip.paste()
            urls = [clean_url(line) for line in text.splitlines() if line.strip()]
            logger.info(f"Found {len(urls)} URLs in clipboard")
            return urls[:MAX_URLS]  # Apply safety limit
        except Exception as e:
            logger.error(f"Clipboard access error: {str(e)}")
            raise LinkedInScraperError("Failed to read clipboard")
    
    try:
        if not os.path.exists(source):
            raise FileNotFoundError(f"Input file not found: {source}")
        
        if source.lower().endswith('.csv'):
            try:
                import pandas as pd
                df = pd.read_csv(source)
                if csv_column not in df.columns:
                    raise ValueError(f"Column '{csv_column}' not found in CSV")
                urls = [clean_url(url) for url in df[csv_column].dropna().tolist()]
                logger.info(f"Found {len(urls)} URLs in CSV file")
                return urls[:MAX_URLS]
            except Exception as e:
                logger.error(f"CSV processing error: {str(e)}")
                raise
        
        # Handle text file
        with open(source, 'r', encoding='utf-8') as f:
            urls = [clean_url(line) for line in f if line.strip()]
            logger.info(f"Found {len(urls)} URLs in text file")
            return urls[:MAX_URLS]
            
    except Exception as e:
        logger.error(f"Input processing error: {str(e)}")
        raise LinkedInScraperError(f"Failed to process input: {str(e)}")

# --- PROFILE SCRAPING ---
def fetch_profile_data(url: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Fetch and parse LinkedIn profile data with anti-bot measures
    Args:
        url: LinkedIn profile URL
    Returns:
        Tuple: (profile_data, error_message)
    """
    try:
        response = make_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract profile data using multiple strategies
        profile_data = extract_profile_data(soup)
        if profile_data['name']:
            return profile_data, None
            
        return None, "No parsable data found"
        
    except Exception as e:
        return None, str(e)

def extract_profile_data(soup: BeautifulSoup) -> Dict:
    """
    Extract profile data from BeautifulSoup object using multiple strategies
    Args:
        soup: BeautifulSoup object of profile page
    Returns:
        Dict: Profile data (name, company, title)
    """
    data = {'name': '', 'company': '', 'title': ''}
    
    # Strategy 1: OpenGraph/Schema.org metadata
    meta_name = soup.find('meta', property='og:title') or \
               soup.find('meta', {'name': 'og:title'})
    
    if meta_name:
        title_parts = meta_name.get('content', '').split('|')[0].split('-')
        data['name'] = title_parts[0].strip() if title_parts else ''
        if len(title_parts) > 1:
            data['title'] = title_parts[1].strip()
    
    # Strategy 2: Description meta tags
    company_meta = soup.find('meta', property='og:description') or \
                  soup.find('meta', {'name': 'description'})
    if company_meta and ' at ' in company_meta.get('content', ''):
        data['company'] = company_meta.get('content', '').split(' at ')[-1].split('.')[0].strip()
    
    # Strategy 3: Title tag fallback
    if not data['name'] and soup.title:
        title_text = soup.title.string.split('|')[0].strip()
        parts = title_text.split(' - ')
        data['name'] = parts[0].strip() if parts else ''
        if len(parts) > 1:
            job_company = parts[1].split(' at ')
            data['title'] = job_company[0].strip()
            if len(job_company) > 1:
                data['company'] = job_company[1].strip()
    
    # Strategy 4: New LinkedIn layout
    if not data['name']:
        name_element = soup.find('h1', class_='text-heading-xlarge')
        if name_element:
            data['name'] = name_element.get_text().strip()
    
    if not data['title']:
        title_element = soup.find('div', class_='text-body-medium')
        if title_element:
            data['title'] = title_element.get_text().strip()
    
    return data

# --- FOLDER CONFIGURATION ---
def load_folder_config(config_path: str) -> Dict:
    """
    Load and validate folder configuration
    Args:
        config_path: Path to config JSON file
    Returns:
        Dict: Folder configuration
    """
    default_config = {
        "Sales": ["sales", "account executive", "ae", "account manager"],
        "Engineering": ["engineer", "developer", "software", "devops", "sre"],
        "Executives": ["ceo", "cto", "founder", "president", "vp", "director"],
        "Marketing": ["marketing", "growth", "demand gen", "digital marketing"]
    }
    
    if not os.path.exists(config_path):
        logger.info(f"No config found at {config_path}, using defaults")
        # Save default config for future use
        save_folder_config(default_config, config_path)
        return default_config
        
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            if not isinstance(config, dict):
                raise ValueError("Config must be a dictionary")
            
            # Validate config structure
            for folder, keywords in config.items():
                if not isinstance(keywords, list):
                    raise ValueError(f"Keywords for {folder} must be a list")
            
            return config
    except Exception as e:
        logger.warning(f"Error loading config: {str(e)}. Using defaults")
        return default_config

def save_folder_config(config: Dict, config_path: str) -> bool:
    """
    Save folder configuration to file
    Args:
        config: Configuration dictionary
        config_path: Path to save config
    Returns:
        bool: True if successful
    """
    try:
        os.makedirs(os.path.dirname(config_path) or '.', exist_ok=True)
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Failed to save config: {str(e)}")
        return False

def assign_folder(profile_data: Dict, config: Dict) -> str:
    """
    Assign profile to folder based on config rules
    Args:
        profile_data: Profile information
        config: Folder configuration
    Returns:
        str: Folder name
    """
    if not profile_data:
        return 'Uncategorized'
    
    search_string = ' '.join([
        profile_data.get('name', ''),
        profile_data.get('title', ''),
        profile_data.get('company', '')
    ]).lower()
    
    for folder, keywords in config.items():
        if any(
            kw.lower() in search_string 
            for kw in keywords 
            if kw  # Skip empty keywords
        ):
            return folder
            
    return 'Uncategorized'

# --- BOOKMARK GENERATION ---
def generate_bookmark_title(profile_data: Dict, url: str) -> str:
    """
    Generate display title for bookmark
    Args:
        profile_data: Profile information
        url: Original URL
    Returns:
        str: Formatted title
    """
    if not profile_data:
        return url
    
    parts = [
        profile_data.get('name', '').strip(),
        profile_data.get('company', '').strip(),
        profile_data.get('title', '').strip()
    ]
    return ' | '.join(filter(None, parts)) or url

def make_bookmark_html(bookmarks_by_folder: Dict) -> str:
    """
    Generate Netscape-format bookmark HTML
    Args:
        bookmarks_by_folder: Organized bookmarks
    Returns:
        str: HTML content
    """
    timestamp = int(time.time())
    html = [
        '<!DOCTYPE NETSCAPE-Bookmark-file-1>',
        '<!-- This is an automatically generated file -->',
        '<!-- Created by LinkedIn Bookmark Generator -->',
        f'<!-- Generated on {datetime.now().isoformat()} -->',
        '<META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=UTF-8">',
        '<TITLE>LinkedIn Bookmarks</TITLE>',
        '<H1>LinkedIn Bookmarks</H1>',
        '<DL><p>'
    ]
    
    # Sort folders alphabetically
    for folder in sorted(bookmarks_by_folder.keys()):
        bookmarks = bookmarks_by_folder[folder]
        html.append(f'    <DT><H3 ADD_DATE="{timestamp}" LAST_MODIFIED="{timestamp}">{folder}</H3>')
        html.append('    <DL><p>')
        
        # Sort bookmarks by name
        for bm in sorted(bookmarks, key=lambda x: x['title'].lower()):
            html.append(
                f'    <DT><A HREF="{bm["url"]}" ADD_DATE="{timestamp}" '
                f'LAST_MODIFIED="{timestamp}">{bm["title"]}</A>'
            )
        
        html.append('    </DL><p>')
    
    html.append('</DL><p>')
    return '\n'.join(html)

def save_results(bookmarks_by_folder: Dict, output_path: str) -> bool:
    """
    Save bookmarks to HTML file
    Args:
        bookmarks_by_folder: Organized bookmarks
        output_path: Output file path
    Returns:
        bool: True if successful
    """
    try:
        html = make_bookmark_html(bookmarks_by_folder)
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        
        # Write with atomic save pattern
        temp_path = f"{output_path}.tmp"
        with open(temp_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        # Atomic rename
        if os.path.exists(output_path):
            os.remove(output_path)
        os.rename(temp_path, output_path)
        
        logger.info(f"Successfully saved {sum(len(v) for v in bookmarks_by_folder.values())} bookmarks to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save results: {str(e)}")
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass
        return False

# --- MAIN FUNCTION ---
def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='Convert LinkedIn profile URLs to organized browser bookmarks with anti-bot protection.',
        epilog='Example: python linkedin_bookmarker.py --input urls.txt --output bookmarks.html'
    )
    parser.add_argument(
        '--input',
        required=True,
        help='Input source (file path or "clipboard")'
    )
    parser.add_argument(
        '--csv-column',
        default='url',
        help='Column name for URLs in CSV files'
    )
    parser.add_argument(
        '--config',
        default=DEFAULT_CONFIG,
        help='Path to folder configuration JSON'
    )
    parser.add_argument(
        '--output',
        default=DEFAULT_OUTPUT,
        help='Output HTML file path'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=DEFAULT_DELAY,
        help='Delay between requests (seconds)'
    )
    parser.add_argument(
        '--skip-errors',
        action='store_true',
        help='Continue processing after errors'
    )
    parser.add_argument(
        '--show-config',
        action='store_true',
        help='Show default configuration and exit'
    )
    
    args = parser.parse_args()
    
    if args.show_config:
        default_config = load_folder_config(DEFAULT_CONFIG)
        print("Default Folder Configuration:")
        print(json.dumps(default_config, indent=2))
        sys.exit(0)
    
    try:
        logger.info("Starting LinkedIn Bookmark Generator with anti-bot protection")
        start_time = time.time()
        
        # Load and validate input
        urls = get_urls_from_source(args.input, args.csv_column)
        valid_urls = [u for u in urls if validate_linkedin_url(u)]
        
        if not valid_urls:
            logger.error("No valid LinkedIn URLs found")
            print("Error: No valid LinkedIn URLs found in input")
            sys.exit(1)
            
        logger.info(f"Processing {len(valid_urls)} valid URLs")
        
        # Load configuration
        folder_config = load_folder_config(args.config)
        bookmarks_by_folder = {}
        failed_urls = []
        processed_count = 0
        consecutive_failures = 0
        
        # Process URLs with progress bar
        with tqdm(
            valid_urls, 
            desc="Processing Profiles", 
            unit="profile",
            dynamic_ncols=True
        ) as pbar:
            for url in pbar:
                try:
                    profile_data, error = fetch_profile_data(url)
                    if error:
                        raise LinkedInScraperError(error)
                    
                    folder = assign_folder(profile_data, folder_config)
                    title = generate_bookmark_title(profile_data, url)
                    
                    bookmarks_by_folder.setdefault(folder, []).append({
                        'url': url,
                        'title': title,
                        'data': profile_data
                    })
                    
                    processed_count += 1
                    consecutive_failures = 0
                    pbar.set_postfix({
                        'success': processed_count,
                        'failed': len(failed_urls)
                    })
                    
                    # Random delay to mimic human behavior
                    time.sleep(random.uniform(args.delay, args.delay * 1.5))
                    
                except Exception as e:
                    logger.error(f"Error processing {url}: {str(e)}")
                    failed_urls.append(url)
                    consecutive_failures += 1
                    
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        logger.error("Too many consecutive failures, stopping...")
                        break
                        
                    if not args.skip_errors:
                        raise
                    continue
                
        # Save results
        if not save_results(bookmarks_by_folder, args.output):
            raise LinkedInScraperError("Failed to save results")
            
        # Print summary
        elapsed = time.time() - start_time
        print(f"\nSuccessfully processed {processed_count} profiles in {elapsed:.1f} seconds")
        print(f"Bookmarks saved to: {args.output}")
        
        if failed_urls:
            print(f"\nWarning: Failed to process {len(failed_urls)} URLs:")
            for url in failed_urls[:10]:  # Show first 10 failures
                print(f"  - {url}")
            if len(failed_urls) > 10:
                print(f"  ... and {len(failed_urls) - 10} more")
            
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"\nError: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()