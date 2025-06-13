#!/usr/bin/env python3
"""
LinkedIn Profile Bookmark Generator
Converts LinkedIn profile URLs into organized browser bookmarks HTML file.
"""

import os
import sys
import time
import json
import argparse
import pyperclip
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib.parse import urlparse, urljoin
from datetime import datetime
import pandas as pd
import logging
from typing import Dict, List, Tuple, Optional

# --- CONFIGURATION ---
DEFAULT_DELAY = 3  # Conservative delay between requests
DEFAULT_OUTPUT = f'linkedin_bookmarks_{datetime.now().strftime("%Y%m%d_%H%M")}.html'
DEFAULT_CONFIG = os.path.join(os.path.expanduser("~"), '.linkedin_bookmarker_config.json')
MAX_RETRIES = 3
REQUEST_TIMEOUT = 20
MAX_URLS = 500  # Safety limit for input URLs

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
def fetch_profile_data(url: str, retries: int = MAX_RETRIES) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Fetch and parse LinkedIn profile data
    Args:
        url: LinkedIn profile URL
        retries: Number of retry attempts
    Returns:
        Tuple: (profile_data, error_message)
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }
    
    for attempt in range(retries):
        try:
            # Respect robots.txt and add delay
            time.sleep(DEFAULT_DELAY * (attempt + 0.5))
            
            resp = requests.get(
                url,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True
            )
            
            # Handle rate limiting
            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 30))
                logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                continue
                
            if resp.status_code != 200:
                return None, f"HTTP {resp.status_code}"
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Extract profile data using multiple strategies
            profile_data = extract_profile_data(soup)
            if profile_data['name']:
                return profile_data, None
                
            return None, "No parsable data found"
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed (attempt {attempt + 1}): {str(e)}")
            if attempt == retries - 1:
                return None, f"Request failed: {str(e)}"
            
    return None, "Max retries exceeded"

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
        description='Convert LinkedIn profile URLs to organized browser bookmarks.',
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
        logger.info("Starting LinkedIn Bookmark Generator")
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
                    pbar.set_postfix({
                        'success': processed_count,
                        'failed': len(failed_urls)
                    })
                    
                    time.sleep(args.delay)
                    
                except Exception as e:
                    logger.error(f"Error processing {url}: {str(e)}")
                    failed_urls.append(url)
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