#!/usr/bin/env python3
"""
Image scraper module for water damage images.
Uses multiple engines and methods to collect images.
"""

import os
import time
import random
import requests
import urllib3
import logging
import ssl
from icrawler.builtin import GoogleImageCrawler, BingImageCrawler
from urllib.parse import urlparse
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Disable SSL warnings (not recommended for production)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure default parameters
DEFAULT_MAX_PER_TERM = 100  # Conservative limit
DEFAULT_DELAY_RANGE = (5, 15)  # Random delay between requests
DEFAULT_MAX_RETRIES = 2
DEFAULT_SESSION_DURATION = 7200  # 2 hour max per session
DEFAULT_TARGET_COUNT = 10000

# Create SSL context
SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

class RobustImageScraper:
    """Robust image scraper that uses multiple methods to collect images."""
    
    def __init__(self, output_dir="water_damage_images", target_count=DEFAULT_TARGET_COUNT,
                 max_per_term=DEFAULT_MAX_PER_TERM, session_duration=DEFAULT_SESSION_DURATION):
        """Initialize the scraper.
        
        Args:
            output_dir (str): Directory to store images
            target_count (int): Target number of images to collect
            max_per_term (int): Maximum images to collect per search term
            session_duration (int): Maximum session duration in seconds
        """
        self.output_dir = output_dir
        self.target_count = target_count
        self.max_per_term = max_per_term
        self.session_duration = session_duration
        self.collected_urls = set()
        self.success_count = 0
        self.start_time = time.time()
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize user agent generator
        self.ua = UserAgent()
        
        # Selenium driver setup
        self.selenium_driver = self.init_selenium_driver()
        
        # Logger
        self.logger = logging.getLogger(__name__)
        
    def init_selenium_driver(self):
        """Initialize Selenium WebDriver with proper options"""
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(f"user-agent={self.ua.random}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver
        
    def safe_download(self, url):
        """Enhanced download function with SSL bypass and better error handling.
        
        Args:
            url (str): URL to download
            
        Returns:
            bool: True if download successful, False otherwise
        """
        if not url.startswith(('http://', 'https://')):
            return False
            
        for attempt in range(DEFAULT_MAX_RETRIES):
            try:
                headers = {
                    'User-Agent': self.ua.random,
                    'Referer': 'https://www.google.com/',
                    'Accept': 'image/webp,*/*',
                    'Accept-Language': 'en-US,en;q=0.5'
                }
                
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=15,
                    stream=True,
                    verify=False,  # Bypass SSL verification
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    content_type = response.headers.get('content-type', '')
                    if 'image' not in content_type:
                        self.logger.warning(f"Non-image content at {url}")
                        return False
                        
                    ext = self.get_extension(content_type, url)
                    if not ext:
                        return False
                        
                    filename = f"{int(time.time())}_{random.randint(1000,9999)}{ext}"
                    filepath = os.path.join(self.output_dir, filename)
                    
                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(8192):
                            f.write(chunk)
                    
                    # Verify the downloaded image
                    if os.path.getsize(filepath) < 1024:  # Too small to be valid
                        os.remove(filepath)
                        return False
                    
                    sleep_time = random.uniform(*DEFAULT_DELAY_RANGE)
                    self.logger.info(f"Downloaded {filename} (Size: {os.path.getsize(filepath)//1024}KB)")
                    time.sleep(sleep_time)
                    return True
                elif response.status_code == 403:
                    self.logger.warning(f"403 Forbidden for {url}")
                    return False
                    
            except Exception as e:
                self.logger.warning(f"Attempt {attempt+1} failed for {url}: {str(e)}")
                time.sleep(attempt * 5)
                
        return False
        
    def get_extension(self, content_type, url):
        """Determine file extension with fallback to URL analysis.
        
        Args:
            content_type (str): Content-Type header
            url (str): URL of the image
            
        Returns:
            str: File extension including the dot, or None if unknown
        """
        # From content-type first
        if 'jpeg' in content_type or 'jpg' in content_type:
            return '.jpg'
        elif 'png' in content_type:
            return '.png'
        elif 'gif' in content_type:
            return '.gif'
        elif 'webp' in content_type:
            return '.webp'
        
        # Fallback to URL analysis
        parsed = urlparse(url)
        path = parsed.path.lower()
        if path.endswith('.jpg') or path.endswith('.jpeg'):
            return '.jpg'
        elif path.endswith('.png'):
            return '.png'
        elif path.endswith('.gif'):
            return '.gif'
        elif path.endswith('.webp'):
            return '.webp'
            
        return None
        
    def run_crawler(self, crawler_class, term, max_num):
        """Run crawler with comprehensive error handling.
        
        Args:
            crawler_class: Image crawler class to use
            term (str): Search term
            max_num (int): Maximum number of images to collect
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            crawler = crawler_class(
                storage={'root_dir': self.output_dir},
                downloader_threads=1,
                parser_threads=1,
                feeder_threads=1,
                log_level=logging.WARNING
            )
            
            crawler.crawl(
                keyword=term,
                max_num=max_num,
                min_size=(400, 400),
                filters={'type': 'photo', 'color': 'color'},
                overwrite=False,
                file_idx_offset='auto'
            )
            return True
        except Exception as e:
            self.logger.error(f"{crawler_class.__name__} failed for '{term}': {str(e)}")
            time.sleep(30)  # Extended sleep after failure
            return False
            
    def scrape_with_selenium(self, term, num_images=50):
        """Alternative method using Selenium for problematic sites.
        
        Args:
            term (str): Search term
            num_images (int): Maximum number of images to collect
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            url = f"https://www.google.com/search?q={term}&tbm=isch"
            self.selenium_driver.get(url)
            time.sleep(random.uniform(3, 7))
            
            images = []
            elements = self.selenium_driver.find_elements(By.CSS_SELECTOR, "img.rg_i")
            
            for i, element in enumerate(elements[:num_images]):
                if self.success_count >= self.target_count:
                    break
                    
                try:
                    self.selenium_driver.execute_script("arguments[0].click();", element)
                    time.sleep(random.uniform(1, 3))
                    
                    # Try multiple selectors for the actual image
                    selectors = [
                        "img.n3VNCb",  # Google's main image selector
                        "img.sFlh5c",   # Alternative selector
                        "img.pT0Scc"    # Another possible selector
                    ]
                    
                    img_src = None
                    for selector in selectors:
                        try:
                            img = self.selenium_driver.find_element(By.CSS_SELECTOR, selector)
                            img_src = img.get_attribute('src')
                            if img_src and img_src.startswith('http'):
                                break
                        except:
                            continue
                    
                    if img_src and img_src not in self.collected_urls:
                        if self.safe_download(img_src):
                            self.collected_urls.add(img_src)
                            self.success_count += 1
                            self.logger.info(f"[Selenium] Downloaded image {self.success_count}/{self.target_count}")
                            
                except Exception as e:
                    self.logger.warning(f"Selenium click failed on image {i}: {str(e)}")
                    continue
                    
            return True
        except Exception as e:
            self.logger.error(f"Selenium failed for '{term}': {str(e)}")
            return False
            
    def process_url_file(self, filename):
        """Process URLs from a saved file.
        
        Args:
            filename (str): Name of the file containing URLs
            
        Returns:
            bool: True if successful, False otherwise
        """
        with open(os.path.join(self.output_dir, filename)) as f:
            urls = [line.strip() for line in f if line.strip()]
            
        for url in urls:
            if self.success_count >= self.target_count:
                return True
                
            if url not in self.collected_urls:
                if self.safe_download(url):
                    self.success_count += 1
                    self.collected_urls.add(url)
                    
        return False
        
    def crawl_images(self, search_terms):
        """Main crawling function with improved session management.
        
        Args:
            search_terms (list): List of search terms
        """
        engines = [
            (GoogleImageCrawler, {'max_num': self.max_per_term}),
            (BingImageCrawler, {'max_num': self.max_per_term})
        ]
        
        # Randomize search terms for better distribution
        random.shuffle(search_terms)
        
        while (time.time() - self.start_time) < self.session_duration and self.success_count < self.target_count:
            for term in search_terms:
                if self.success_count >= self.target_count:
                    break
                    
                self.logger.info(f"Processing term: '{term}' (Total: {self.success_count}/{self.target_count})")
                
                # First try traditional crawlers
                for crawler_class, config in engines:
                    if (time.time() - self.start_time) > self.session_duration:
                        self.logger.info("Session duration limit reached")
                        return
                        
                    self.logger.info(f"Using {crawler_class.__name__}")
                    self.run_crawler(crawler_class, term, config['max_num'])
                    time.sleep(random.uniform(20, 40))  # Extended delay
                
                # Then try Selenium if needed
                if self.success_count < self.target_count * 0.8:  # Only if we're behind target
                    self.logger.info("Attempting Selenium scrape")
                    self.scrape_with_selenium(term)
                    time.sleep(random.uniform(30, 60))  # Longer delay after Selenium
                    
            # Process any saved URL files
            for filename in os.listdir(self.output_dir):
                if filename.endswith('.txt'):
                    self.process_url_file(filename)
                    
            self.logger.info(f"Completed cycle. Total images: {self.success_count}")
            time.sleep(60)  # Major break between cycles
            
    def __del__(self):
        """Clean up resources"""
        if hasattr(self, 'selenium_driver'):
            try:
                self.selenium_driver.quit()
            except:
                pass

if __name__ == "__main__":
    # Configure logging for standalone usage
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('water_damage_scraper.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    # Example standalone usage
    logger.info("Starting enhanced water damage image scraper")
    
    # Load search terms
    with open('search_terms.json', 'r', encoding='utf-8') as f:
        search_terms_data = json.load(f)
        search_terms = search_terms_data.get('search_terms', [])
    
    scraper = RobustImageScraper()
    try:
        scraper.crawl_images(search_terms)
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    finally:
        logger.info(f"Scraping completed. Total images downloaded: {scraper.success_count}")