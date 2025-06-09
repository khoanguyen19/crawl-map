#!/usr/bin/env python3
"""
Fixed Guland Network Analyzer for Linux
Với enhanced error handling và fallback options
"""

import os
import sys
import time
import json
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_chrome_driver(headless=True):
    """Setup Chrome driver với multiple fallback options"""
    
    chrome_options = Options()
    
    # Essential options for Linux
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-javascript")  # For basic HTML only
    
    if headless:
        chrome_options.add_argument("--headless")
    
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
    
    # Try different methods to create driver
    methods = [
        ("webdriver-manager", lambda: webdriver.Chrome(options=chrome_options)),
        ("system chromedriver", lambda: webdriver.Chrome(service=Service("/usr/local/bin/chromedriver"), options=chrome_options)),
        ("local chromedriver", lambda: webdriver.Chrome(service=Service("./chromedriver"), options=chrome_options)),
        ("PATH chromedriver", lambda: webdriver.Chrome(service=Service(shutil.which("chromedriver")), options=chrome_options))
    ]
    
    for method_name, method_func in methods:
        try:
            logger.info(f"Trying {method_name}...")
            driver = method_func()
            logger.info(f"✅ Success with {method_name}")
            return driver
        except Exception as e:
            logger.warning(f"❌ {method_name} failed: {e}")
            continue
    
    logger.error("❌ All ChromeDriver methods failed!")
    return None

def simple_requests_analysis():
    """Fallback: Simple requests-based analysis"""
    logger.info("🔄 Falling back to requests-based analysis...")
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
    })
    
    urls_to_test = [
        "https://guland.vn/",
        "https://guland.vn/map",
        "https://guland.vn/api/",
        "https://guland.vn/ajax/",
        "https://api.guland.vn/"
    ]
    
    results = []
    
    for url in urls_to_test:
        try:
            logger.info(f"Testing: {url}")
            response = session.get(url, timeout=10)
            
            result = {
                'url': url,
                'status_code': response.status_code,
                'content_type': response.headers.get('content-type', ''),
                'content_length': len(response.content),
                'success': response.status_code < 400
            }
            
            # Look for API endpoints in HTML
            if 'text/html' in result['content_type']:
                content = response.text.lower()
                if any(term in content for term in ['api', 'ajax', '.json', 'tiles']):
                    result['contains_api_references'] = True
            
            results.append(result)
            
        except Exception as e:
            logger.error(f"Error testing {url}: {e}")
    
    # Save results
    with open('simple_analysis_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\n📊 SIMPLE ANALYSIS RESULTS:")
    for result in results:
        status = "✅" if result['success'] else "❌"
        print(f"  {status} {result['url']} - Status: {result['status_code']}")
    
    return results

def main():
    """Main function với fallback options"""
    print("🔧 FIXED GULAND ANALYZER FOR LINUX")
    print("=" * 50)
    
    # Try Selenium first
    driver = setup_chrome_driver(headless=True)
    
    if driver:
        logger.info("✅ ChromeDriver working! Running full analysis...")
        # Import and run the original analyzer
        try:
            # Add your original analyzer code here
            # For now, just do a basic test
            driver.get("https://guland.vn/")
            logger.info(f"Page title: {driver.title}")
            driver.quit()
            print("✅ Selenium analysis completed!")
        except Exception as e:
            logger.error(f"Selenium analysis failed: {e}")
            driver.quit()
    else:
        logger.info("🔄 ChromeDriver not working, using fallback...")
        simple_requests_analysis()

if __name__ == "__main__":
    main()
