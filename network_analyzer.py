#!/usr/bin/env python3
"""
Guland.vn Network Analysis Toolkit
Phân tích network traffic để tìm API endpoints và data patterns

Author: AI Assistant
Version: 1.0
"""

import json
import time
import re
import requests
import urllib.parse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import logging
from datetime import datetime
import os
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('guland_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GulandNetworkAnalyzer:
    def __init__(self, headless=False):
        """
        Initialize network analyzer
        """
        self.headless = headless
        self.driver = None
        self.captured_requests = []
        self.api_endpoints = set()
        self.tile_patterns = []
        self.data_endpoints = []
        
        # Create output directories
        os.makedirs('output/network_logs', exist_ok=True)
        os.makedirs('output/screenshots', exist_ok=True)
        os.makedirs('output/api_responses', exist_ok=True)
        
    def setup_driver(self):
        """
        Setup Chrome driver với network monitoring
        """
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
            
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Enable logging
        chrome_options.add_argument("--enable-logging")
        chrome_options.add_argument("--log-level=0")
        chrome_options.add_argument("--v=1")
        
        # Disable images để tăng tốc (optional)
        # chrome_options.add_argument("--disable-images")
        
        # User agent để không bị detect
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        
        self.driver = webdriver.Chrome(options=chrome_options)
        
        # Enable network domain
        self.driver.execute_cdp_cmd('Network.enable', {})
        self.driver.execute_cdp_cmd('Runtime.enable', {})
        
        logger.info("Chrome driver initialized with network monitoring")
        
    def capture_network_requests(self, duration=30):
        """
        Capture network requests trong khoảng thời gian nhất định
        """
        start_time = time.time()
        captured_requests = []
        
        def capture_request(message):
            if message['method'] == 'Network.responseReceived':
                response = message['params']['response']
                captured_requests.append({
                    'timestamp': time.time(),
                    'url': response['url'],
                    'method': response.get('method', 'GET'),
                    'status': response['status'],
                    'mimeType': response.get('mimeType', ''),
                    'headers': response.get('headers', {}),
                    'responseHeaders': response.get('responseHeaders', {})
                })
        
        # Add listener
        self.driver.add_cdp_listener('Network.responseReceived', capture_request)
        
        logger.info(f"Starting network capture for {duration} seconds...")
        
        # Wait for specified duration
        time.sleep(duration)
        
        self.captured_requests.extend(captured_requests)
        logger.info(f"Captured {len(captured_requests)} network requests")
        
        return captured_requests
    
    def analyze_homepage(self):
        """
        Phân tích trang chủ Guland
        """
        logger.info("=== ANALYZING HOMEPAGE ===")
        
        try:
            self.driver.get("https://guland.vn/")
            
            # Wait for page load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Take screenshot
            self.driver.save_screenshot("output/screenshots/homepage.png")
            
            # Capture network requests
            requests = self.capture_network_requests(15)
            
            # Save to file
            with open("output/network_logs/homepage_requests.json", "w") as f:
                json.dump(requests, f, indent=2, ensure_ascii=False)
                
            self.analyze_requests(requests, "homepage")
            
        except Exception as e:
            logger.error(f"Error analyzing homepage: {e}")
    
    def analyze_map_interface(self):
        """
        Phân tích giao diện bản đồ chính
        """
        logger.info("=== ANALYZING MAP INTERFACE ===")
        
        try:
            self.driver.get("https://guland.vn/map")
            
            # Wait for map to load
            time.sleep(10)
            
            # Take screenshot
            self.driver.save_screenshot("output/screenshots/map_interface.png")
            
            # Capture initial load
            requests = self.capture_network_requests(20)
            
            # Try to interact with map
            self.interact_with_map()
            
            # Capture interaction requests
            interaction_requests = self.capture_network_requests(15)
            
            # Combine all requests
            all_requests = requests + interaction_requests
            
            # Save to file
            with open("output/network_logs/map_requests.json", "w") as f:
                json.dump(all_requests, f, indent=2, ensure_ascii=False)
                
            self.analyze_requests(all_requests, "map")
            
        except Exception as e:
            logger.error(f"Error analyzing map interface: {e}")
    
    def interact_with_map(self):
        """
        Tương tác với bản đồ để trigger network requests
        """
        logger.info("Interacting with map...")
        
        try:
            # Find map container
            map_element = self.driver.find_element(By.TAG_NAME, "body")
            
            # Zoom in/out
            actions = ActionChains(self.driver)
            
            # Simulate mouse wheel zoom
            for i in range(3):
                actions.scroll_by_amount(0, 120).perform()  # Zoom in
                time.sleep(2)
                
            for i in range(2):
                actions.scroll_by_amount(0, -120).perform()  # Zoom out
                time.sleep(2)
            
            # Try to pan around
            actions.drag_and_drop_by_offset(map_element, 100, 100).perform()
            time.sleep(2)
            
            actions.drag_and_drop_by_offset(map_element, -150, -50).perform()
            time.sleep(2)
            
            # Try to click on map elements
            try:
                clickable_elements = self.driver.find_elements(By.CSS_SELECTOR, 
                    "[onclick], [onmousedown], .clickable, .marker, .pin")
                
                for elem in clickable_elements[:3]:  # Click first 3 elements
                    try:
                        elem.click()
                        time.sleep(2)
                    except:
                        continue
                        
            except Exception as e:
                logger.warning(f"Could not interact with map elements: {e}")
                
        except Exception as e:
            logger.error(f"Error during map interaction: {e}")
    
    def analyze_provincial_pages(self, provinces=None):
        """
        Phân tích các trang tỉnh thành cụ thể
        """
        if provinces is None:
            provinces = ["ha-noi", "ho-chi-minh", "da-nang"]  # Test với 3 tỉnh
            
        logger.info(f"=== ANALYZING PROVINCIAL PAGES: {provinces} ===")
        
        for province in provinces:
            try:
                logger.info(f"Analyzing province: {province}")
                
                url = f"https://guland.vn/soi-quy-hoach/{province}"
                self.driver.get(url)
                
                # Wait for page load
                time.sleep(8)
                
                # Take screenshot
                self.driver.save_screenshot(f"output/screenshots/{province}.png")
                
                # Capture requests
                requests = self.capture_network_requests(15)
                
                # Try interactions
                self.interact_with_map()
                interaction_requests = self.capture_network_requests(10)
                
                all_requests = requests + interaction_requests
                
                # Save to file
                with open(f"output/network_logs/{province}_requests.json", "w") as f:
                    json.dump(all_requests, f, indent=2, ensure_ascii=False)
                    
                self.analyze_requests(all_requests, province)
                
            except Exception as e:
                logger.error(f"Error analyzing province {province}: {e}")
    
    def analyze_requests(self, requests, context):
        """
        Phân tích chi tiết các network requests
        """
        logger.info(f"Analyzing {len(requests)} requests for context: {context}")
        
        # Categorize requests
        api_requests = []
        tile_requests = []
        static_requests = []
        data_requests = []
        
        for req in requests:
            url = req['url']
            mime_type = req.get('mimeType', '')
            
            # Identify API endpoints
            if any(pattern in url.lower() for pattern in ['/api/', '/ajax/', '/json', '.json']):
                api_requests.append(req)
                self.api_endpoints.add(url)
                
            # Identify map tiles
            elif any(pattern in url.lower() for pattern in ['/tiles/', '.png', '.jpg', '/map/', 'tile']):
                if any(coord in url for coord in ['{z}', '{x}', '{y}']) or \
                   re.search(r'/\d+/\d+/\d+\.(png|jpg)', url):
                    tile_requests.append(req)
                    self.tile_patterns.append(url)
                    
            # Identify data endpoints
            elif any(pattern in url.lower() for pattern in ['data', 'geojson', 'kml', 'wms', 'wfs']):
                data_requests.append(req)
                self.data_endpoints.append(url)
                
            # Static resources
            else:
                static_requests.append(req)
        
        # Print analysis
        print(f"\n=== ANALYSIS RESULTS FOR {context.upper()} ===")
        print(f"Total Requests: {len(requests)}")
        print(f"API Requests: {len(api_requests)}")
        print(f"Tile Requests: {len(tile_requests)}")
        print(f"Data Requests: {len(data_requests)}")
        print(f"Static Requests: {len(static_requests)}")
        
        # Print interesting URLs
        if api_requests:
            print(f"\n🔥 API ENDPOINTS FOUND:")
            for req in api_requests[:10]:  # Show first 10
                print(f"  - {req['url']}")
                
        if tile_requests:
            print(f"\n🗺️  TILE PATTERNS FOUND:")
            unique_tiles = list(set([req['url'] for req in tile_requests]))
            for url in unique_tiles[:5]:  # Show first 5
                print(f"  - {url}")
                
        if data_requests:
            print(f"\n📊 DATA ENDPOINTS FOUND:")
            for req in data_requests[:5]:  # Show first 5
                print(f"  - {req['url']}")
    
    def test_discovered_endpoints(self):
        """
        Test các endpoints đã phát hiện
        """
        logger.info("=== TESTING DISCOVERED ENDPOINTS ===")
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://guland.vn/',
            'Accept': 'application/json, text/plain, */*'
        })
        
        tested_endpoints = []
        
        for endpoint in list(self.api_endpoints)[:10]:  # Test first 10
            try:
                logger.info(f"Testing endpoint: {endpoint}")
                
                response = session.get(endpoint, timeout=10)
                
                result = {
                    'url': endpoint,
                    'status_code': response.status_code,
                    'content_type': response.headers.get('content-type', ''),
                    'response_size': len(response.content),
                    'success': response.status_code == 200
                }
                
                if response.status_code == 200:
                    # Try to parse as JSON
                    try:
                        json_data = response.json()
                        result['json_structure'] = self.analyze_json_structure(json_data)
                        
                        # Save response
                        filename = endpoint.split('/')[-1] or 'response'
                        with open(f"output/api_responses/{filename}.json", "w") as f:
                            json.dump(json_data, f, indent=2, ensure_ascii=False)
                            
                    except:
                        result['content_preview'] = response.text[:500]
                
                tested_endpoints.append(result)
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error testing endpoint {endpoint}: {e}")
        
        # Save test results
        with open("output/endpoint_test_results.json", "w") as f:
            json.dump(tested_endpoints, f, indent=2, ensure_ascii=False)
            
        return tested_endpoints
    
    def analyze_json_structure(self, data, max_depth=3, current_depth=0):
        """
        Phân tích cấu trúc JSON response
        """
        if current_depth >= max_depth:
            return "..."
            
        if isinstance(data, dict):
            return {key: self.analyze_json_structure(value, max_depth, current_depth + 1) 
                   for key, value in list(data.items())[:5]}  # First 5 keys only
        elif isinstance(data, list):
            if len(data) > 0:
                return [self.analyze_json_structure(data[0], max_depth, current_depth + 1)]
            else:
                return []
        else:
            return type(data).__name__
    
    def generate_report(self):
        """
        Tạo báo cáo tổng hợp
        """
        logger.info("=== GENERATING ANALYSIS REPORT ===")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_api_endpoints': len(self.api_endpoints),
                'total_tile_patterns': len(self.tile_patterns),
                'total_data_endpoints': len(self.data_endpoints)
            },
            'api_endpoints': list(self.api_endpoints),
            'tile_patterns': self.tile_patterns,
            'data_endpoints': self.data_endpoints,
            'recommendations': self.generate_recommendations()
        }
        
        with open("output/analysis_report.json", "w") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
            
        # Generate human-readable report
        self.generate_human_report(report)
        
        return report
    
    def generate_recommendations(self):
        """
        Tạo khuyến nghị dựa trên phân tích
        """
        recommendations = []
        
        if self.api_endpoints:
            recommendations.append({
                "type": "API_ENDPOINTS_FOUND",
                "priority": "HIGH",
                "description": "Found API endpoints - recommend direct API scraping",
                "action": "Test authentication and rate limits for API endpoints"
            })
        
        if self.tile_patterns:
            recommendations.append({
                "type": "TILE_PATTERNS_FOUND", 
                "priority": "HIGH",
                "description": "Found tile server patterns - recommend tile scraping",
                "action": "Implement tile downloading with proper zoom levels"
            })
            
        if len(self.data_endpoints) > 0:
            recommendations.append({
                "type": "DATA_ENDPOINTS_FOUND",
                "priority": "MEDIUM", 
                "description": "Found data endpoints - potential for bulk data access",
                "action": "Analyze data formats and implement parsers"
            })
        
        return recommendations
    
    def generate_human_report(self, report):
        """
        Tạo báo cáo dễ đọc cho con người
        """
        report_text = f"""
# GULAND.VN NETWORK ANALYSIS REPORT
Generated: {report['timestamp']}

## SUMMARY
- API Endpoints Found: {report['summary']['total_api_endpoints']}
- Tile Patterns Found: {report['summary']['total_tile_patterns']}  
- Data Endpoints Found: {report['summary']['total_data_endpoints']}

## API ENDPOINTS
"""
        
        for endpoint in report['api_endpoints'][:10]:
            report_text += f"- {endpoint}\n"
            
        report_text += f"""
## TILE PATTERNS
"""
        
        for pattern in report['tile_patterns'][:5]:
            report_text += f"- {pattern}\n"
            
        report_text += f"""
## RECOMMENDATIONS
"""
        
        for rec in report['recommendations']:
            report_text += f"- [{rec['priority']}] {rec['description']}\n"
            report_text += f"  Action: {rec['action']}\n\n"
            
        with open("output/analysis_report.txt", "w", encoding="utf-8") as f:
            f.write(report_text)
    
    def run_full_analysis(self):
        """
        Chạy phân tích hoàn chỉnh
        """
        logger.info("🚀 STARTING FULL GULAND NETWORK ANALYSIS")
        
        try:
            # Setup
            self.setup_driver()
            
            # Step 1: Homepage analysis
            self.analyze_homepage()
            
            # Step 2: Map interface analysis  
            self.analyze_map_interface()
            
            # Step 3: Provincial pages analysis
            self.analyze_provincial_pages()
            
            # Step 4: Test discovered endpoints
            self.test_discovered_endpoints()
            
            # Step 5: Generate report
            report = self.generate_report()
            
            logger.info("✅ ANALYSIS COMPLETED SUCCESSFULLY")
            print(f"\n🎉 Analysis complete! Check the 'output' folder for results.")
            print(f"📊 Found {len(self.api_endpoints)} API endpoints")
            print(f"🗺️  Found {len(self.tile_patterns)} tile patterns")
            
            return report
            
        except Exception as e:
            logger.error(f"❌ Analysis failed: {e}")
            raise
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Browser closed")

def main():
    """
    Main function để chạy analysis
    """
    print("🔍 GULAND.VN NETWORK ANALYZER")
    print("=" * 50)
    
    # Tạo analyzer instance
    analyzer = GulandNetworkAnalyzer(headless=False)  # Set True để chạy headless
    
    try:
        # Chạy phân tích đầy đủ
        report = analyzer.run_full_analysis()
        
        print("\n" + "=" * 50)
        print("✅ ANALYSIS COMPLETED!")
        print("📁 Check 'output' folder for:")
        print("  - analysis_report.json (machine readable)")
        print("  - analysis_report.txt (human readable)")  
        print("  - network_logs/ (detailed request logs)")
        print("  - screenshots/ (page screenshots)")
        print("  - api_responses/ (API response samples)")
        
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        
if __name__ == "__main__":
    main()