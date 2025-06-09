#!/usr/bin/env python3
"""
Quick Test Script for Guland Network Analysis
Chạy test nhanh để kiểm tra setup và phát hiện endpoints cơ bản

Usage: python quick_test.py
"""

import os
import sys
import time
import json
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QuickTester:
    def __init__(self):
        self.driver = None
        self.findings = {
            'requests': [],
            'potential_apis': [],
            'tile_patterns': [],
            'javascript_data': []
        }
    
    def setup_browser(self):
        """Setup browser với network monitoring"""
        logger.info("🔧 Setting up browser...")
        
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Chạy headless cho test nhanh
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_cdp_cmd('Network.enable', {})
            logger.info("✅ Browser setup successful")
            return True
        except Exception as e:
            logger.error(f"❌ Browser setup failed: {e}")
            return False
    
    def capture_network_traffic(self, url, duration=10):
        """Capture network traffic trong thời gian ngắn"""
        logger.info(f"🔍 Analyzing: {url}")
        
        captured_requests = []
        
        def capture_request(message):
            if message['method'] == 'Network.responseReceived':
                response = message['params']['response']
                captured_requests.append({
                    'url': response['url'],
                    'status': response['status'],
                    'mimeType': response.get('mimeType', ''),
                    'method': response.get('method', 'GET')
                })
        
        self.driver.add_cdp_listener('Network.responseReceived', capture_request)
        
        try:
            self.driver.get(url)
            time.sleep(duration)
            
            # Quick interaction
            try:
                body = self.driver.find_element(By.TAG_NAME, "body")
                self.driver.execute_script("window.scrollTo(0, 500);")
                time.sleep(2)
            except:
                pass
                
        except Exception as e:
            logger.error(f"Error loading {url}: {e}")
        
        self.findings['requests'].extend(captured_requests)
        return captured_requests
    
    def analyze_page_source(self):
        """Phân tích source code để tìm API endpoints"""
        logger.info("🔍 Analyzing page source for APIs...")
        
        try:
            source = self.driver.page_source
            
            # Tìm các patterns API thường gặp
            import re
            
            # API URL patterns
            api_patterns = [
                r'["\']https?://[^"\']*api[^"\']*["\']',
                r'["\']https?://[^"\']*ajax[^"\']*["\']',
                r'["\'].*\.json["\']',
                r'["\'].*tiles/\d+/\d+/\d+["\']'
            ]
            
            for pattern in api_patterns:
                matches = re.findall(pattern, source, re.IGNORECASE)
                for match in matches:
                    clean_url = match.strip('"\'')
                    if clean_url not in self.findings['potential_apis']:
                        self.findings['potential_apis'].append(clean_url)
            
            # Tìm JavaScript configuration objects
            js_patterns = [
                r'config\s*[:=]\s*{[^}]*}',
                r'API_URL\s*[:=]\s*["\'][^"\']*["\']',
                r'mapConfig\s*[:=]\s*{[^}]*}'
            ]
            
            for pattern in js_patterns:
                matches = re.findall(pattern, source, re.IGNORECASE)
                self.findings['javascript_data'].extend(matches)
                
        except Exception as e:
            logger.error(f"Error analyzing source: {e}")
    
    def categorize_requests(self, requests):
        """Phân loại requests"""
        apis = []
        tiles = []
        data = []
        
        for req in requests:
            url = req['url'].lower()
            
            if any(pattern in url for pattern in ['api', 'ajax', '.json']):
                apis.append(req)
            elif any(pattern in url for pattern in ['tile', '.png', '.jpg', '/map/']):
                tiles.append(req)
            elif any(pattern in url for pattern in ['data', 'geojson', 'wms', 'wfs']):
                data.append(req)
        
        return apis, tiles, data
    
    def test_basic_endpoints(self):
        """Test một số endpoints cơ bản"""
        logger.info("🧪 Testing basic endpoints...")
        
        # Thử một số endpoints thường gặp
        common_endpoints = [
            "https://guland.vn/api/",
            "https://guland.vn/api/maps/",
            "https://guland.vn/api/tiles/",
            "https://guland.vn/ajax/",
            "https://api.guland.vn/",
        ]
        
        working_endpoints = []
        
        for endpoint in common_endpoints:
            try:
                response = requests.get(endpoint, timeout=5)
                if response.status_code != 404:
                    working_endpoints.append({
                        'url': endpoint,
                        'status': response.status_code,
                        'content_type': response.headers.get('content-type', '')
                    })
                    logger.info(f"✅ Found endpoint: {endpoint} (Status: {response.status_code})")
            except:
                pass
        
        return working_endpoints
    
    def run_quick_test(self):
        """Chạy test nhanh"""
        logger.info("🚀 Starting Quick Network Analysis of Guland.vn")
        logger.info("=" * 60)
        
        if not self.setup_browser():
            return False
        
        try:
            # Test 1: Homepage
            logger.info("📍 Step 1: Analyzing Homepage")
            homepage_requests = self.capture_network_traffic("https://guland.vn/", 8)
            
            # Test 2: Map page  
            logger.info("📍 Step 2: Analyzing Map Interface")
            map_requests = self.capture_network_traffic("https://guland.vn/map", 12)
            
            # Test 3: Analyze source
            logger.info("📍 Step 3: Analyzing Page Source")
            self.analyze_page_source()
            
            # Test 4: Test common endpoints
            logger.info("📍 Step 4: Testing Common API Endpoints")
            working_endpoints = self.test_basic_endpoints()
            
            # Analyze results
            all_requests = homepage_requests + map_requests
            apis, tiles, data = self.categorize_requests(all_requests)
            
            # Generate quick report
            self.generate_quick_report(apis, tiles, data, working_endpoints)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Test failed: {e}")
            return False
            
        finally:
            if self.driver:
                self.driver.quit()
    
    def generate_quick_report(self, apis, tiles, data, working_endpoints):
        """Tạo báo cáo nhanh"""
        
        print("\n" + "=" * 60)
        print("🎯 QUICK ANALYSIS RESULTS")
        print("=" * 60)
        
        print(f"\n📊 SUMMARY:")
        print(f"  • Total Requests Captured: {len(self.findings['requests'])}")
        print(f"  • Potential API Requests: {len(apis)}")
        print(f"  • Map Tile Requests: {len(tiles)}")
        print(f"  • Data Requests: {len(data)}")
        print(f"  • Working Endpoints Found: {len(working_endpoints)}")
        
        if apis:
            print(f"\n🔥 API REQUESTS FOUND:")
            for api in apis[:5]:  # Show first 5
                print(f"  • {api['url']}")
        
        if tiles:
            print(f"\n🗺️ TILE REQUESTS FOUND:")
            unique_tiles = list(set([t['url'] for t in tiles]))
            for tile in unique_tiles[:3]:  # Show first 3
                print(f"  • {tile}")
        
        if self.findings['potential_apis']:
            print(f"\n🔍 POTENTIAL APIS IN SOURCE:")
            for api in self.findings['potential_apis'][:5]:
                print(f"  • {api}")
        
        if working_endpoints:
            print(f"\n✅ WORKING ENDPOINTS:")
            for endpoint in working_endpoints:
                print(f"  • {endpoint['url']} (Status: {endpoint['status']})")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        
        if apis or self.findings['potential_apis']:
            print("  ✅ APIs detected - Proceed with API-based crawling")
            print("     Next: Run full network analysis to map all endpoints")
        
        if tiles:
            print("  ✅ Map tiles detected - Tile server crawling possible")
            print("     Next: Analyze tile URL patterns for systematic download")
        
        if not apis and not tiles:
            print("  ⚠️  No obvious APIs/tiles found - May need Selenium scraping")
            print("     Next: Try mobile app reverse engineering")
        
        print(f"\n🎯 NEXT STEPS:")
        print("  1. Run full analysis: python network_analyzer.py")
        print("  2. Check mobile app for API endpoints")
        print("  3. Implement crawling based on findings")
        
        # Save results
        results = {
            'timestamp': time.time(),
            'summary': {
                'total_requests': len(self.findings['requests']),
                'api_requests': len(apis),
                'tile_requests': len(tiles),
                'working_endpoints': len(working_endpoints)
            },
            'api_requests': [api['url'] for api in apis],
            'tile_requests': [tile['url'] for tile in tiles],
            'potential_apis': self.findings['potential_apis'],
            'working_endpoints': working_endpoints
        }
        
        with open('quick_test_results.json', 'w') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Results saved to: quick_test_results.json")

def main():
    """Main function"""
    print("⚡ GULAND.VN QUICK NETWORK TEST")
    print("This will take ~2-3 minutes...")
    print("-" * 40)
    
    tester = QuickTester()
    
    try:
        success = tester.run_quick_test()
        
        if success:
            print("\n🎉 Quick test completed successfully!")
            print("📁 Check 'quick_test_results.json' for detailed findings")
        else:
            print("\n❌ Quick test failed. Check your setup:")
            print("  • Chrome browser installed?")
            print("  • ChromeDriver in PATH?")
            print("  • Internet connection working?")
            
    except KeyboardInterrupt:
        print("\n⏹️ Test interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")

if __name__ == "__main__":
    main()