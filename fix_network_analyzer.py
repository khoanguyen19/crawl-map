#!/usr/bin/env python3
"""
Fixed Guland Network Analyzer
Multiple methods để capture network traffic khi CDP không work

Author: AI Assistant
Version: 2.0 (Fixed)
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
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from bs4 import BeautifulSoup
import logging
from datetime import datetime
import os
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('guland_analysis_fixed.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GulandNetworkAnalyzerFixed:
    def __init__(self, headless=False):
        """
        Initialize fixed network analyzer với multiple fallback methods
        """
        self.headless = headless
        self.driver = None
        self.captured_requests = []
        self.api_endpoints = set()
        self.tile_patterns = []
        self.data_endpoints = []
        self.performance_logs = []
        
        # Create output directories
        os.makedirs('output/network_logs', exist_ok=True)
        os.makedirs('output/screenshots', exist_ok=True)
        os.makedirs('output/api_responses', exist_ok=True)
        os.makedirs('output/page_sources', exist_ok=True)
        
    def setup_driver_with_logging(self):
        """
        Setup Chrome driver với performance logging (alternative to CDP)
        """
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
            
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Enable performance logging
        chrome_options.add_argument("--enable-logging")
        chrome_options.add_argument("--log-level=0")
        chrome_options.add_argument("--v=1")
        
        # User agent
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        
        # Enable performance logs
        caps = DesiredCapabilities.CHROME
        caps['goog:loggingPrefs'] = {'performance': 'ALL'}
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options, desired_capabilities=caps)
            logger.info("Chrome driver initialized with performance logging")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Chrome with performance logging: {e}")
            
            # Fallback: basic Chrome driver
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
                logger.info("Chrome driver initialized (basic mode)")
                return True
            except Exception as e2:
                logger.error(f"Failed to initialize Chrome driver: {e2}")
                return False
        
    def capture_performance_logs(self):
        """
        Capture network requests từ performance logs
        """
        try:
            logs = self.driver.get_log('performance')
            network_requests = []
            
            for log in logs:
                message = json.loads(log['message'])
                
                # Filter network events
                if message.get('message', {}).get('method') in [
                    'Network.requestWillBeSent',
                    'Network.responseReceived'
                ]:
                    network_requests.append({
                        'timestamp': log['timestamp'],
                        'level': log['level'],
                        'message': message
                    })
            
            return network_requests
            
        except Exception as e:
            logger.warning(f"Could not capture performance logs: {e}")
            return []
    
    def analyze_page_source(self, url):
        """
        Phân tích page source để tìm API endpoints và patterns
        """
        logger.info(f"🔍 Analyzing page source for: {url}")
        
        try:
            page_source = self.driver.page_source
            
            # Save page source
            filename = urllib.parse.quote(url, safe='').replace('/', '_')
            with open(f'output/page_sources/{filename}.html', 'w', encoding='utf-8') as f:
                f.write(page_source)
            
            # Parse với BeautifulSoup
            soup = BeautifulSoup(page_source, 'html.parser')
            
            analysis_results = {
                'url': url,
                'title': soup.title.string if soup.title else 'No title',
                'scripts': [],
                'api_references': [],
                'tile_patterns': [],
                'data_attributes': {},
                'forms': [],
                'external_resources': []
            }
            
            # Analyze scripts
            for script in soup.find_all('script'):
                if script.get('src'):
                    analysis_results['scripts'].append(script['src'])
                elif script.string:
                    # Look for API patterns trong inline scripts
                    apis = self.extract_apis_from_js(script.string)
                    analysis_results['api_references'].extend(apis)
            
            # Look for data attributes
            for elem in soup.find_all(attrs=lambda x: x and any(attr.startswith('data-') for attr in x.keys())):
                for attr, value in elem.attrs.items():
                    if attr.startswith('data-'):
                        analysis_results['data_attributes'][attr] = value
            
            # Analyze forms
            for form in soup.find_all('form'):
                form_info = {
                    'action': form.get('action', ''),
                    'method': form.get('method', 'GET'),
                    'inputs': []
                }
                
                for inp in form.find_all('input'):
                    form_info['inputs'].append({
                        'name': inp.get('name'),
                        'type': inp.get('type'),
                        'value': inp.get('value')
                    })
                
                analysis_results['forms'].append(form_info)
            
            # Look for images that might be tiles
            for img in soup.find_all('img', src=True):
                src = img['src']
                if any(pattern in src.lower() for pattern in ['tile', 'map', '/z/', '/x/', '/y/']):
                    analysis_results['tile_patterns'].append(src)
                    self.tile_patterns.append(src)
            
            # Look for external resources
            for link in soup.find_all('link', href=True):
                href = link['href']
                if href.startswith('http') and 'guland.vn' not in href:
                    analysis_results['external_resources'].append(href)
            
            return analysis_results
            
        except Exception as e:
            logger.error(f"Error analyzing page source: {e}")
            return {'url': url, 'error': str(e)}
    
    def extract_apis_from_js(self, js_content):
        """
        Extract API endpoints từ JavaScript content
        """
        api_patterns = [
            r'["\']https?://[^"\']*api[^"\']*["\']',
            r'["\']https?://[^"\']*ajax[^"\']*["\']',
            r'["\'][^"\']*\.json["\']',
            r'api[Uu]rl\s*[:=]\s*["\'][^"\']+["\']',
            r'baseUrl\s*[:=]\s*["\'][^"\']+["\']',
            r'endpoint\s*[:=]\s*["\'][^"\']+["\']',
            r'["\']/?api/[^"\']*["\']',
            r'["\']/?tiles/[^"\']*["\']',
            r'fetch\(["\'][^"\']+["\']',
            r'XMLHttpRequest.*["\'][^"\']+["\']'
        ]
        
        found_apis = []
        
        for pattern in api_patterns:
            matches = re.findall(pattern, js_content, re.IGNORECASE)
            for match in matches:
                clean_url = match.strip('"\'')
                if clean_url and len(clean_url) > 5:  # Filter out very short matches
                    found_apis.append(clean_url)
                    self.api_endpoints.add(clean_url)
        
        return found_apis
    
    def test_discovered_endpoints_advanced(self, base_url="https://guland.vn"):
        """
        Test các endpoints đã phát hiện + common endpoints
        """
        logger.info("🧪 Testing discovered and common endpoints...")
        
        # Combine discovered endpoints với common paths
        test_endpoints = list(self.api_endpoints)
        
        # Add common API paths
        common_paths = [
            '/api/',
            '/api/v1/',
            '/api/maps/',
            '/api/tiles/',
            '/api/planning/',
            '/api/properties/',
            '/api/search/',
            '/api/data/',
            '/ajax/',
            '/json/',
            '/data/',
            '/services/',
            '/rest/',
            '/map/api/',
            '/tiles/',
            '/wms',
            '/wfs',
            '/geojson'
        ]
        
        for path in common_paths:
            test_endpoints.append(urllib.parse.urljoin(base_url, path))
        
        # Remove duplicates
        test_endpoints = list(set(test_endpoints))
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': base_url,
            'Accept': 'application/json, text/plain, */*'
        })
        
        working_endpoints = []
        
        for endpoint in test_endpoints[:20]:  # Test first 20
            try:
                logger.info(f"Testing: {endpoint}")
                
                # Try both HEAD and GET
                for method in ['HEAD', 'GET']:
                    try:
                        if method == 'HEAD':
                            response = session.head(endpoint, timeout=5)
                        else:
                            response = session.get(endpoint, timeout=10)
                        
                        if response.status_code < 400:
                            result = {
                                'url': endpoint,
                                'method': method,
                                'status_code': response.status_code,
                                'content_type': response.headers.get('content-type', ''),
                                'content_length': response.headers.get('content-length', 0),
                                'response_headers': dict(response.headers)
                            }
                            
                            # Try to get response content for GET requests
                            if method == 'GET' and response.status_code == 200:
                                try:
                                    if 'json' in response.headers.get('content-type', ''):
                                        result['json_data'] = response.json()
                                    else:
                                        result['content_preview'] = response.text[:500]
                                except:
                                    result['content_preview'] = 'Could not parse content'
                            
                            working_endpoints.append(result)
                            logger.info(f"✅ {method} {endpoint} - Status: {response.status_code}")
                            break  # Success, no need to try other method
                            
                    except requests.exceptions.Timeout:
                        continue
                    except requests.exceptions.RequestException:
                        continue
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Error testing {endpoint}: {e}")
                continue
        
        # Save results
        with open('output/api_responses/endpoint_tests.json', 'w') as f:
            json.dump(working_endpoints, f, indent=2, ensure_ascii=False)
        
        return working_endpoints
    
    def analyze_page_with_interaction(self, url, context_name):
        """
        Analyze page với interaction để trigger more requests
        """
        logger.info(f"🔍 Analyzing {context_name}: {url}")
        
        try:
            # Navigate to page
            self.driver.get(url)
            
            # Wait for page load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Take screenshot
            self.driver.save_screenshot(f"output/screenshots/{context_name}.png")
            
            # Capture initial performance logs
            initial_logs = self.capture_performance_logs()
            
            # Analyze page source
            source_analysis = self.analyze_page_source(url)
            
            # Interact with page
            self.interact_with_page()
            
            # Wait a bit for any async requests
            time.sleep(5)
            
            # Capture logs after interaction
            final_logs = self.capture_performance_logs()
            
            # Combine results
            all_logs = initial_logs + final_logs
            
            # Save logs
            with open(f"output/network_logs/{context_name}_requests.json", "w") as f:
                json.dump({
                    'url': url,
                    'context': context_name,
                    'source_analysis': source_analysis,
                    'performance_logs': all_logs,
                    'timestamp': datetime.now().isoformat()
                }, f, indent=2, ensure_ascii=False)
            
            # Extract network requests from logs
            network_requests = self.extract_network_requests_from_logs(all_logs)
            
            return network_requests
            
        except Exception as e:
            logger.error(f"Error analyzing {context_name}: {e}")
            return []
    
    def interact_with_page(self):
        """
        Interact with page để trigger network requests
        """
        try:
            # Scroll down
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)
            
            # Scroll back up
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            
            # Try to find and interact with common elements
            interactive_selectors = [
                'button',
                'input[type="search"]',
                'input[type="text"]',
                '.search-box',
                '.map-container',
                '.btn',
                '[onclick]',
                '[data-toggle]'
            ]
            
            for selector in interactive_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements[:2]:  # Interact with first 2 elements
                        try:
                            if elem.is_displayed() and elem.is_enabled():
                                elem.click()
                                time.sleep(1)
                        except:
                            continue
                except:
                    continue
            
            # Try keyboard interactions
            try:
                self.driver.find_element(By.TAG_NAME, "body").send_keys(" ")  # Space key
                time.sleep(1)
            except:
                pass
                
        except Exception as e:
            logger.warning(f"Error during page interaction: {e}")
    
    def extract_network_requests_from_logs(self, logs):
        """
        Extract network requests từ performance logs
        """
        requests = []
        
        for log in logs:
            try:
                message = log.get('message', {})
                method = message.get('method', '')
                params = message.get('params', {})
                
                if method == 'Network.requestWillBeSent':
                    request_info = params.get('request', {})
                    requests.append({
                        'type': 'request',
                        'url': request_info.get('url', ''),
                        'method': request_info.get('method', ''),
                        'headers': request_info.get('headers', {}),
                        'timestamp': log.get('timestamp')
                    })
                    
                elif method == 'Network.responseReceived':
                    response_info = params.get('response', {})
                    requests.append({
                        'type': 'response',
                        'url': response_info.get('url', ''),
                        'status': response_info.get('status', 0),
                        'mimeType': response_info.get('mimeType', ''),
                        'headers': response_info.get('headers', {}),
                        'timestamp': log.get('timestamp')
                    })
                    
            except Exception as e:
                continue
        
        return requests
    
    def generate_comprehensive_report(self):
        """
        Generate comprehensive analysis report
        """
        logger.info("📊 Generating comprehensive report...")
        
        # Collect all findings
        all_api_endpoints = list(self.api_endpoints)
        all_tile_patterns = list(set(self.tile_patterns))
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'analysis_method': 'Fixed Network Analyzer v2.0',
            'summary': {
                'total_api_endpoints': len(all_api_endpoints),
                'total_tile_patterns': len(all_tile_patterns),
                'pages_analyzed': ['homepage', 'map', 'provincial_pages'],
                'methods_used': ['page_source_analysis', 'performance_logs', 'endpoint_testing']
            },
            'findings': {
                'api_endpoints': all_api_endpoints,
                'tile_patterns': all_tile_patterns,
                'analysis_details': 'Check individual log files for detailed findings'
            },
            'recommendations': self.generate_recommendations()
        }
        
        # Save comprehensive report
        with open("output/comprehensive_analysis_report.json", "w") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate human-readable report
        self.generate_human_readable_report(report)
        
        return report
    
    def generate_recommendations(self):
        """
        Generate actionable recommendations
        """
        recommendations = []
        
        if len(self.api_endpoints) > 0:
            recommendations.append({
                "priority": "HIGH",
                "type": "API_ENDPOINTS_FOUND",
                "description": f"Found {len(self.api_endpoints)} potential API endpoints",
                "action": "Test these endpoints for data access and implement API-based crawling"
            })
        
        if len(self.tile_patterns) > 0:
            recommendations.append({
                "priority": "HIGH", 
                "type": "TILE_PATTERNS_FOUND",
                "description": f"Found {len(self.tile_patterns)} tile patterns",
                "action": "Implement systematic tile downloading based on these patterns"
            })
        
        if len(self.api_endpoints) == 0 and len(self.tile_patterns) == 0:
            recommendations.append({
                "priority": "MEDIUM",
                "type": "NO_OBVIOUS_APIS",
                "description": "No obvious API endpoints found in initial analysis",
                "action": "Try mobile app reverse engineering or deeper JavaScript analysis"
            })
        
        recommendations.append({
            "priority": "MEDIUM",
            "type": "NEXT_STEPS",
            "description": "Continue with deeper analysis",
            "action": "Use requests-based analyzer and mobile app analysis"
        })
        
        return recommendations
    
    def generate_human_readable_report(self, report):
        """
        Generate human-readable text report
        """
        text_report = f"""
# GULAND.VN COMPREHENSIVE NETWORK ANALYSIS
Generated: {report['timestamp']}
Method: {report['analysis_method']}

## 📊 SUMMARY
- API Endpoints Found: {report['summary']['total_api_endpoints']}
- Tile Patterns Found: {report['summary']['total_tile_patterns']}
- Pages Analyzed: {', '.join(report['summary']['pages_analyzed'])}
- Methods Used: {', '.join(report['summary']['methods_used'])}

## 🔥 API ENDPOINTS DISCOVERED
"""
        
        for endpoint in report['findings']['api_endpoints']:
            text_report += f"- {endpoint}\n"
        
        text_report += "\n## 🗺️ TILE PATTERNS FOUND\n"
        for pattern in report['findings']['tile_patterns']:
            text_report += f"- {pattern}\n"
        
        text_report += "\n## 💡 RECOMMENDATIONS\n"
        for rec in report['recommendations']:
            text_report += f"[{rec['priority']}] {rec['description']}\n"
            text_report += f"Action: {rec['action']}\n\n"
        
        text_report += "\n## 📁 OUTPUT FILES\n"
        text_report += "- output/network_logs/ - Detailed request logs\n"
        text_report += "- output/screenshots/ - Page screenshots\n"
        text_report += "- output/page_sources/ - HTML sources\n"
        text_report += "- output/api_responses/ - API test results\n"
        
        with open("output/comprehensive_analysis_report.txt", "w", encoding="utf-8") as f:
            f.write(text_report)
    
    def run_full_analysis(self):
        """
        Run complete analysis với multiple methods
        """
        logger.info("🚀 STARTING COMPREHENSIVE GULAND ANALYSIS (FIXED)")
        
        try:
            # Setup driver
            if not self.setup_driver_with_logging():
                logger.error("❌ Could not setup Chrome driver")
                return None
            
            # Step 1: Analyze homepage
            self.analyze_page_with_interaction("https://guland.vn/", "homepage")
            
            # Step 2: Analyze map interface
            self.analyze_page_with_interaction("https://guland.vn/map", "map_interface")
            
            # Step 3: Analyze provincial pages
            provinces = ["ha-noi", "ho-chi-minh", "da-nang"]
            for province in provinces:
                province_url = f"https://guland.vn/soi-quy-hoach/{province}"
                self.analyze_page_with_interaction(province_url, f"province_{province}")
            
            # Step 4: Test discovered endpoints
            working_endpoints = self.test_discovered_endpoints_advanced()
            
            # Step 5: Generate comprehensive report
            report = self.generate_comprehensive_report()
            
            logger.info("✅ COMPREHENSIVE ANALYSIS COMPLETED")
            
            # Print results
            print(f"\n🎉 Analysis Complete!")
            print(f"📊 Found {len(self.api_endpoints)} API endpoints")
            print(f"🗺️ Found {len(self.tile_patterns)} tile patterns")
            print(f"🧪 Tested {len(working_endpoints)} working endpoints")
            print(f"\n📁 Check 'output' folder for detailed results:")
            print("  • comprehensive_analysis_report.txt (summary)")
            print("  • network_logs/ (detailed logs)")
            print("  • screenshots/ (page captures)")
            print("  • page_sources/ (HTML sources)")
            
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
    Main function
    """
    print("🔧 GULAND.VN FIXED NETWORK ANALYZER")
    print("(Handles CDP issues + Multiple Methods)")
    print("=" * 60)
    
    try:
        # Create analyzer instance
        analyzer = GulandNetworkAnalyzerFixed(headless=False)
        
        # Run full analysis
        report = analyzer.run_full_analysis()
        
        if report:
            print("\n" + "=" * 60)
            print("✅ SUCCESS! Analysis completed with multiple methods.")
            print("💡 Even if some methods failed, we captured what we could.")
            print("📋 Check the comprehensive report for next steps.")
        
    except KeyboardInterrupt:
        print("\n⏹️ Analysis interrupted by user")
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        print("💡 Try the requests-based analyzer as fallback")

if __name__ == "__main__":
    main()