#!/usr/bin/env python3
"""
Browser-Based Guland Crawler
Uses real browser to discover tile URL patterns for cities

Author: AI Assistant
Version: 4.0 (Pattern Discovery Only)
"""
import time
import json
import os
import random
import logging
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from map_interaction_handler import MapInteractionHandler

# Setup logging with city-based structure
def setup_city_logging(city_name):
    """Setup logging for specific city"""
    city_log_dir = f'output_browser_crawl/cities/{city_name}/logs'
    os.makedirs(city_log_dir, exist_ok=True)
    
    # Create city-specific logger
    city_logger = logging.getLogger(f'browser_crawler_{city_name}')
    city_logger.setLevel(logging.INFO)
    
    # Remove existing handlers
    for handler in city_logger.handlers[:]:
        city_logger.removeHandler(handler)
    
    # Add city-specific file handler
    city_handler = logging.FileHandler(f'{city_log_dir}/crawl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    city_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    city_logger.addHandler(city_handler)
    
    return city_logger

# Main logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('output_browser_crawl/browser_pattern_discovery.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BrowserGulandCrawler:
    def __init__(self, headless=False):
        self.driver = None
        self.headless = headless
        self.map_handler = None
        self.discovered_data = {
            'all_locations': [],
            'tile_servers': set(),
            'tile_patterns': set(),
            'success_count': 0,
            'failure_count': 0
        }
        
        # Create organized output directories
        self.setup_output_structure()
        
        self.test_locations = [
            # ("TP Hồ Chí Minh", 10.8231, 106.6297, "soi-quy-hoach/tp-ho-chi-minh"),
            ("Đồng Nai", 11.0686, 107.1676, "soi-quy-hoach/dong-nai"),
            ("Bà Rịa - Vũng Tàu", 10.5417, 107.2431, "soi-quy-hoach/ba-ria-vung-tau"),
            ("An Giang", 10.3889, 105.4359, "soi-quy-hoach/an-giang"),
            # ("Bắc Giang", 21.2731, 106.1946, "soi-quy-hoach/bac-giang"),
            # ("Bắc Kạn", 22.1474, 105.8348, "soi-quy-hoach/bac-kan"),
            # ("Bạc Liêu", 9.2515, 105.7244, "soi-quy-hoach/bac-lieu"),
            # ("Bắc Ninh", 21.1861, 106.0763, "soi-quy-hoach/bac-ninh"),
            # ("Bến Tre", 10.2433, 106.3756, "soi-quy-hoach/ben-tre"),
            # ("Bình Dương", 11.3254, 106.4770, "soi-quy-hoach/binh-duong"),
            # ("Bình Phước", 11.7511, 106.7234, "soi-quy-hoach/binh-phuoc"),
            # ("Bình Thuận", 11.0904, 108.0721, "soi-quy-hoach/binh-thuan"),
            # ("Bình Định", 13.7757, 109.2219, "soi-quy-hoach/binh-dinh"),
            # ("Cà Mau", 9.1769, 105.1524, "soi-quy-hoach/ca-mau")
            # ("Cần Thơ", 10.0452, 105.7469, "soi-quy-hoach/can-tho"),
            # ("Cao Bằng", 22.6666, 106.2639, "soi-quy-hoach/cao-bang"),
            # ("Gia Lai", 13.8078, 108.1094, "soi-quy-hoach/gia-lai"),
            # ("Hà Nam", 20.5835, 105.9230, "soi-quy-hoach/ha-nam"),
            # ("Hà Giang", 22.8025, 104.9784, "soi-quy-hoach/ha-giang"),
            # ("Hà Nội", 21.0285, 105.8542, "soi-quy-hoach/ha-noi"),
            # ("Hà Tĩnh", 18.3560, 105.9069, "soi-quy-hoach/ha-tinh"),
            # ("Hải Phòng", 20.8449, 106.6881, "soi-quy-hoach/hai-phong"),
            # ("Hậu Giang", 9.7571, 105.6412, "soi-quy-hoach/hau-giang"),
            # ("Hòa Bình", 20.8156, 105.3373, "soi-quy-hoach/hoa-binh"),
            # ("Hưng Yên", 20.6464, 106.0511, "soi-quy-hoach/hung-yen"),
            # ("Khánh Hòa", 12.2388, 109.1967, "soi-quy-hoach/khanh-hoa"),
            # ("Kiên Giang", 10.0125, 105.0808, "soi-quy-hoach/kien-giang"),
            # ("Kon Tum", 14.3497, 108.0005, "soi-quy-hoach/kon-tum"),
            # ("Lai Châu", 22.3856, 103.4707, "soi-quy-hoach/lai-chau"),
            # ("Lâm Đồng", 11.5753, 108.1429, "soi-quy-hoach/lam-dong"),
            # ("Lạng Sơn", 21.8537, 106.7610, "soi-quy-hoach/lang-son"),
            # ("Lào Cai", 22.4809, 103.9755, "soi-quy-hoach/lao-cai"),
            # ("Long An", 10.6957, 106.2431, "soi-quy-hoach/long-an"),
            # ("Nam Định", 20.4341, 106.1675, "soi-quy-hoach/nam-dinh"),
            # ("Nghệ An", 18.6745, 105.6905, "soi-quy-hoach/nghe-an"),
            # ("Ninh Bình", 20.2506, 105.9744, "soi-quy-hoach/ninh-binh"),
            # ("Ninh Thuận", 11.5645, 108.9899, "soi-quy-hoach/ninh-thuan"),
            # ("Phú Thọ", 21.4208, 105.2045, "soi-quy-hoach/phu-tho"),
            # ("Phú Yên", 13.0882, 109.0929, "soi-quy-hoach/phu-yen"),
            # ("Quảng Bình", 17.4809, 106.6238, "soi-quy-hoach/quang-binh"),
            # ("Quảng Nam", 15.5394, 108.0191, "soi-quy-hoach/quang-nam"),
            # ("Quảng Ngãi", 15.1214, 108.8044, "soi-quy-hoach/quang-ngai"),
            # ("Quảng Ninh", 21.0064, 107.2925, "soi-quy-hoach/quang-ninh"),
            # ("Quảng Trị", 16.7404, 107.1854, "soi-quy-hoach/quang-tri"),
            # ("Sóc Trăng", 9.6002, 105.9800, "soi-quy-hoach/soc-trang"),
            # ("Sơn La", 21.3256, 103.9188, "soi-quy-hoach/son-la"),
            # ("Tây Ninh", 11.3100, 106.0989, "soi-quy-hoach/tay-ninh"),
            # ("Thái Bình", 20.4500, 106.3400, "soi-quy-hoach/thai-binh"),
            # ("Thái Nguyên", 21.5944, 105.8480, "soi-quy-hoach/thai-nguyen"),
            # ("Thanh Hóa", 19.8069, 105.7851, "soi-quy-hoach/thanh-hoa"),
            # ("Thừa Thiên Huế", 16.4674, 107.5905, "soi-quy-hoach/thua-thien-hue"),
            # ("Tiền Giang", 10.4493, 106.3420, "soi-quy-hoach/tien-giang"),
            # ("Trà Vinh", 9.9477, 106.3524, "soi-quy-hoach/tra-vinh"),
            # ("Tuyên Quang", 21.8267, 105.2280, "soi-quy-hoach/tuyen-quang"),
            # ("Vĩnh Long", 10.2397, 105.9571, "soi-quy-hoach/vinh-long"),
            # ("Vĩnh Phúc", 21.3609, 105.6049, "soi-quy-hoach/vinh-phuc"),
            # ("Yên Bái", 21.7168, 104.8986, "soi-quy-hoach/yen-bai"),
            # ("Đà Nẵng", 16.0544563, 108.0717219, "soi-quy-hoach/da-nang"),
            # ("Đắk Lắk", 12.7100, 108.2378, "soi-quy-hoach/dak-lak"),
            # ("Đắk Nông", 12.2646, 107.6098, "soi-quy-hoach/dak-nong"),
            # ("Điện Biên", 21.3847, 103.0175, "soi-quy-hoach/dien-bien"),
            # ("Đồng Tháp", 10.4938, 105.6881, "soi-quy-hoach/dong-thap")
        ]
        
        # Network request tracking
        self.captured_requests = []

    def setup_output_structure(self):
        """Setup organized directory structure"""
        base_dirs = [
            'output_browser_crawl',
            'output_browser_crawl/cities',
            'output_browser_crawl/patterns',
            'output_browser_crawl/reports',
            'output_browser_crawl/screenshots'
        ]
        
        for dir_path in base_dirs:
            os.makedirs(dir_path, exist_ok=True)
        
        logger.info("📁 Output directory structure created")

    def setup_city_directories(self, city_name):
        """Setup directories for specific city"""
        clean_city_name = city_name.replace(' ', '_').replace('TP ', '')
        city_dirs = [
            f'output_browser_crawl/cities/{clean_city_name}',
            f'output_browser_crawl/cities/{clean_city_name}/logs',
            f'output_browser_crawl/cities/{clean_city_name}/reports',
            f'output_browser_crawl/cities/{clean_city_name}/screenshots',
            f'output_browser_crawl/cities/{clean_city_name}/network_logs'
        ]
        
        for dir_path in city_dirs:
            os.makedirs(dir_path, exist_ok=True)
        
        return clean_city_name

    def setup_driver(self):
        """Setup Chrome driver with network logging enabled"""
        logger.info("🚀 Setting up Chrome driver for pattern discovery...")
        
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
        
        # Enable logging for network requests
        chrome_options.add_argument("--enable-logging")
        chrome_options.add_argument("--log-level=0")
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        
        # Enable performance logging to capture network requests
        chrome_options.set_capability('goog:loggingPrefs', {
            'performance': 'ALL',
            'network': 'ALL'
        })
        
        # Make browser look realistic
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # User agent
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Execute script to remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Initialize map interaction handler
            self.map_handler = MapInteractionHandler(self.driver)
            
            logger.info("✅ Chrome driver setup successful for pattern discovery")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to setup Chrome driver: {e}")
            return False

    def start_network_capture(self):
        """Start capturing network requests"""
        logger.info("📡 Starting network capture for pattern discovery...")
        self.captured_requests = []
        
        # Clear existing performance logs
        self.driver.get_log('performance')

    def get_network_requests(self):
        """Get all network requests from performance log with enhanced filtering"""
        logs = self.driver.get_log('performance')
        requests = []
        
        for log in logs:
            try:
                message = json.loads(log['message'])
                
                # Capture both response received and request sent
                if message['message']['method'] in ['Network.responseReceived', 'Network.requestWillBeSent']:
                    if message['message']['method'] == 'Network.responseReceived':
                        response = message['message']['params']['response']
                        url = response['url']
                        status = response.get('status', 0)
                    elif message['message']['method'] == 'Network.requestWillBeSent':
                        request = message['message']['params']['request']
                        url = request['url']
                        status = 'pending'
                    
                    requests.append({
                        'url': url,
                        'timestamp': log['timestamp'],
                        'status': status,
                        'method': message['message']['method']
                    })
            except (KeyError, json.JSONDecodeError) as e:
                # Skip malformed log entries
                continue
        
        # Remove duplicates and sort by timestamp
        unique_urls = set()
        unique_requests = []
        
        for request in sorted(requests, key=lambda x: x['timestamp']):
            if request['url'] not in unique_urls:
                unique_urls.add(request['url'])
                unique_requests.append(request)
        
        logger.info(f"📡 Total unique requests captured: {len(unique_requests)}")
        
        return unique_requests

    def extract_tile_urls(self, requests):
        """Enhanced tile URL extraction with better pattern matching"""
        tile_urls = []
        tile_patterns = set()
        
        logger.info(f"🔍 Analyzing {len(requests)} requests for tile patterns...")
        
        for request in requests:
            url = request['url']
            
            # Enhanced tile detection patterns
            tile_indicators = [
                '.png', '.jpg', '.jpeg', '.webp', '.tiff', '.tif'
            ]
            
            # Check if this looks like a tile URL
            if any(ext in url.lower() for ext in tile_indicators):
                logger.info(f"🔍 Checking potential tile URL: {url}")
                
                # Pattern 1: Standard /z/x/y.ext
                tile_pattern = re.search(r'/(\d+)/(\d+)/(\d+)\.(png|jpg|jpeg|webp|tiff|tif)', url, re.IGNORECASE)
                
                if tile_pattern:
                    zoom, x, y, ext = tile_pattern.groups()
                    logger.info(f"✅ Found tile: z={zoom}, x={x}, y={y}, ext={ext}")
                    
                    # Extract base URL pattern
                    base_url = url.split(f'/{zoom}/{x}/{y}.{ext}')[0]
                    pattern = f"{base_url}/{{z}}/{{x}}/{{y}}.{ext}"
                    tile_patterns.add(pattern)
                    
                    tile_urls.append({
                        'url': url,
                        'base_url': base_url,
                        'zoom': int(zoom),
                        'x': int(x),
                        'y': int(y),
                        'format': ext.lower(),
                        'status': request['status'],
                        'pattern': pattern
                    })
                else:
                    # Pattern 2: Alternative formats
                    alt_patterns = [
                        r'/(\d+)_(\d+)_(\d+)\.(png|jpg|jpeg|webp)',
                        r'/(\d+)-(\d+)-(\d+)\.(png|jpg|jpeg|webp)',
                        r'/tile_(\d+)_(\d+)_(\d+)\.(png|jpg|jpeg|webp)'
                    ]
                    
                    for alt_pattern in alt_patterns:
                        alt_match = re.search(alt_pattern, url, re.IGNORECASE)
                        if alt_match:
                            zoom, x, y, ext = alt_match.groups()
                            logger.info(f"✅ Found alternative tile format: z={zoom}, x={x}, y={y}")
                            
                            # Create pattern
                            base_url = re.sub(alt_pattern, '', url)
                            pattern = f"{base_url}/{{z}}_{{x}}_{{y}}.{ext}"
                            tile_patterns.add(pattern)
                            
                            tile_urls.append({
                                'url': url,
                                'base_url': base_url,
                                'zoom': int(zoom),
                                'x': int(x),
                                'y': int(y),
                                'format': ext.lower(),
                                'status': request['status'],
                                'pattern': pattern
                            })
                            break
        
        logger.info(f"🎯 Extracted {len(tile_urls)} tile URLs")
        logger.info(f"🎯 Found {len(tile_patterns)} unique patterns")
        
        # Log found patterns for debugging
        for pattern in tile_patterns:
            logger.info(f"🎯 Pattern: {pattern}")
        
        return tile_urls, tile_patterns

    def systematic_zoom_coverage_for_patterns(self, location_name, lat, lng, duration_per_zoom=20):
        """Systematically cover all zoom levels 10-18 to discover patterns only"""
        logger.info(f"🎯 Starting systematic pattern discovery for {location_name}")
        
        zoom_levels = list(range(10, 19))  # 10 to 18
        all_discovered_patterns = set()
        
        for zoom_index, zoom in enumerate(zoom_levels):
            logger.info(f"🔍 Discovering patterns at zoom level {zoom} ({zoom_index+1}/{len(zoom_levels)})")
            
            try:
                # Clear network logs for this zoom level
                self.driver.get_log('performance')
                
                # Set specific zoom level
                zoom_success = self.map_handler.set_map_zoom(zoom)
                if not zoom_success:
                    self.map_handler.simulate_zoom_interaction(zoom)
                
                # Wait for tiles to load at this zoom
                time.sleep(3)
                
                # Perform coverage at this zoom using map handler
                action_count = self.map_handler.comprehensive_map_coverage(zoom, duration_per_zoom)
                
                # Get patterns discovered during this coverage
                requests = self.get_network_requests()
                tile_urls, zoom_patterns = self.extract_tile_urls(requests)
                
                # Add new patterns to our collection
                all_discovered_patterns.update(zoom_patterns)
                
                if zoom_patterns:
                    logger.info(f"✅ Zoom {zoom}: Discovered {len(zoom_patterns)} patterns")
                    for pattern in zoom_patterns:
                        logger.info(f"  📋 Pattern: {pattern}")
                else:
                    logger.warning(f"⚠️ Zoom {zoom}: No patterns discovered")
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Error at zoom {zoom}: {e}")
                continue
        
        logger.info(f"🎉 Pattern discovery complete: {len(all_discovered_patterns)} unique patterns")
        return list(all_discovered_patterns)

    def crawl_location_for_patterns(self, location_name, lat, lng, path):
        """Crawl location to discover tile patterns only"""
        clean_city_name = self.setup_city_directories(location_name)
        city_logger = setup_city_logging(clean_city_name)
        
        city_logger.info(f"🎯 PATTERN DISCOVERY: {location_name}")
        city_logger.info("=" * 60)
        
        try:
            # Navigate to location page
            if not self.navigate_to_location_page(location_name, path):
                return None
            
            # Take initial screenshot
            screenshot_path = f"output_browser_crawl/cities/{clean_city_name}/screenshots/initial_{datetime.now().strftime('%H%M%S')}.png"
            self.driver.save_screenshot(screenshot_path)
            city_logger.info(f"📸 Initial screenshot: {screenshot_path}")
            
            # Detect city boundaries
            bounds = self.map_handler.detect_city_boundaries(location_name)
            
            # Start pattern discovery across all zoom levels
            discovered_patterns = self.systematic_zoom_coverage_for_patterns(location_name, lat, lng)
            
            # Extract unique servers
            tile_servers = set()
            for pattern in discovered_patterns:
                server = self.extract_server_from_url(pattern)
                tile_servers.add(server)
            
            # Update global discovered data
            self.discovered_data['tile_patterns'].update(discovered_patterns)
            self.discovered_data['tile_servers'].update(tile_servers)
            self.discovered_data['success_count'] += 1
            
            location_result = {
                'location_name': location_name,
                'clean_name': clean_city_name,
                'timestamp': datetime.now().isoformat(),
                'coordinates': {
                    'lat': lat,
                    'lng': lng
                },
                'discovered_patterns': list(discovered_patterns),
                'tile_servers': list(tile_servers),
                'bounds': bounds,
                'pattern_count': len(discovered_patterns),
                'server_count': len(tile_servers)
            }
            
            # Save city-specific pattern report
            self.save_city_pattern_report(clean_city_name, location_result)
            
            city_logger.info(f"🎉 PATTERN DISCOVERY COMPLETE: {location_name}")
            city_logger.info(f"📊 Patterns discovered: {len(discovered_patterns)}")
            city_logger.info(f"📊 Servers found: {len(tile_servers)}")
            
            return location_result
            
        except Exception as e:
            city_logger.error(f"❌ Error in pattern discovery: {e}")
            self.discovered_data['failure_count'] += 1
            return None

    def save_city_pattern_report(self, clean_city_name, location_result):
        """Save detailed pattern report for city"""
        
        # Save JSON report
        json_path = f"output_browser_crawl/cities/{clean_city_name}/reports/patterns_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(location_result, f, indent=2, ensure_ascii=False)
        
        # Save text report
        text_report = f"""# TILE PATTERN DISCOVERY REPORT: {location_result['location_name']}
Generated: {location_result['timestamp']}
Method: Browser-based systematic zoom coverage (10-18)

## 📍 LOCATION INFO
• Name: {location_result['location_name']}
• Coordinates: {location_result['coordinates']['lat']}, {location_result['coordinates']['lng']}
• Bounds: {location_result['bounds']}

## 📊 DISCOVERY SUMMARY
• Patterns discovered: {location_result['pattern_count']}
• Tile servers: {location_result['server_count']}

## 🎯 DISCOVERED PATTERNS
"""
        
        for pattern in location_result['discovered_patterns']:
            text_report += f"• {pattern}\n"
        
        text_report += f"\n## 🗺️ TILE SERVERS\n"
        for server in location_result['tile_servers']:
            text_report += f"• {server}\n"
        
        text_path = f"output_browser_crawl/cities/{clean_city_name}/reports/patterns_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        logger.info(f"💾 City pattern report saved: {clean_city_name}")

    def extract_server_from_url(self, url):
        """Extract server base URL from tile URL"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def generate_final_patterns_report(self):
        """Generate final comprehensive patterns report"""
        logger.info("📊 Generating final patterns discovery report...")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'crawler': 'Browser Pattern Discovery Crawler v4.0',
            'method': 'Systematic zoom coverage 10-18 for pattern discovery',
            'summary': {
                'total_attempted': len(self.test_locations),
                'total_successful': self.discovered_data['success_count'],
                'total_failed': self.discovered_data['failure_count'],
                'success_rate': (self.discovered_data['success_count'] / len(self.test_locations) * 100) if len(self.test_locations) > 0 else 0,
                'unique_tile_patterns': len(self.discovered_data['tile_patterns']),
                'tile_servers': len(self.discovered_data['tile_servers'])
            },
            'tile_patterns': list(self.discovered_data['tile_patterns']),
            'tile_servers': list(self.discovered_data['tile_servers']),
            'successful_locations': self.discovered_data['all_locations']
        }
        
        # Save comprehensive report
        final_report_path = 'output_browser_crawl/reports/final_patterns_report.json'
        with open(final_report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_report = f"""# GULAND TILE PATTERNS DISCOVERY REPORT
Generated: {report['timestamp']}
Method: Browser-based systematic pattern discovery

## 📊 DISCOVERY SUMMARY
• Total Locations Attempted: {report['summary']['total_attempted']}
• Successful Discoveries: {report['summary']['total_successful']}
• Success Rate: {report['summary']['success_rate']:.1f}%
• Unique Tile Patterns: {report['summary']['unique_tile_patterns']}
• Tile Servers: {report['summary']['tile_servers']}

## 🎯 ALL DISCOVERED TILE PATTERNS
"""
        
        for pattern in report['tile_patterns']:
            text_report += f"• {pattern}\n"
        
        text_report += f"\n## 🗺️ TILE SERVERS\n"
        for server in report['tile_servers']:
            text_report += f"• {server}\n"
        
        # Add location breakdown
        text_report += f"\n## 📍 LOCATION BREAKDOWN\n"
        for location in report['successful_locations']:
            text_report += f"### {location['location_name']}\n"
            text_report += f"• Patterns found: {location['pattern_count']}\n"
            text_report += f"• Servers: {location['server_count']}\n"
            for pattern in location['discovered_patterns']:
                text_report += f"  - {pattern}\n"
            text_report += "\n"
        
        text_report_path = 'output_browser_crawl/reports/final_patterns_summary.txt'
        with open(text_report_path, 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        logger.info("✅ Final patterns report generated")
        
        # Print summary to console
        print(f"\n🎉 PATTERN DISCOVERY COMPLETED!")
        print("=" * 60)
        print(f"📊 Discovery Results:")
        print(f"  • Locations processed: {report['summary']['total_successful']}/{report['summary']['total_attempted']}")
        print(f"  • Success rate: {report['summary']['success_rate']:.1f}%")
        print(f"  • Unique tile patterns: {report['summary']['unique_tile_patterns']}")
        print(f"  • Tile servers: {report['summary']['tile_servers']}")
        print(f"\n📁 Reports saved to: output_browser_crawl/reports/")
        print(f"📁 City-specific data: output_browser_crawl/cities/")
        print(f"\n🚀 Next step: Run pattern_based_tile_crawler.py to download tiles")
        
        return report

    def run_pattern_discovery_crawl(self, max_hours=2):
        """Run complete pattern discovery crawl"""
        logger.info("🚀 STARTING PATTERN DISCOVERY CRAWL")
        logger.info("=" * 70)
        
        crawl_start_time = time.time()
        max_crawl_time = max_hours * 3600
        
        try:
            if not self.setup_driver():
                return None
            
            logger.info(f"⏰ Time limit set to {max_hours} hours")
            logger.info("🌐 Warming up session...")
            self.driver.get("https://guland.vn/")
            time.sleep(random.uniform(3, 6))
            
            for i, (location_name, lat, lng, path) in enumerate(self.test_locations, 1):
                # Check global timeout
                elapsed_time = time.time() - crawl_start_time
                remaining_time = max_crawl_time - elapsed_time
                
                if remaining_time < 300:  # Less than 5 minutes remaining
                    logger.warning(f"⏰ Time limit reached: {elapsed_time/3600:.1f}h/{max_hours}h")
                    break
                
                logger.info(f"\n🌍 PROCESSING {i}/{len(self.test_locations)}: {location_name}")
                logger.info(f"⏱️ Elapsed: {elapsed_time/3600:.1f}h, Remaining: {remaining_time/3600:.1f}h")
                logger.info("=" * 60)
                
                location_start_time = time.time()
                
                # Discover patterns for this location
                location_info = self.crawl_location_for_patterns(location_name, lat, lng, path)
                
                location_elapsed = time.time() - location_start_time
                logger.info(f"⏱️ {location_name} processed in {location_elapsed:.1f}s")
                
                if location_info:
                    self.discovered_data['all_locations'].append(location_info)
                    logger.info(f"✅ {location_name}: {location_info['pattern_count']} patterns discovered")
                else:
                    logger.warning(f"⚠️ Failed to discover patterns for {location_name}")
                
                # Delay between locations
                if i < len(self.test_locations):
                    delay = min(30, max(5, remaining_time * 0.02))
                    logger.info(f"⏳ Waiting {delay:.1f}s before next location...")
                    time.sleep(delay)
            
            # Generate final comprehensive report
            final_report = self.generate_final_patterns_report()
            return final_report
            
        except Exception as e:
            logger.error(f"❌ Pattern discovery crawl failed: {e}")
            return None
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("🔚 Browser closed")
            
            total_elapsed = time.time() - crawl_start_time
            logger.info(f"⏱️ Total crawl time: {total_elapsed/3600:.1f} hours")

    def navigate_to_location_page(self, location_name, path):
        """Navigate to planning page of location"""
        logger.info(f"🌐 Navigating to {location_name} planning page...")
        
        try:
            url = f"https://guland.vn/{path}"
            logger.info(f"🔗 Opening URL: {url}")
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for map/JS to initialize
            time.sleep(random.uniform(5, 8))
            
            logger.info(f"✅ Successfully navigated to {location_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to navigate to {location_name}: {e}")
            return False

def main():
    """Main function for pattern discovery"""
    print("🇻🇳 GULAND TILE PATTERN DISCOVERY")
    print("Discovers tile URL patterns for Vietnamese cities")
    print("=" * 60)
    
    # Time limit configuration
    print("\nTime limit options:")
    print("1. Quick (1 hour) - 2-3 cities")
    print("2. Standard (2 hours) - 5-7 cities") 
    print("3. Extended (4 hours) - 10-15 cities")
    print("4. Full (8 hours) - All cities")
    
    time_choice = input("Choose time limit (1-4, default 2): ").strip()
    time_map = {'1': 1, '2': 2, '3': 4, '4': 8}
    max_hours = time_map.get(time_choice, 2)
    
    # Browser mode
    headless_input = input("Run headless browser? (y/N): ").lower().strip()
    headless = headless_input in ['y', 'yes']
    
    # Create crawler
    crawler = BrowserGulandCrawler(headless=headless)
    
    print(f"\n🎯 DISCOVERY CONFIGURATION:")
    print(f"⏰ Time limit: {max_hours} hours")
    print(f"📊 Max locations: {len(crawler.test_locations)}")
    print(f"🖥️ Headless: {'Yes' if headless else 'No'}")
    print(f"🎯 Goal: Discover tile URL patterns only")
    
    confirm = input("\nStart pattern discovery? (Y/n): ").lower().strip()
    if confirm in ['n', 'no']:
        print("Cancelled.")
        return
    
    try:
        results = crawler.run_pattern_discovery_crawl(max_hours=max_hours)
        
        if results and results['summary']['total_successful'] > 0:
            print("\n✅ SUCCESS! Discovered tile patterns")
            print(f"🎯 Found {results['summary']['unique_tile_patterns']} unique patterns")
            print(f"🗺️ From {results['summary']['tile_servers']} different servers")
            print("📁 Check 'output_browser_crawl/' for detailed results")
            print("\n🚀 Next: Run pattern_based_tile_crawler.py to download tiles")
        else:
            print("\n⚠️ No patterns discovered")
            print("💡 Try increasing time limit or check target locations")
        
    except KeyboardInterrupt:
        print("\n⏹️ Discovery stopped by user")
    except Exception as e:
        print(f"\n❌ Discovery failed: {e}")
        print("💡 Check logs for details")

if __name__ == "__main__":
    main()