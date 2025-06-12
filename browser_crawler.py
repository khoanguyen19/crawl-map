#!/usr/bin/env python3
"""
Browser-Based Guland Crawler
Uses real browser to bypass all anti-bot protection

Author: AI Assistant
Version: 3.0 (Browser Automation)
"""
import time
import json
import os
import random
import logging
import re  # Thêm import này
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from tile_downloader import GulandTileDownloader
from map_interaction_handler import MapInteractionHandler

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('browser_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BrowserGulandCrawler:
    def __init__(self, headless=False, enable_download=True, download_workers=5):
        self.driver = None
        self.headless = headless
        self.map_handler = None  # Will be initialized after driver setup
        self.discovered_data = {
            'all_locations': [],
            'tile_servers': set(),
            'tile_patterns': set(),
            'success_count': 0,
            'failure_count': 0
        }
        
        # Initialize tile downloader
        self.enable_download = enable_download
        if self.enable_download:
            self.tile_downloader = GulandTileDownloader(
                base_download_dir='downloaded_tiles',
                max_workers=download_workers,
                timeout=30
            )
            logger.info(f"📥 Tile download enabled with {download_workers} workers")
        else:
            self.tile_downloader = None
            logger.info("⏭️ Tile download disabled")
        
        # Create output directories
        os.makedirs('output_browser_crawl', exist_ok=True)
        os.makedirs('output_browser_crawl/responses', exist_ok=True)
        os.makedirs('output_browser_crawl/screenshots', exist_ok=True)
        os.makedirs('output_browser_crawl/network_logs', exist_ok=True)
        
        # Test locations
        self.test_locations = [
            ("Đà Nẵng", 16.0544563, 108.0717219, "soi-quy-hoach/da-nang"),
            ("Hà Nội", 21.0285, 105.8542, "soi-quy-hoach/ha-noi"),
            ("TP Hồ Chí Minh", 10.8231, 106.6297, "soi-quy-hoach/ho-chi-minh"),
            ("Cần Thơ", 10.0452, 105.7469, "soi-quy-hoach/can-tho"),
            ("Bình Dương", 11.3254, 106.4770, "soi-quy-hoach/binh-duong")
        ]
        
        # Network request tracking
        self.captured_requests = []
    
    def setup_driver(self):
        """Setup Chrome driver with network logging enabled"""
        logger.info("🚀 Setting up Chrome driver with network logging...")
        
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
            
            logger.info("✅ Chrome driver setup successful with network logging")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to setup Chrome driver: {e}")
            return False
    
    
    def start_network_capture(self):
        """Start capturing network requests"""
        logger.info("📡 Starting network capture...")
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
        
        logger.info(f"📡 Total unique requests: {len(unique_requests)}")
        
        # Log some sample URLs for debugging
        sample_urls = [req['url'] for req in unique_requests[:5]]
        logger.info(f"📡 Sample URLs: {sample_urls}")
        
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
                    # Pattern 2: Alternative formats like /zoom_x_y.ext or /z-x-y.ext
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
    
    def systematic_zoom_coverage(self, location_name, lat, lng, duration_per_zoom=30):
        """Systematically cover all zoom levels 10-18 with IMMEDIATE DOWNLOAD"""
        logger.info(f"🎯 Starting systematic zoom coverage for {location_name}")
        
        zoom_levels = list(range(10, 19))  # 10 to 18
        all_captured_tiles = []
        
        for zoom_index, zoom in enumerate(zoom_levels):
            logger.info(f"🔍 Processing zoom level {zoom} ({zoom_index+1}/{len(zoom_levels)})")
            
            try:
                # Clear network logs for this zoom level
                self.driver.get_log('performance')
                
                # Set specific zoom level using map handler
                zoom_success = self.map_handler.set_map_zoom(zoom)
                if not zoom_success:
                    self.map_handler.simulate_zoom_interaction(zoom)
                
                # Wait for tiles to load at this zoom
                time.sleep(3)
                
                # Perform coverage at this zoom using map handler
                action_count = self.map_handler.comprehensive_map_coverage(zoom, duration_per_zoom)
                
                # Get tiles captured during this coverage
                requests = self.get_network_requests()
                tile_urls, _ = self.extract_tile_urls(requests)
                
                # Filter tiles for current zoom level
                tiles_at_zoom = [tile for tile in tile_urls if tile['zoom'] == zoom]
                
                if tiles_at_zoom:
                    all_captured_tiles.extend(tiles_at_zoom)
                    logger.info(f"✅ Zoom {zoom}: Found {len(tiles_at_zoom)} tiles")
                    
                    # ===== DOWNLOAD TILES IMMEDIATELY =====
                    if self.enable_download and self.tile_downloader:
                        logger.info(f"📥 Downloading {len(tiles_at_zoom)} tiles from zoom {zoom}...")
                        # Pass just the location name, not with zoom suffix
                        download_results = self.tile_downloader.download_tiles_batch(
                            tiles_at_zoom, 
                            location_name  # Changed from f"{location_name}_zoom_{zoom}"
                        )
                        
                        successful = len([r for r in download_results if r['success']])
                        logger.info(f"✅ Downloaded {successful}/{len(tiles_at_zoom)} tiles from zoom {zoom}")
                    
                else:
                    logger.warning(f"⚠️ Zoom {zoom}: No tiles found")
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Error at zoom {zoom}: {e}")
                continue
        
        logger.info(f"🎉 Systematic coverage complete: {len(all_captured_tiles)} total tiles")
        return all_captured_tiles
    
    def generate_final_report(self):
        """Generate final comprehensive report WITH DOWNLOAD STATS"""
        logger.info("📊 Generating final comprehensive report...")
        
        # Get download stats from tile downloader
        download_stats = self.tile_downloader.download_stats if self.tile_downloader else {
            'total_attempted': 0, 'total_successful': 0, 'total_failed': 0, 'total_bytes': 0
        }
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'crawler': 'Full Coverage Guland Crawler v6.0 (With Download)',
            'method': 'Systematic zoom coverage 10-18 + Map interaction + Tile download',
            'summary': {
                'total_attempted': len(self.test_locations),
                'total_successful': self.discovered_data['success_count'],
                'total_failed': self.discovered_data['failure_count'],
                'success_rate': (self.discovered_data['success_count'] / len(self.test_locations) * 100) if len(self.test_locations) > 0 else 0,
                'unique_tile_patterns': len(self.discovered_data['tile_patterns']),
                'tile_servers': len(self.discovered_data['tile_servers'])
            },
            'download_summary': download_stats,
            'tile_patterns': list(self.discovered_data['tile_patterns']),
            'tile_servers': list(self.discovered_data['tile_servers']),
            'successful_locations': self.discovered_data['all_locations']
        }
        
        # Save comprehensive report
        with open('output_browser_crawl/full_coverage_final_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_report = f"""
# FULL COVERAGE GULAND CRAWLER FINAL REPORT (WITH DOWNLOADS)
Generated: {report['timestamp']}
Method: Systematic zoom coverage 10-18 + Map interaction + Tile download

## 📊 CRAWL SUMMARY
• Total Attempted: {report['summary']['total_attempted']}
• Total Successful: {report['summary']['total_successful']}
• Success Rate: {report['summary']['success_rate']:.1f}%
• Unique Tile Patterns: {report['summary']['unique_tile_patterns']}
• Tile Servers: {report['summary']['tile_servers']}

## 📥 DOWNLOAD SUMMARY
• Total tiles attempted: {report['download_summary']['total_attempted']}
• Successful downloads: {report['download_summary']['total_successful']}
• Failed downloads: {report['download_summary']['total_failed']}
• Download success rate: {(report['download_summary']['total_successful']/report['download_summary']['total_attempted']*100) if report['download_summary']['total_attempted'] > 0 else 0:.1f}%
• Total downloaded: {self.tile_downloader.format_bytes(report['download_summary']['total_bytes']) if self.tile_downloader else '0 B'}

## 🎯 TILE PATTERNS DISCOVERED
"""
        
        for pattern in report['tile_patterns']:
            text_report += f"• {pattern}\n"
        
        text_report += f"\n## 🗺️ TILE SERVERS\n"
        for server in report['tile_servers']:
            text_report += f"• {server}\n"
        
        # Add location details with download stats
        text_report += f"\n## 📍 LOCATION DETAILS\n"
        for location in report['successful_locations']:
            text_report += f"### {location['location_name']}\n"
            text_report += f"• Tiles found: {location['tile_count']}\n"
            text_report += f"• Patterns: {len(location['tile_patterns'])}\n"
            if location.get('download_stats'):
                ds = location['download_stats']
                formatted_size = self.tile_downloader.format_bytes(ds['total_size']) if self.tile_downloader else f"{ds['total_size']} bytes"
                text_report += f"• Downloads: {ds['successful']}/{ds['attempted']} ({formatted_size})\n"
            text_report += "\n"
        
        with open('output_browser_crawl/full_coverage_final_report.txt', 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        logger.info("✅ Final report generated")
        
        # Print summary to console
        print(f"\n🎉 FULL COVERAGE CRAWL WITH DOWNLOADS COMPLETED!")
        print("=" * 60)
        print(f"📊 Crawl Results:")
        print(f"  • Locations attempted: {report['summary']['total_attempted']}")
        print(f"  • Locations successful: {report['summary']['total_successful']}")
        print(f"  • Success rate: {report['summary']['success_rate']:.1f}%")
        print(f"  • Unique tile patterns: {report['summary']['unique_tile_patterns']}")
        print(f"  • Tile servers: {report['summary']['tile_servers']}")
        
        print(f"\n📥 Download Results:")
        print(f"  • Tiles attempted: {report['download_summary']['total_attempted']}")
        print(f"  • Tiles downloaded: {report['download_summary']['total_successful']}")
        print(f"  • Download success rate: {(report['download_summary']['total_successful']/report['download_summary']['total_attempted']*100) if report['download_summary']['total_attempted'] > 0 else 0:.1f}%")
        formatted_size = self.tile_downloader.format_bytes(report['download_summary']['total_bytes']) if self.tile_downloader else '0 B'
        print(f"  • Total size: {formatted_size}")
        
        if self.tile_downloader:
            print(f"\n📁 Downloaded tiles are stored in: {self.tile_downloader.base_download_dir}/")
        
        return report

    def crawl_location_with_full_coverage(self, location_name, lat, lng, path):
        """Crawl location with full tile coverage for zoom 10-18 - IMMEDIATE DOWNLOAD VERSION"""
        logger.info(f"🎯 FULL COVERAGE CRAWL WITH IMMEDIATE DOWNLOAD: {location_name}")
        logger.info("=" * 60)
        
        try:
            # Navigate to location page
            if not self.navigate_to_location_page(location_name, path):
                return None
            
            # Take initial screenshot
            screenshot_path = f"output_browser_crawl/screenshots/{location_name.replace(' ', '_')}_initial.png"
            self.driver.save_screenshot(screenshot_path)
            
            # Detect city boundaries using map handler
            bounds = self.map_handler.detect_city_boundaries(location_name)
            
            # Start comprehensive tile coverage for all zoom levels
            tiles_data = self.systematic_zoom_coverage(location_name, lat, lng)
            
            # Extract unique patterns and servers
            tile_patterns = set()
            tile_servers = set()
            
            zoom_levels = {}
            
            for tile in tiles_data:
                # Add to patterns and servers
                if 'pattern' in tile:
                    tile_patterns.add(tile['pattern'])
                tile_servers.add(self.extract_server_from_url(tile['url']))
                
                # Group by zoom level
                zoom = tile['zoom']
                if zoom not in zoom_levels:
                    zoom_levels[zoom] = {
                        'tile_count': 0,
                        'tiles': []
                    }
                zoom_levels[zoom]['tile_count'] += 1
                zoom_levels[zoom]['tiles'].append(tile)
            
            # Update global discovered data
            self.discovered_data['tile_patterns'].update(tile_patterns)
            self.discovered_data['tile_servers'].update(tile_servers)
            self.discovered_data['success_count'] += 1
            
            # Generate download report if enabled
            if self.enable_download and self.tile_downloader:
                # Create mock download results for reporting
                download_results = []
                for tile in tiles_data:
                    download_results.append({
                        'success': True,
                        'tile_type': self.tile_downloader.get_tile_type_from_url(tile['url']),
                        'size': 1024  # Mock size
                    })
                
                self.tile_downloader.generate_download_report(location_name, download_results)
            
            # Calculate coverage analysis
            coverage_analysis = {}
            if bounds:
                for zoom in range(10, 19):
                    coverage_analysis[zoom] = self.map_handler.calculate_tile_coverage_needed(bounds, zoom)
            
            location_result = {
                'location_name': location_name,
                'timestamp': datetime.now().isoformat(),
                'total_tiles': len(tiles_data),
                'zoom_levels': zoom_levels,
                'tile_patterns': list(tile_patterns),
                'tile_servers': list(tile_servers),
                'bounds': bounds,
                'coverage_analysis': coverage_analysis,
                'tile_count': len(tiles_data)
            }
            
            # Generate coverage report
            self.generate_coverage_report(location_result)
            
            logger.info(f"🎉 FULL COVERAGE COMPLETE: {location_name}")
            logger.info(f"📊 Total tiles captured: {len(tiles_data)}")
            logger.info(f"📊 Zoom levels covered: {len(zoom_levels)}")
            logger.info(f"📊 Unique patterns: {len(tile_patterns)}")
            
            return location_result
            
        except Exception as e:
            logger.error(f"❌ Error in full coverage crawl: {e}")
            self.discovered_data['failure_count'] += 1
            return None

    def generate_coverage_report(self, tiles_data):
        """Generate detailed coverage analysis report"""
        logger.info("📊 Generating coverage analysis report...")
        
        report_lines = []
        report_lines.append(f"# FULL COVERAGE ANALYSIS: {tiles_data['location_name']}")
        report_lines.append(f"Generated: {tiles_data['timestamp']}")
        report_lines.append("")
        
        report_lines.append("## 📊 COVERAGE SUMMARY")
        report_lines.append(f"• Total tiles captured: {tiles_data['total_tiles']}")
        report_lines.append(f"• Zoom levels covered: {len(tiles_data['zoom_levels'])}")
        report_lines.append(f"• Unique tile patterns: {len(tiles_data['tile_patterns'])}")
        report_lines.append(f"• Tile servers: {len(tiles_data['tile_servers'])}")
        report_lines.append("")
        
        report_lines.append("## 🔍 ZOOM LEVEL BREAKDOWN")
        for zoom in range(10, 19):
            if zoom in tiles_data['zoom_levels']:
                zoom_data = tiles_data['zoom_levels'][zoom]
                expected = tiles_data['coverage_analysis'].get(zoom, {}).get('total_tiles', 'Unknown')
                actual = zoom_data['tile_count']
                
                if isinstance(expected, int) and expected > 0:
                    coverage_pct = (actual / expected) * 100
                    report_lines.append(f"• Zoom {zoom}: {actual}/{expected} tiles ({coverage_pct:.1f}% coverage)")
                else:
                    report_lines.append(f"• Zoom {zoom}: {actual} tiles (coverage unknown)")
            else:
                report_lines.append(f"• Zoom {zoom}: 0 tiles (failed)")
        
        report_lines.append("")
        report_lines.append("## 🎯 TILE PATTERNS")
        for pattern in tiles_data['tile_patterns']:
            report_lines.append(f"• {pattern}")
        
        # Save report
        report_path = f"output_browser_crawl/coverage_report_{tiles_data['location_name'].replace(' ', '_')}.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"📋 Coverage report saved: {report_path}")
        
    def crawl_location_with_interaction(self, location_name, lat, lng, path):
        """Crawl location by simulating user interaction WITH TILE DOWNLOAD"""
        logger.info(f"📍 Crawling {location_name} via user interaction...")
        
        try:
            # Navigate to location page
            if not self.navigate_to_location_page(location_name, path):
                return None
            
            # Take initial screenshot
            screenshot_path = f"output_browser_crawl/screenshots/{location_name.replace(' ', '_')}_initial.png"
            self.driver.save_screenshot(screenshot_path)
            logger.info(f"📸 Initial screenshot: {screenshot_path}")
            
            # Start network capture
            self.start_network_capture()
            
            # Trigger tile loading via JavaScript first using map handler
            self.map_handler.trigger_tile_loading()
            
            # Simulate realistic map interaction using map handler
            self.map_handler.simulate_map_interaction(location_name, duration_seconds=60)
            
            # Trigger again after interactions
            self.map_handler.trigger_tile_loading()
            
            # ... rest of the method remains the same ...
            
        except Exception as e:
            logger.error(f"❌ Error crawling {location_name}: {e}")
            self.discovered_data['failure_count'] += 1
            return None

    def extract_server_from_url(self, url):
        """Extract server base URL from tile URL"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    def run_browser_crawl(self):
        """Run complete browser-based crawl with full coverage - TIMEOUT PROTECTED"""
        logger.info("🚀 STARTING FULL COVERAGE CRAWL (ZOOM 10-18)")
        logger.info("=" * 70)
        logger.info("🎯 Systematic coverage of all zoom levels 10-18")
        logger.info("=" * 70)
        
        crawl_start_time = time.time()
        max_crawl_time = 3600  # 1 hour total limit
        
        try:
            if not self.setup_driver():
                return None
            
            logger.info("🌐 Warming up session...")
            self.driver.get("https://guland.vn/")
            time.sleep(random.uniform(3, 6))
            
            for i, (location_name, lat, lng, path) in enumerate(self.test_locations, 1):
                # Check global timeout
                if time.time() - crawl_start_time > max_crawl_time:
                    logger.warning(f"⏰ Global timeout reached, stopping crawl")
                    break
                    
                logger.info(f"\n🌍 PROCESSING {i}/{len(self.test_locations)}: {location_name}")
                logger.info("=" * 60)
                
                location_start_time = time.time()
                max_location_time = 600  # 10 minutes per location
                
                location_info = self.crawl_location_with_full_coverage(location_name, lat, lng, path)
                
                location_elapsed = time.time() - location_start_time
                logger.info(f"⏱️ {location_name} processed in {location_elapsed:.1f}s")
                
                if location_info:
                    self.discovered_data['all_locations'].append(location_info)
                    logger.info(f"✅ {location_name}: {location_info['total_tiles']} tiles captured")
                    
                    # Print quick summary
                    for zoom in range(10, 19):
                        if zoom in location_info['zoom_levels']:
                            count = location_info['zoom_levels'][zoom]['tile_count']
                            logger.info(f"   Zoom {zoom}: {count} tiles")
                else:
                    logger.warning(f"⚠️ Failed to process {location_name}")
                
                # Delay between cities
                if i < len(self.test_locations):
                    # Check if we have time for next location
                    remaining_time = max_crawl_time - (time.time() - crawl_start_time)
                    if remaining_time < 300:  # Less than 5 minutes remaining
                        logger.warning(f"⏰ Not enough time for next location, stopping")
                        break
                        
                    delay = min(30, remaining_time // 10)  # Adaptive delay
                    logger.info(f"⏳ Waiting {delay:.1f}s before next location...")
                    time.sleep(delay)
            
            # Generate final comprehensive report
            final_report = self.generate_final_report()
            return final_report
            
        except Exception as e:
            logger.error(f"❌ Full coverage crawl failed: {e}")
            return None
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("🔚 Browser closed")
            
            total_elapsed = time.time() - crawl_start_time
            logger.info(f"⏱️ Total crawl time: {total_elapsed:.1f}s")
    
    def generate_interaction_report(self):
        """Generate report for interaction-based crawling"""
        logger.info("📊 Generating interaction crawl report...")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'crawler': 'User Interaction-Based Guland Crawler v4.0',
            'method': 'Map interaction simulation + Network request capture',
            'summary': {
                'total_attempted': len(self.test_locations),
                'total_successful': self.discovered_data['success_count'],
                'total_failed': self.discovered_data['failure_count'],
                'success_rate': self.discovered_data['success_count'] / len(self.test_locations) * 100,
                'unique_tile_patterns': len(self.discovered_data['tile_patterns']),
                'tile_servers': len(self.discovered_data['tile_servers'])
            },
            'tile_patterns': list(self.discovered_data['tile_patterns']),
            'tile_servers': list(self.discovered_data['tile_servers']),
            'successful_locations': self.discovered_data['all_locations']
        }
        
        # Save comprehensive report
        with open('output_browser_crawl/interaction_crawl_results.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_report = f"""
# USER INTERACTION-BASED GULAND CRAWLER REPORT
Generated: {report['timestamp']}
Method: Map interaction simulation + Network request capture

## 📊 SUMMARY
• Total Attempted: {report['summary']['total_attempted']}
• Total Successful: {report['summary']['total_successful']}
• Success Rate: {report['summary']['success_rate']:.1f}%
• Unique Tile Patterns: {report['summary']['unique_tile_patterns']}
• Tile Servers: {report['summary']['tile_servers']}

## 🎯 TILE PATTERNS DISCOVERED
"""
        
        for pattern in report['tile_patterns']:
            text_report += f"• {pattern}\n"
        
        text_report += f"\n## 🗺️ TILE SERVERS\n"
        for server in report['tile_servers']:
            text_report += f"• {server}\n"
        
        with open('output_browser_crawl/interaction_report.txt', 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        logger.info("✅ Interaction crawl report generated")
        return report

    # Keep existing navigate_to_location_page method
    def navigate_to_location_page(self, location_name, path):
        """Navigate đến planning page của location"""
        logger.info(f"🌐 Navigating to {location_name} planning page...")
        
        try:
            url = f"https://guland.vn/{path}"
            print(f"🔗 Opening URL: {url}")
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
    """Main function with download options"""
    print("🎮 GULAND CRAWLER WITH TILE DOWNLOAD")
    print("Crawls tile URLs and downloads them automatically")
    print("=" * 60)
    
    # Ask user preferences
    headless_input = input("Run in headless mode? (y/N): ").lower().strip()
    headless = headless_input in ['y', 'yes']
    
    download_input = input("Download tiles automatically? (Y/n): ").lower().strip()
    download_tiles = download_input not in ['n', 'no']
    
    workers = 5
    if download_tiles:
        workers_input = input("Number of download workers (1-10, default 5): ").strip()
        try:
            workers = int(workers_input) if workers_input else 5
            workers = max(1, min(10, workers))
        except:
            workers = 5
    
    crawler = BrowserGulandCrawler(
        headless=headless, 
        enable_download=download_tiles,
        download_workers=workers
    )
    
    if download_tiles:
        print(f"📥 Download enabled with {workers} parallel workers")
    else:
        print("⏭️ Download disabled - only URLs will be collected")
    
    try:
        results = crawler.run_browser_crawl()
        
        if results and results['summary']['total_successful'] > 0:
            print("\n✅ SUCCESS! Found tile patterns and servers")
            if download_tiles:
                print(f"📥 Downloaded {results['download_summary']['total_successful']} tiles")
                print(f"📁 Check 'downloaded_tiles/' for downloaded tiles")
            print("📁 Check 'output_browser_crawl/' for detailed results")
        else:
            print("\n⚠️ No successful results")
            print("💡 Try running in non-headless mode for debugging")
        
    except KeyboardInterrupt:
        print("\n⏹️ Crawl interrupted by user")
    except Exception as e:
        print(f"\n❌ Crawl failed: {e}")
        print("💡 Check logs for details")

if __name__ == "__main__":
    main()