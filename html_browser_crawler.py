#!/usr/bin/env python3
"""
HTML Pattern Extractor for Guland
Directly extracts tile patterns from HTML data-url attributes
Much simpler and faster than network monitoring approach

Author: AI Assistant  
Version: 6.0 (HTML Parsing Approach)
"""

import time
import json
import os
import logging
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('guland_html_pattern_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GulandHTMLPatternExtractor:
    def __init__(self, headless=False):
        self.driver = None
        self.headless = headless
        self.extracted_data = {
            'all_locations': [],
            'all_patterns': {},
            'success_count': 0,
            'failure_count': 0,
            'total_patterns_found': 0
        }
        
        # Vietnam provinces/cities to process
        self.test_locations = [
            ("TP Hồ Chí Minh", "soi-quy-hoach/tp-ho-chi-minh"),
            ("Đồng Nai", "soi-quy-hoach/dong-nai"),
            ("Bà Rịa - Vũng Tàu", "soi-quy-hoach/ba-ria-vung-tau"),
            ("An Giang", "soi-quy-hoach/an-giang"),
            ("Bắc Giang", "soi-quy-hoach/bac-giang"),
            ("Bắc Kạn", "soi-quy-hoach/bac-kan"),
            ("Bạc Liêu", "soi-quy-hoach/bac-lieu"),
            ("Bắc Ninh", "soi-quy-hoach/bac-ninh"),
            ("Bến Tre", "soi-quy-hoach/ben-tre"),
            ("Bình Dương", "soi-quy-hoach/binh-duong"),
            ("Bình Phước", "soi-quy-hoach/binh-phuoc"),
            ("Bình Thuận", "soi-quy-hoach/binh-thuan"),
            ("Bình Định", "soi-quy-hoach/binh-dinh"),
            ("Cà Mau", "soi-quy-hoach/ca-mau"),
            ("Cần Thơ", "soi-quy-hoach/can-tho"),
            ("Cao Bằng", "soi-quy-hoach/cao-bang"),
            ("Gia Lai", "soi-quy-hoach/gia-lai"),
            ("Hà Nam", "soi-quy-hoach/ha-nam"),
            ("Hà Giang", "soi-quy-hoach/ha-giang"),
            ("Hà Nội", "soi-quy-hoach/ha-noi"),
            ("Hà Tĩnh", "soi-quy-hoach/ha-tinh"),
            ("Hải Dương", "soi-quy-hoach/hai-duong"),
            ("Hải Phòng", "soi-quy-hoach/hai-phong"),
            ("Hậu Giang", "soi-quy-hoach/hau-giang"),
            ("Hòa Bình", "soi-quy-hoach/hoa-binh"),
            ("Hưng Yên", "soi-quy-hoach/hung-yen"),
            ("Khánh Hòa", "soi-quy-hoach/khanh-hoa"),
            ("Kiên Giang", "soi-quy-hoach/kien-giang"),
            ("Kon Tum", "soi-quy-hoach/kon-tum"),
            ("Lai Châu", "soi-quy-hoach/lai-chau"),
            ("Lâm Đồng", "soi-quy-hoach/lam-dong"),
            ("Lạng Sơn", "soi-quy-hoach/lang-son"),
            ("Lào Cai", "soi-quy-hoach/lao-cai"),
            ("Long An", "soi-quy-hoach/long-an"),
            ("Nam Định", "soi-quy-hoach/nam-dinh"),
            ("Nghệ An", "soi-quy-hoach/nghe-an"),
            ("Ninh Bình", "soi-quy-hoach/ninh-binh"),
            ("Ninh Thuận", "soi-quy-hoach/ninh-thuan"),
            ("Phú Thọ", "soi-quy-hoach/phu-tho"),
            ("Phú Yên", "soi-quy-hoach/phu-yen"),
            ("Quảng Bình", "soi-quy-hoach/quang-binh"),
            ("Quảng Nam", "soi-quy-hoach/quang-nam"),
            ("Quảng Ngãi", "soi-quy-hoach/quang-ngai"),
            ("Quảng Ninh", "soi-quy-hoach/quang-ninh"),
            ("Quảng Trị", "soi-quy-hoach/quang-tri"),
            ("Sóc Trăng", "soi-quy-hoach/soc-trang"),
            ("Sơn La", "soi-quy-hoach/son-la"),
            ("Tây Ninh", "soi-quy-hoach/tay-ninh"),
            ("Thái Bình", "soi-quy-hoach/thai-binh"),
            ("Thái Nguyên", "soi-quy-hoach/thai-nguyen"),
            ("Thanh Hóa", "soi-quy-hoach/thanh-hoa"),
            ("Thừa Thiên Huế", "soi-quy-hoach/thua-thien-hue"),
            ("Tiền Giang", "soi-quy-hoach/tien-giang"),
            ("Trà Vinh", "soi-quy-hoach/tra-vinh"),
            ("Tuyên Quang", "soi-quy-hoach/tuyen-quang"),
            ("Vĩnh Long", "soi-quy-hoach/vinh-long"),
            ("Vĩnh Phúc", "soi-quy-hoach/vinh-phuc"),
            ("Yên Bái", "soi-quy-hoach/yen-bai"),
            ("Đà Nẵng", "soi-quy-hoach/da-nang"),
            ("Đắk Lắk", "soi-quy-hoach/dak-lak"),
            ("Đắk Nông", "soi-quy-hoach/dak-nong"),
            ("Điện Biên", "soi-quy-hoach/dien-bien"),
            ("Đồng Tháp", "soi-quy-hoach/dong-thap")
        ]
        
        self.setup_output_structure()

    def setup_output_structure(self):
        """Setup output directories"""
        dirs = [
            'output_html_patterns',
            'output_html_patterns/locations',
            'output_html_patterns/reports'
        ]
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)
        logger.info("📁 Output structure created")

    def setup_driver(self):
        """Setup Chrome driver"""
        logger.info("🚀 Setting up Chrome driver...")
        
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            logger.info("✅ Chrome driver setup successful")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to setup Chrome driver: {e}")
            return False

    def extract_map_patterns_from_html(self, location_name):
        """Extract all map type patterns from HTML data-url attributes"""
        logger.info(f"🔍 Extracting patterns for {location_name}")
        
        patterns_found = {
            'location_name': location_name,
            'timestamp': datetime.now().isoformat(),
            'map_types': {},
            'total_patterns': 0
        }
        
        try:
            # Find the map control container
            container_selectors = [
                ".sqh-btn-btm",
                ".sqh-btn-btm__wrp", 
                ".btn--map-switch"
            ]
            
            map_buttons = []
            for selector in container_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if selector == ".btn--map-switch":
                        map_buttons = elements
                        break
                    elif elements:
                        # Look for buttons within container
                        for container in elements:
                            buttons = container.find_elements(By.CSS_SELECTOR, ".btn--map-switch")
                            if buttons:
                                map_buttons = buttons
                                break
                        if map_buttons:
                            break
                except:
                    continue
            
            if not map_buttons:
                logger.warning(f"⚠️ No map buttons found for {location_name}")
                return patterns_found
            
            logger.info(f"✅ Found {len(map_buttons)} map type buttons")
            
            # Extract data from each button
            for button in map_buttons:
                try:
                    button_text = button.text.strip()
                    data_type = button.get_attribute('data-type')
                    data_url = button.get_attribute('data-url')
                    data_url_2030 = button.get_attribute('data-url-2030')
                    max_zoom = button.get_attribute('data-max-zoom')
                    is_active = 'active' in (button.get_attribute('class') or '')
                    
                    # Skip if no URL data
                    if not data_url and not data_url_2030:
                        continue
                    
                    # Determine map type from button text and data-type
                    map_type_key = self.classify_map_type(button_text, data_type)
                    
                    map_type_data = {
                        'display_name': button_text,
                        'data_type': data_type,
                        'tile_url': data_url,
                        'tile_url_2030': data_url_2030,
                        'max_zoom': max_zoom,
                        'is_active': is_active,
                        'server': self.extract_server_from_url(data_url) if data_url else None
                    }
                    
                    patterns_found['map_types'][map_type_key] = map_type_data
                    patterns_found['total_patterns'] += 1
                    
                    logger.info(f"📋 {button_text}: {data_url}")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error extracting button data: {e}")
                    continue
            
            logger.info(f"✅ Extracted {patterns_found['total_patterns']} patterns from {location_name}")
            return patterns_found
            
        except Exception as e:
            logger.error(f"❌ Error extracting patterns from {location_name}: {e}")
            return patterns_found

    def classify_map_type(self, button_text, data_type):
        """Classify map type based on button text and data-type"""
        text_lower = button_text.lower()
        
        if 'qh 2030' in text_lower or data_type == 'layer_1':
            return 'QH_2030'
        elif 'kh 2025' in text_lower or data_type == 'layer_2022':
            return 'KH_2025'
        elif 'qh phân khu' in text_lower or 'qh 1/500' in text_lower:
            return 'QH_PHAN_KHU'
        elif 'qh khác' in text_lower or data_type == 'layer_qhpk':
            return 'QH_KHAC'
        else:
            # Generic classification
            return f"UNKNOWN_{data_type or 'NO_TYPE'}"

    def extract_server_from_url(self, url):
        """Extract server base URL"""
        if not url:
            return None
        try:
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}"
        except:
            return None

    def process_location(self, location_name, path):
        """Process single location to extract patterns"""
        logger.info(f"🌍 Processing {location_name}")
        
        try:
            # Navigate to location
            url = f"https://guland.vn/{path}"
            logger.info(f"🔗 Opening: {url}")
            self.driver.get(url)
            
            # Wait for page load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for JavaScript and map to initialize
            time.sleep(8)
            
            # Extract patterns
            location_data = self.extract_map_patterns_from_html(location_name)
            
            if location_data['total_patterns'] > 0:
                # Save location-specific data
                self.save_location_data(location_name, location_data)
                
                # Update global data
                self.extracted_data['all_locations'].append(location_data)
                self.extracted_data['success_count'] += 1
                self.extracted_data['total_patterns_found'] += location_data['total_patterns']
                
                # Add to global patterns collection
                if location_name not in self.extracted_data['all_patterns']:
                    self.extracted_data['all_patterns'][location_name] = location_data['map_types']
                
                logger.info(f"✅ {location_name}: {location_data['total_patterns']} patterns extracted")
                return location_data
            else:
                logger.warning(f"⚠️ No patterns found for {location_name}")
                self.extracted_data['failure_count'] += 1
                return None
                
        except Exception as e:
            logger.error(f"❌ Error processing {location_name}: {e}")
            self.extracted_data['failure_count'] += 1
            return None

    def save_location_data(self, location_name, location_data):
        """Save individual location data"""
        clean_name = location_name.replace(' ', '_').replace('TP ', '')
        
        # JSON data
        json_path = f"output_html_patterns/locations/{clean_name}_patterns.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(location_data, f, indent=2, ensure_ascii=False)
        
        # Text summary
        text_report = f"""# TILE PATTERNS: {location_name}
Generated: {location_data['timestamp']}
Method: HTML data-url attribute extraction

## 📊 SUMMARY
• Total map types: {location_data['total_patterns']}

## 🗺️ MAP TYPE PATTERNS
"""
        
        for map_type, data in location_data['map_types'].items():
            text_report += f"""
### {data['display_name']} ({map_type})
• Data Type: {data['data_type']}
• Tile URL: {data['tile_url']}
• Server: {data['server']}
• Max Zoom: {data['max_zoom']}
• Active: {data['is_active']}
"""
            if data['tile_url_2030']:
                text_report += f"• Alternative URL (2030): {data['tile_url_2030']}\n"
        
        text_path = f"output_html_patterns/locations/{clean_name}_summary.txt"
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(text_report)

    def generate_comprehensive_report(self):
        """Generate final comprehensive report"""
        logger.info("📊 Generating comprehensive patterns report...")
        
        # Collect all unique patterns and servers
        all_tile_patterns = set()
        all_servers = set()
        
        for location_data in self.extracted_data['all_locations']:
            for map_type, data in location_data['map_types'].items():
                if data['tile_url']:
                    all_tile_patterns.add(data['tile_url'])
                    if data['server']:
                        all_servers.add(data['server'])
                if data['tile_url_2030']:
                    all_tile_patterns.add(data['tile_url_2030'])
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'extraction_method': 'HTML data-url attribute parsing',
            'summary': {
                'total_locations_processed': len(self.test_locations),
                'successful_extractions': self.extracted_data['success_count'],
                'failed_extractions': self.extracted_data['failure_count'],
                'success_rate': (self.extracted_data['success_count'] / len(self.test_locations) * 100) if len(self.test_locations) > 0 else 0,
                'total_patterns_found': self.extracted_data['total_patterns_found'],
                'unique_tile_urls': len(all_tile_patterns),
                'unique_servers': len(all_servers)
            },
            'all_tile_patterns': list(all_tile_patterns),
            'all_servers': list(all_servers),
            'location_breakdown': self.extracted_data['all_patterns']
        }
        
        # Save JSON report
        json_path = 'output_html_patterns/reports/comprehensive_patterns_report.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_report = f"""# COMPREHENSIVE GULAND TILE PATTERNS REPORT
Generated: {report['timestamp']}
Method: Direct HTML parsing of data-url attributes

## 📊 EXTRACTION SUMMARY
• Locations processed: {report['summary']['successful_extractions']}/{report['summary']['total_locations_processed']}
• Success rate: {report['summary']['success_rate']:.1f}%
• Total patterns found: {report['summary']['total_patterns_found']}
• Unique tile URLs: {report['summary']['unique_tile_urls']}
• Unique servers: {report['summary']['unique_servers']}

## 🗺️ ALL UNIQUE TILE PATTERNS
"""
        
        for pattern in sorted(report['all_tile_patterns']):
            text_report += f"• {pattern}\n"
        
        text_report += f"\n## 🏢 TILE SERVERS\n"
        for server in sorted(report['all_servers']):
            text_report += f"• {server}\n"
        
        text_report += f"\n## 📍 LOCATION BREAKDOWN\n"
        for location, map_types in report['location_breakdown'].items():
            text_report += f"\n### {location}\n"
            for map_type, data in map_types.items():
                text_report += f"• {data['display_name']}: {data['tile_url']}\n"
        
        text_path = 'output_html_patterns/reports/comprehensive_summary.txt'
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        logger.info("✅ Comprehensive report generated")
        
        # Print summary
        print(f"\n🎉 PATTERN EXTRACTION COMPLETED!")
        print("=" * 60)
        print(f"📊 Results:")
        print(f"  • Successful: {report['summary']['successful_extractions']}/{report['summary']['total_locations_processed']} locations")
        print(f"  • Success rate: {report['summary']['success_rate']:.1f}%")
        print(f"  • Total patterns: {report['summary']['total_patterns_found']}")
        print(f"  • Unique URLs: {report['summary']['unique_tile_urls']}")
        print(f"  • Servers: {report['summary']['unique_servers']}")
        print(f"\n📁 Reports saved to: output_html_patterns/reports/")
        
        return report

    def run_extraction(self, max_locations=None):
        """Run the complete pattern extraction process"""
        logger.info("🚀 STARTING HTML PATTERN EXTRACTION")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            if not self.setup_driver():
                return None
            
            # Determine locations to process
            locations_to_process = self.test_locations[:max_locations] if max_locations else self.test_locations
            
            logger.info(f"🎯 Processing {len(locations_to_process)} locations")
            
            for i, (location_name, path) in enumerate(locations_to_process, 1):
                logger.info(f"\n🌍 ({i}/{len(locations_to_process)}) Processing: {location_name}")
                
                self.process_location(location_name, path)
                
                # Small delay between locations
                if i < len(locations_to_process):
                    time.sleep(2)
            
            # Generate final report
            final_report = self.generate_comprehensive_report()
            
            elapsed_time = time.time() - start_time
            logger.info(f"⏱️ Total extraction time: {elapsed_time:.1f} seconds")
            
            return final_report
            
        except Exception as e:
            logger.error(f"❌ Extraction failed: {e}")
            return None
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("🔚 Browser closed")

def main():
    """Main function"""
    print("🇻🇳 GULAND HTML PATTERN EXTRACTOR")
    print("Direct extraction of tile patterns from HTML attributes")
    print("=" * 60)
    
    # Location limit for testing
    print("\nProcessing options:")
    print("1. Test (5 locations)")
    print("2. Sample (15 locations)")  
    print("3. All locations (63 provinces)")
    
    choice = input("Choose option (1/2/3, default 1): ").strip()
    
    if choice == '2':
        max_locations = 15
    elif choice == '3':
        max_locations = None
    else:
        max_locations = 5
    
    # Browser mode
    headless_input = input("Run headless? (y/N): ").lower().strip()
    headless = headless_input in ['y', 'yes']
    
    print(f"\n🎯 EXTRACTION CONFIGURATION:")
    print(f"📍 Locations: {max_locations if max_locations else 'All (63)'}")
    print(f"🖥️ Headless: {'Yes' if headless else 'No'}")
    print(f"⚡ Method: Direct HTML parsing (super fast!)")
    
    confirm = input("\nStart extraction? (Y/n): ").lower().strip()
    if confirm in ['n', 'no']:
        print("Cancelled.")
        return
    
    try:
        extractor = GulandHTMLPatternExtractor(headless=headless)
        results = extractor.run_extraction(max_locations=max_locations)
        
        if results:
            print("\n✅ SUCCESS! All tile patterns extracted")
            print("📁 Check 'output_html_patterns/' for results")
        else:
            print("\n❌ Extraction failed")
        
    except KeyboardInterrupt:
        print("\n⏹️ Extraction stopped by user")
    except Exception as e:
        print(f"\n❌ Extraction failed: {e}")

if __name__ == "__main__":
    main()