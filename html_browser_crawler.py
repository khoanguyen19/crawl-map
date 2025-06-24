#!/usr/bin/env python3
"""
Enhanced HTML Pattern Extractor for Guland
Now includes district-level extraction for KH_2025 map type
Directly extracts tile patterns from HTML data-url attributes

Author: AI Assistant  
Version: 7.0 (Enhanced with District Level Support)
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
import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('guland_enhanced_pattern_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedGulandPatternExtractor:
    def __init__(self, headless=False):
        self.driver = None
        self.headless = headless
        self.extracted_data = {
            'all_locations': [],
            'all_patterns': {},
            'district_patterns': {},  # New: Store district-level patterns
            'success_count': 0,
            'failure_count': 0,
            'total_patterns_found': 0,
            'district_success_count': 0,  # New: Track district extractions
            'district_failure_count': 0   # New: Track district failures
        }
        
        # Vietnam provinces/cities with their administrative codes
        # You can enhance this with actual administrative codes from your Excel file
        self.test_locations = [
            ("TP Hồ Chí Minh", "soi-quy-hoach/tp-ho-chi-minh", "79"),  # Added admin code
            ("Đồng Nai", "soi-quy-hoach/dong-nai", "75"),
            ("Bà Rịa - Vũng Tàu", "soi-quy-hoach/ba-ria-vung-tau", "77"),
            ("An Giang", "soi-quy-hoach/an-giang", "89"),
            ("Bắc Giang", "soi-quy-hoach/bac-giang", "24"),
            ("Bắc Kạn", "soi-quy-hoach/bac-kan", "06"),
            ("Bạc Liêu", "soi-quy-hoach/bac-lieu", "95"),
            ("Bắc Ninh", "soi-quy-hoach/bac-ninh", "27"),
            ("Bến Tre", "soi-quy-hoach/ben-tre", "83"),
            ("Bình Dương", "soi-quy-hoach/binh-duong", "74"),
            ("Bình Phước", "soi-quy-hoach/binh-phuoc", "70"),
            ("Bình Thuận", "soi-quy-hoach/binh-thuan", "60"),
            ("Bình Định", "soi-quy-hoach/binh-dinh", "52"),
            ("Cà Mau", "soi-quy-hoach/ca-mau", "96"),
            ("Cần Thơ", "soi-quy-hoach/can-tho", "92"),
            ("Cao Bằng", "soi-quy-hoach/cao-bang", "04"),
            ("Gia Lai", "soi-quy-hoach/gia-lai", "64"),  # Known admin code from example
            ("Hà Nam", "soi-quy-hoach/ha-nam", "35"),
            ("Hà Giang", "soi-quy-hoach/ha-giang", "02"),
            ("Hà Nội", "soi-quy-hoach/ha-noi", "01"),
            ("Hà Tĩnh", "soi-quy-hoach/ha-tinh", "42"),
            ("Hải Dương", "soi-quy-hoach/hai-duong", "31"),
            ("Hải Phòng", "soi-quy-hoach/hai-phong", "31"),
            ("Hậu Giang", "soi-quy-hoach/hau-giang", "93"),
            ("Hòa Bình", "soi-quy-hoach/hoa-binh", "17"),
            ("Hưng Yên", "soi-quy-hoach/hung-yen", "33"),
            ("Khánh Hòa", "soi-quy-hoach/khanh-hoa", "58"),
            ("Kiên Giang", "soi-quy-hoach/kien-giang", "91"),
            ("Kon Tum", "soi-quy-hoach/kon-tum", "62"),
            ("Lai Châu", "soi-quy-hoach/lai-chau", "12"),
            ("Lâm Đồng", "soi-quy-hoach/lam-dong", "68"),
            ("Lạng Sơn", "soi-quy-hoach/lang-son", "20"),
            ("Lào Cai", "soi-quy-hoach/lao-cai", "10"),
            ("Long An", "soi-quy-hoach/long-an", "80"),
            ("Nam Định", "soi-quy-hoach/nam-dinh", "36"),
            ("Nghệ An", "soi-quy-hoach/nghe-an", "40"),
            ("Ninh Bình", "soi-quy-hoach/ninh-binh", "37"),
            ("Ninh Thuận", "soi-quy-hoach/ninh-thuan", "58"),
            ("Phú Thọ", "soi-quy-hoach/phu-tho", "25"),
            ("Phú Yên", "soi-quy-hoach/phu-yen", "54"),
            ("Quảng Bình", "soi-quy-hoach/quang-binh", "44"),
            ("Quảng Nam", "soi-quy-hoach/quang-nam", "49"),
            ("Quảng Ngãi", "soi-quy-hoach/quang-ngai", "51"),
            ("Quảng Ninh", "soi-quy-hoach/quang-ninh", "22"),
            ("Quảng Trị", "soi-quy-hoach/quang-tri", "45"),
            ("Sóc Trăng", "soi-quy-hoach/soc-trang", "94"),
            ("Sơn La", "soi-quy-hoach/son-la", "14"),
            ("Tây Ninh", "soi-quy-hoach/tay-ninh", "72"),
            ("Thái Bình", "soi-quy-hoach/thai-binh", "34"),
            ("Thái Nguyên", "soi-quy-hoach/thai-nguyen", "19"),
            ("Thanh Hóa", "soi-quy-hoach/thanh-hoa", "38"),
            ("Thừa Thiên Huế", "soi-quy-hoach/thua-thien-hue", "46"),
            ("Tiền Giang", "soi-quy-hoach/tien-giang", "82"),
            ("Trà Vinh", "soi-quy-hoach/tra-vinh", "84"),
            ("Tuyên Quang", "soi-quy-hoach/tuyen-quang", "08"),
            ("Vĩnh Long", "soi-quy-hoach/vinh-long", "86"),
            ("Vĩnh Phúc", "soi-quy-hoach/vinh-phuc", "26"),
            ("Yên Bái", "soi-quy-hoach/yen-bai", "15"),
            ("Đà Nẵng", "soi-quy-hoach/da-nang", "48"),
            ("Đắk Lắk", "soi-quy-hoach/dak-lak", "66"),
            ("Đắk Nông", "soi-quy-hoach/dak-nong", "67"),
            ("Điện Biên", "soi-quy-hoach/dien-bien", "11"),
            ("Đồng Tháp", "soi-quy-hoach/dong-thap", "87")
        ]
        
        self.setup_output_structure()

    def setup_output_structure(self):
        """Setup output directories"""
        dirs = [
            'output_enhanced_patterns',
            'output_enhanced_patterns/provinces',
            'output_enhanced_patterns/districts',
            'output_enhanced_patterns/reports'
        ]
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)
        logger.info("📁 Enhanced output structure created")

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

    def get_csrf_token(self):
        """Extract CSRF token from current page"""
        try:
            # Try multiple methods to get CSRF token
            csrf_selectors = [
                'meta[name="csrf-token"]',
                'input[name="_token"]',
                'meta[name="_token"]'
            ]
            
            for selector in csrf_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    token = element.get_attribute('content') or element.get_attribute('value')
                    if token:
                        logger.info(f"✅ Found CSRF token via {selector}")
                        return token
                except:
                    continue
            
            # Try to extract from JavaScript
            try:
                token = self.driver.execute_script("""
                    // Try window.Laravel
                    if (window.Laravel && window.Laravel.csrfToken) {
                        return window.Laravel.csrfToken;
                    }
                    // Try meta tag
                    var meta = document.querySelector('meta[name="csrf-token"]');
                    if (meta) return meta.getAttribute('content');
                    // Try input
                    var input = document.querySelector('input[name="_token"]');
                    if (input) return input.value;
                    return null;
                """)
                if token:
                    logger.info("✅ Found CSRF token via JavaScript")
                    return token
            except:
                pass
            
            logger.warning("⚠️ Could not find CSRF token")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting CSRF token: {e}")
            return None

    def get_districts_via_api(self, admin_code):
        """Get districts list via independent API call (bypass Cloudflare)"""
        try:
            logger.info(f"🌐 Fetching districts for admin code: {admin_code}")
            
            # Use independent requests session (not from Selenium)
            # This bypasses Cloudflare's detection of automated browsers
            url = f"https://guland.vn/get-sub-location?id={admin_code}&is_bds=1"
            
            # Simple headers like Postman
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            logger.info(f"🔗 API URL: {url}")
            logger.info("🚀 Using independent requests (bypassing Selenium session)")
            
            # Make simple GET request like Postman
            response = requests.get(url, headers=headers, timeout=15)
            
            logger.info(f"📡 Response status: {response.status_code}")
            
            if response.status_code == 200:
                # Parse HTML response
                soup = BeautifulSoup(response.text, 'html.parser')
                options = soup.find_all('option')
                
                districts = []
                for option in options:
                    value = option.get('value', '').strip()
                    text = option.text.strip()
                    
                    if value and value != "" and text != "- Chọn -":
                        districts.append({
                            'id': value,
                            'name': text,
                            'slug': self.create_district_slug(text)
                        })
                
                logger.info(f"✅ Found {len(districts)} districts")
                return districts
                
            elif response.status_code == 403:
                logger.warning("⚠️ Still getting 403 - trying with different user agent...")
                return self.get_districts_alternative_ua(admin_code)
                
            else:
                logger.warning(f"⚠️ API call failed with status: {response.status_code}")
                logger.warning(f"Response content: {response.text[:200]}...")
                return self.get_districts_fallback_method(admin_code)
                
        except Exception as e:
            logger.error(f"❌ Error fetching districts via API: {e}")
            return self.get_districts_fallback_method(admin_code)

    def get_districts_alternative_ua(self, admin_code):
        """Try with Postman-like user agent"""
        try:
            logger.info("🔄 Trying with Postman-like headers...")
            
            url = f"https://guland.vn/get-sub-location?id={admin_code}&is_bds=1"
            
            # Postman default headers
            headers = {
                'User-Agent': 'PostmanRuntime/7.32.3',
                'Accept': '*/*',
                'Cache-Control': 'no-cache',
                'Host': 'guland.vn',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive'
            }
            
            response = requests.get(url, headers=headers, timeout=15)
            logger.info(f"📡 Alternative request status: {response.status_code}")
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                options = soup.find_all('option')
                
                districts = []
                for option in options:
                    value = option.get('value', '').strip()
                    text = option.text.strip()
                    
                    if value and value != "" and text != "- Chọn -":
                        districts.append({
                            'id': value,
                            'name': text,
                            'slug': self.create_district_slug(text)
                        })
                
                logger.info(f"✅ Alternative method found {len(districts)} districts")
                return districts
            else:
                logger.warning(f"⚠️ Alternative method also failed: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Alternative method error: {e}")
            return []

    def get_districts_fallback_method(self, admin_code):
        """Fallback method: try to interact with the district dropdown directly"""
        try:
            logger.info("🔄 Trying fallback method: direct dropdown interaction")
            
            # Wait a bit for page to fully load
            time.sleep(3)
            
            # Try to find and interact with the district dropdown
            district_selectors = [
                'select[name="district_id"]',
                'select#district_id',
                '.district-select',
                'select.form-control:nth-of-type(2)'
            ]
            
            for selector in district_selectors:
                try:
                    dropdown = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    # Trigger change event or click to load options
                    self.driver.execute_script("arguments[0].click();", dropdown)
                    time.sleep(2)
                    
                    # Get options
                    options = dropdown.find_elements(By.TAG_NAME, 'option')
                    
                    districts = []
                    for option in options:
                        value = option.get_attribute('value').strip()
                        text = option.text.strip()
                        
                        if value and value != "" and text != "- Chọn -":
                            districts.append({
                                'id': value,
                                'name': text,
                                'slug': self.create_district_slug(text)
                            })
                    
                    if districts:
                        logger.info(f"✅ Fallback method found {len(districts)} districts")
                        return districts
                        
                except Exception as e:
                    logger.debug(f"Selector {selector} failed: {e}")
                    continue
            
            logger.warning("⚠️ Fallback method also failed")
            return []
            
        except Exception as e:
            logger.error(f"❌ Fallback method error: {e}")
            return []

    def create_district_slug(self, district_name):
        """Create URL slug from district name"""
        # Convert Vietnamese district name to URL slug
        name = district_name.lower()
        
        # Replace Vietnamese characters
        vietnamese_chars = {
            'à': 'a', 'á': 'a', 'ạ': 'a', 'ả': 'a', 'ã': 'a',
            'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ậ': 'a', 'ẩ': 'a', 'ẫ': 'a',
            'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ặ': 'a', 'ẳ': 'a', 'ẵ': 'a',
            'è': 'e', 'é': 'e', 'ẹ': 'e', 'ẻ': 'e', 'ẽ': 'e',
            'ê': 'e', 'ề': 'e', 'ế': 'e', 'ệ': 'e', 'ể': 'e', 'ễ': 'e',
            'ì': 'i', 'í': 'i', 'ị': 'i', 'ỉ': 'i', 'ĩ': 'i',
            'ò': 'o', 'ó': 'o', 'ọ': 'o', 'ỏ': 'o', 'õ': 'o',
            'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ộ': 'o', 'ổ': 'o', 'ỗ': 'o',
            'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ợ': 'o', 'ở': 'o', 'ỡ': 'o',
            'ù': 'u', 'ú': 'u', 'ụ': 'u', 'ủ': 'u', 'ũ': 'u',
            'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ự': 'u', 'ử': 'u', 'ữ': 'u',
            'ỳ': 'y', 'ý': 'y', 'ỵ': 'y', 'ỷ': 'y', 'ỹ': 'y',
            'đ': 'd'
        }
        
        for vn_char, en_char in vietnamese_chars.items():
            name = name.replace(vn_char, en_char)
        
        # Remove prefixes and clean up
        name = re.sub(r'^(thành phố|thị xã|huyện|quận)\s+', '', name)
        name = re.sub(r'[^\w\s-]', '', name)
        name = re.sub(r'\s+', '-', name)
        name = name.strip('-')
        
        return name

    def extract_district_patterns(self, province_name, province_path, admin_code):
        """Extract patterns from all districts in a province"""
        logger.info(f"🏘️ Starting district-level extraction for {province_name}")
        
        district_data = {
            'province_name': province_name,
            'admin_code': admin_code,
            'timestamp': datetime.now().isoformat(),
            'districts': {},
            'total_districts': 0,
            'successful_districts': 0,
            'failed_districts': 0,
            'api_method_used': None
        }
        
        try:
            # First, visit the province page to get cookies/session
            province_url = f"https://guland.vn/{province_path}"
            logger.info(f"🔗 Visiting province page: {province_url}")
            self.driver.get(province_url)
            
            # Wait for page to fully load and JavaScript to execute
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(5)
            
            # Try to ensure we have a valid session
            try:
                # Trigger any dropdowns or interactions to initialize session
                page_source = self.driver.page_source
                if 'csrf' in page_source.lower() or 'token' in page_source.lower():
                    logger.info("✅ Page appears to have CSRF tokens")
            except:
                pass
            
            # Get districts via API with retry
            max_retries = 3
            districts = []
            
            for attempt in range(max_retries):
                logger.info(f"🔄 Attempt {attempt + 1}/{max_retries} to get districts")
                districts = self.get_districts_via_api(admin_code)
                
                if districts:
                    district_data['api_method_used'] = 'API_SUCCESS'
                    break
                elif attempt < max_retries - 1:
                    logger.info("⏳ Waiting before retry...")
                    time.sleep(3)
                else:
                    district_data['api_method_used'] = 'API_FAILED'
            
            district_data['total_districts'] = len(districts)
            
            if not districts:
                logger.warning(f"⚠️ No districts found for {province_name} after {max_retries} attempts")
                return district_data
            
            logger.info(f"📍 Processing {len(districts)} districts for {province_name}")
            
            # Process each district
            for i, district in enumerate(districts, 1):
                logger.info(f"🏘️ ({i}/{len(districts)}) Processing district: {district['name']}")
                
                try:
                    # Construct district URL
                    province_slug = province_path.split('/')[-1]
                    district_url = f"https://guland.vn/soi-quy-hoach/{province_slug}/{district['slug']}"
                    
                    logger.info(f"🔗 Visiting: {district_url}")
                    self.driver.get(district_url)
                    
                    # Wait for page load with better error handling
                    try:
                        WebDriverWait(self.driver, 15).until(
                            EC.presence_of_element_located((By.TAG_NAME, "body"))
                        )
                        time.sleep(6)  # Extra time for map initialization
                        
                        # Check if page loaded correctly
                        if "404" in self.driver.title.lower() or "not found" in self.driver.page_source.lower():
                            logger.warning(f"⚠️ District page not found: {district_url}")
                            district_data['failed_districts'] += 1
                            self.extracted_data['district_failure_count'] += 1
                            continue
                            
                    except Exception as e:
                        logger.warning(f"⚠️ Page load timeout for {district['name']}: {e}")
                        district_data['failed_districts'] += 1
                        self.extracted_data['district_failure_count'] += 1
                        continue
                    
                    # Extract patterns for this district
                    district_patterns = self.extract_map_patterns_from_html(f"{province_name} - {district['name']}")
                    
                    if district_patterns['total_patterns'] > 0:
                        district_data['districts'][district['name']] = {
                            'district_info': district,
                            'patterns': district_patterns,
                            'url': district_url
                        }
                        district_data['successful_districts'] += 1
                        self.extracted_data['district_success_count'] += 1
                        
                        # Log specific KH_2025 patterns for districts
                        kh_pattern = district_patterns['map_types'].get('KH_2025')
                        if kh_pattern and kh_pattern['tile_url']:
                            logger.info(f"✅ {district['name']} KH_2025: {kh_pattern['tile_url']}")
                        else:
                            logger.info(f"✅ {district['name']}: {district_patterns['total_patterns']} patterns found")
                    else:
                        district_data['failed_districts'] += 1
                        self.extracted_data['district_failure_count'] += 1
                        logger.warning(f"⚠️ No patterns found for {district['name']}")
                    
                    # Moderate delay between districts to avoid rate limiting
                    time.sleep(3)
                    
                except Exception as e:
                    logger.error(f"❌ Error processing district {district['name']}: {e}")
                    district_data['failed_districts'] += 1
                    self.extracted_data['district_failure_count'] += 1
                    continue
            
            # Save district data
            if district_data['successful_districts'] > 0:
                self.save_district_data(province_name, district_data)
            
            logger.info(f"✅ District extraction completed for {province_name}: "
                       f"{district_data['successful_districts']}/{district_data['total_districts']} successful")
            
            return district_data
            
        except Exception as e:
            logger.error(f"❌ Error in district extraction for {province_name}: {e}")
            return district_data

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

    def process_location(self, location_name, path, admin_code=None):
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
            time.sleep(8)
            
            # Extract province-level patterns
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
                
                # NEW: Extract district-level patterns for KH_2025
                if admin_code:
                    logger.info(f"🏘️ Starting district-level extraction for {location_name}")
                    district_data = self.extract_district_patterns(location_name, path, admin_code)
                    
                    if district_data['successful_districts'] > 0:
                        self.extracted_data['district_patterns'][location_name] = district_data
                        logger.info(f"✅ District extraction completed: {district_data['successful_districts']} districts")
                    else:
                        logger.warning(f"⚠️ No district patterns found for {location_name}")
                
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
        json_path = f"output_enhanced_patterns/provinces/{clean_name}_patterns.json"
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
        
        text_path = f"output_enhanced_patterns/provinces/{clean_name}_summary.txt"
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(text_report)

    def save_district_data(self, province_name, district_data):
        """Save district-level data"""
        clean_name = province_name.replace(' ', '_').replace('TP ', '')
        
        # JSON data
        json_path = f"output_enhanced_patterns/districts/{clean_name}_districts.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(district_data, f, indent=2, ensure_ascii=False)
        
        # Text summary
        text_report = f"""# DISTRICT PATTERNS: {province_name}
Generated: {district_data['timestamp']}
Admin Code: {district_data['admin_code']}
Method: District-level HTML extraction + API

## 📊 SUMMARY
• Total districts: {district_data['total_districts']}
• Successful: {district_data['successful_districts']}
• Failed: {district_data['failed_districts']}

## 🏘️ DISTRICT BREAKDOWN
"""
        
        for district_name, district_info in district_data['districts'].items():
            text_report += f"""
### {district_name}
• District ID: {district_info['district_info']['id']}
• URL: {district_info['url']}
• Patterns found: {district_info['patterns']['total_patterns']}

"""
            for map_type, data in district_info['patterns']['map_types'].items():
                if map_type == 'KH_2025':  # Focus on KH_2025 for districts
                    text_report += f"  • {data['display_name']}: {data['tile_url']}\n"
        
        text_path = f"output_enhanced_patterns/districts/{clean_name}_districts_summary.txt"
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(text_report)

    def generate_comprehensive_report(self):
        """Generate final comprehensive report including district data"""
        logger.info("📊 Generating enhanced comprehensive patterns report...")
        
        # Collect all unique patterns and servers
        all_tile_patterns = set()
        all_servers = set()
        district_tile_patterns = set()
        
        # Province-level patterns
        for location_data in self.extracted_data['all_locations']:
            for map_type, data in location_data['map_types'].items():
                if data['tile_url']:
                    all_tile_patterns.add(data['tile_url'])
                    if data['server']:
                        all_servers.add(data['server'])
                if data['tile_url_2030']:
                    all_tile_patterns.add(data['tile_url_2030'])
        
        # District-level patterns
        for province_name, district_data in self.extracted_data['district_patterns'].items():
            for district_name, district_info in district_data['districts'].items():
                for map_type, data in district_info['patterns']['map_types'].items():
                    if data['tile_url']:
                        district_tile_patterns.add(data['tile_url'])
                        all_tile_patterns.add(data['tile_url'])
                        if data['server']:
                            all_servers.add(data['server'])
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'extraction_method': 'Enhanced HTML parsing with district-level support',
            'summary': {
                'total_locations_processed': len(self.test_locations),
                'successful_extractions': self.extracted_data['success_count'],
                'failed_extractions': self.extracted_data['failure_count'],
                'success_rate': (self.extracted_data['success_count'] / len(self.test_locations) * 100) if len(self.test_locations) > 0 else 0,
                'total_patterns_found': self.extracted_data['total_patterns_found'],
                'unique_tile_urls': len(all_tile_patterns),
                'unique_servers': len(all_servers),
                # New district metrics
                'district_successful_extractions': self.extracted_data['district_success_count'],
                'district_failed_extractions': self.extracted_data['district_failure_count'],
                'unique_district_tile_urls': len(district_tile_patterns),
                'provinces_with_districts': len(self.extracted_data['district_patterns'])
            },
            'all_tile_patterns': list(all_tile_patterns),
            'district_tile_patterns': list(district_tile_patterns),
            'all_servers': list(all_servers),
            'location_breakdown': self.extracted_data['all_patterns'],
            'district_breakdown': self.extracted_data['district_patterns']
        }
        
        # Save JSON report
        json_path = 'output_enhanced_patterns/reports/enhanced_comprehensive_report.json'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_report = f"""# ENHANCED GULAND TILE PATTERNS REPORT
Generated: {report['timestamp']}
Method: Enhanced HTML parsing with district-level extraction

## 📊 EXTRACTION SUMMARY
### Province Level
• Locations processed: {report['summary']['successful_extractions']}/{report['summary']['total_locations_processed']}
• Success rate: {report['summary']['success_rate']:.1f}%
• Total patterns found: {report['summary']['total_patterns_found']}

### District Level (NEW!)
• Districts processed: {report['summary']['district_successful_extractions']}
• Failed districts: {report['summary']['district_failed_extractions']}
• Provinces with district data: {report['summary']['provinces_with_districts']}
• Unique district patterns: {report['summary']['unique_district_tile_urls']}

### Overall
• Total unique tile URLs: {report['summary']['unique_tile_urls']}
• Unique servers: {report['summary']['unique_servers']}

## 🗺️ ALL UNIQUE TILE PATTERNS
"""
        
        for pattern in sorted(report['all_tile_patterns']):
            text_report += f"• {pattern}\n"
        
        text_report += f"\n## 🏘️ DISTRICT-SPECIFIC PATTERNS (KH_2025)\n"
        for pattern in sorted(report['district_tile_patterns']):
            if pattern not in [p for p in report['all_tile_patterns'] if 'district' not in p]:
                text_report += f"• {pattern}\n"
        
        text_report += f"\n## 🏢 TILE SERVERS\n"
        for server in sorted(report['all_servers']):
            text_report += f"• {server}\n"
        
        text_report += f"\n## 📍 PROVINCE BREAKDOWN\n"
        for location, map_types in report['location_breakdown'].items():
            text_report += f"\n### {location}\n"
            for map_type, data in map_types.items():
                text_report += f"• {data['display_name']}: {data['tile_url']}\n"
        
        text_report += f"\n## 🏘️ DISTRICT BREAKDOWN\n"
        for province, district_data in report['district_breakdown'].items():
            text_report += f"\n### {province} ({district_data['successful_districts']} districts)\n"
            for district_name, district_info in district_data['districts'].items():
                text_report += f"  #### {district_name}\n"
                for map_type, data in district_info['patterns']['map_types'].items():
                    if map_type == 'KH_2025':
                        text_report += f"    • {data['display_name']}: {data['tile_url']}\n"
        
        text_path = 'output_enhanced_patterns/reports/enhanced_comprehensive_summary.txt'
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        logger.info("✅ Enhanced comprehensive report generated")
        
        # Print summary
        print(f"\n🎉 ENHANCED PATTERN EXTRACTION COMPLETED!")
        print("=" * 70)
        print(f"📊 Province-level Results:")
        print(f"  • Successful: {report['summary']['successful_extractions']}/{report['summary']['total_locations_processed']} locations")
        print(f"  • Success rate: {report['summary']['success_rate']:.1f}%")
        print(f"  • Total patterns: {report['summary']['total_patterns_found']}")
        print(f"\n🏘️ District-level Results (NEW!):")
        print(f"  • Districts processed: {report['summary']['district_successful_extractions']}")
        print(f"  • Provinces with districts: {report['summary']['provinces_with_districts']}")
        print(f"  • Unique district patterns: {report['summary']['unique_district_tile_urls']}")
        print(f"\n📊 Overall:")
        print(f"  • Total unique URLs: {report['summary']['unique_tile_urls']}")
        print(f"  • Servers: {report['summary']['unique_servers']}")
        print(f"\n📁 Reports saved to: output_enhanced_patterns/reports/")
        
        return report

    def run_extraction(self, max_locations=None, enable_district_extraction=True):
        """Run the complete enhanced pattern extraction process"""
        logger.info("🚀 STARTING ENHANCED HTML PATTERN EXTRACTION")
        logger.info("=" * 70)
        
        start_time = time.time()
        
        try:
            if not self.setup_driver():
                return None
            
            # Determine locations to process
            locations_to_process = self.test_locations[:max_locations] if max_locations else self.test_locations
            
            logger.info(f"🎯 Processing {len(locations_to_process)} locations")
            if enable_district_extraction:
                logger.info("🏘️ District-level extraction: ENABLED")
            else:
                logger.info("🏘️ District-level extraction: DISABLED")
            
            for i, location_tuple in enumerate(locations_to_process, 1):
                if len(location_tuple) == 3:
                    location_name, path, admin_code = location_tuple
                else:
                    location_name, path = location_tuple
                    admin_code = None
                
                logger.info(f"\n🌍 ({i}/{len(locations_to_process)}) Processing: {location_name}")
                
                if enable_district_extraction and admin_code:
                    self.process_location(location_name, path, admin_code)
                else:
                    self.process_location(location_name, path)
                
                # Small delay between locations
                if i < len(locations_to_process):
                    time.sleep(3)
            
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

    def test_api_call(self, admin_code):
        """Test API call independently to verify it works"""
        print(f"\n🧪 TESTING API CALL FOR ADMIN CODE: {admin_code}")
        print("=" * 50)
        
        try:
            url = f"https://guland.vn/get-sub-location?id={admin_code}&is_bds=1"
            
            # Test 1: Minimal headers (like Postman default)
            print("🔬 Test 1: Minimal headers")
            headers1 = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
            
            response1 = requests.get(url, headers=headers1, timeout=10)
            print(f"   Status: {response1.status_code}")
            
            if response1.status_code == 200:
                soup = BeautifulSoup(response1.text, 'html.parser')
                options = soup.find_all('option')
                count = len([opt for opt in options if opt.get('value', '').strip() and opt.get('value', '').strip() != ""])
                print(f"   Districts found: {count}")
                if count > 0:
                    print("   ✅ SUCCESS!")
                    return True
            
            # Test 2: Postman-like headers
            print("🔬 Test 2: Postman-like headers")
            headers2 = {
                'User-Agent': 'PostmanRuntime/7.32.3',
                'Accept': '*/*',
                'Cache-Control': 'no-cache'
            }
            
            response2 = requests.get(url, headers=headers2, timeout=10)
            print(f"   Status: {response2.status_code}")
            
            if response2.status_code == 200:
                soup = BeautifulSoup(response2.text, 'html.parser')
                options = soup.find_all('option')
                count = len([opt for opt in options if opt.get('value', '').strip() and opt.get('value', '').strip() != ""])
                print(f"   Districts found: {count}")
                if count > 0:
                    print("   ✅ SUCCESS!")
                    return True
            
            # Test 3: No headers
            print("🔬 Test 3: No special headers")
            response3 = requests.get(url, timeout=10)
            print(f"   Status: {response3.status_code}")
            
            if response3.status_code == 200:
                soup = BeautifulSoup(response3.text, 'html.parser')
                options = soup.find_all('option')
                count = len([opt for opt in options if opt.get('value', '').strip() and opt.get('value', '').strip() != ""])
                print(f"   Districts found: {count}")
                if count > 0:
                    print("   ✅ SUCCESS!")
                    return True
            
            print("   ❌ All tests failed")
            return False
            
        except Exception as e:
            print(f"   ❌ Test error: {e}")
            return False
    
def main():    
    """Main function"""
    print("🇻🇳 ENHANCED GULAND PATTERN EXTRACTOR")
    print("Province + District level extraction with KH_2025 district support")
    print("=" * 70)
    
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
    
    # District extraction option
    district_input = input("Enable district-level extraction? (Y/n): ").lower().strip()
    enable_districts = district_input not in ['n', 'no']
    
    # Browser mode
    headless_input = input("Run headless? (y/N): ").lower().strip()
    headless = headless_input in ['y', 'yes']
    
    print(f"\n🎯 ENHANCED EXTRACTION CONFIGURATION:")
    print(f"📍 Locations: {max_locations if max_locations else 'All (63)'}")
    print(f"🏘️ District extraction: {'Enabled' if enable_districts else 'Disabled'}")
    print(f"🖥️ Headless: {'Yes' if headless else 'No'}")
    print(f"⚡ Method: Enhanced HTML parsing + API calls")
    
    confirm = input("\nStart extraction? (Y/n): ").lower().strip()
    if confirm in ['n', 'no']:
        print("Cancelled.")
        return
    
    try:
        extractor = EnhancedGulandPatternExtractor(headless=headless)
        results = extractor.run_extraction(
            max_locations=max_locations,
            enable_district_extraction=enable_districts
        )
        
        if results:
            print("\n✅ SUCCESS! All tile patterns extracted")
            print("📁 Check 'output_enhanced_patterns/' for results")
            if enable_districts:
                print("🏘️ District-level data available in 'districts/' folder")
        else:
            print("\n❌ Extraction failed")
        
    except KeyboardInterrupt:
        print("\n⏹️ Extraction stopped by user")
    except Exception as e:
        print(f"\n❌ Extraction failed: {e}")

if __name__ == "__main__":
    main()