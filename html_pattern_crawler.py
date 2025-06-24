#!/usr/bin/env python3
"""
Enhanced Multi-Map Type Tile Downloader for Guland
Downloads tiles using patterns from HTML extractor with organized structure

Author: AI Assistant
Version: 2.0 - Multi Map Type Support
Folder Structure: downloaded_tiles/cities/<city>/<map_type>/<zoom>/
"""
import os
import json
import time
import logging
import requests
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import math
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_tile_downloader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Map type configurations
MAP_TYPE_CONFIG = {
    'QH_2030': {
        'folder_name': 'qh-2030',
        'display_name': 'QH 2030',
        'url_patterns': ['-2030', 'qh/'],
        'priority': 1
    },
    'KH_2025': {
        'folder_name': 'kh-2025', 
        'display_name': 'KH 2025',
        'url_patterns': ['qh-2025/', 'qh-2024/', 'kh-2025'],
        'priority': 2
    },
    'QH_PHAN_KHU': {
        'folder_name': 'qh-phan-khu',
        'display_name': 'QH phân khu',
        'url_patterns': ['qhc/', 'qhxd3/', 'phan-khu'],
        'priority': 3
    },
    'QH_KHAC': {
        'folder_name': 'qh-khac',
        'display_name': 'QH khác',
        'url_patterns': ['qhxd/', 'khac'],
        'priority': 4
    },
    'UNKNOWN': {
        'folder_name': 'unknown',
        'display_name': 'Unknown Type',
        'url_patterns': [],
        'priority': 99
    }
}

class EnhancedMultiMapTileDownloader:
    def __init__(self, max_workers=10, timeout=30, user_agent=None, enable_download=True):
        self.max_workers = max_workers
        self.timeout = timeout
        self.session = requests.Session()
        
        # Set realistic headers
        self.session.headers.update({
            'User-Agent': user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'image',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'cross-site'
        })
        
        # NEW folder structure: downloaded_tiles/cities/<city>/<map_type>/<zoom>/
        self.base_download_dir = 'downloaded_tiles'
        self.base_output_dir = 'multi_map_verification'
        
        # Create base directories
        os.makedirs(self.base_download_dir, exist_ok=True)
        os.makedirs(f'{self.base_download_dir}/cities', exist_ok=True)
        os.makedirs(self.base_output_dir, exist_ok=True)
        os.makedirs(f'{self.base_output_dir}/reports', exist_ok=True)
        
        # Statistics by map type
        self.stats = {
            'total_attempted': 0,
            'total_successful': 0,
            'total_failed': 0,
            'total_bytes': 0,
            'patterns_tested': 0,
            'valid_patterns': 0,
            'map_type_stats': {}  # NEW: Track by map type
        }
        self.stats_lock = threading.Lock()
        
        self.enable_download = enable_download
            
        logger.info(f"🔍 Enhanced multi-map tile downloader initialized")
        logger.info(f"👥 Workers: {self.max_workers}, Timeout: {self.timeout}s")
        logger.info(f"📥 Download enabled: {enable_download}")
        logger.info(f"📁 NEW Structure: downloaded_tiles/cities/<city>/<map_type>/<zoom>/")
        
        # NEW: Store district data for KH_2025
        self.district_data = {}
        self.load_district_data()
    
    def load_district_data(self):
        """Load district data from HTML extractor district results"""
        try:
            # Load from enhanced HTML extractor district outputs
            districts_dir = Path('output_enhanced_patterns/districts')
            if not districts_dir.exists():
                logger.warning("⚠️ No district data found from HTML extractor")
                return
            
            district_files = list(districts_dir.glob('*_districts.json'))
            
            for district_file in district_files:
                try:
                    with open(district_file, 'r', encoding='utf-8') as f:
                        district_data = json.load(f)
                    
                    province_name = district_data.get('province_name', '')
                    clean_city_name = self.clean_city_name(province_name)
                    
                    # Store district info for this city
                    if clean_city_name not in self.district_data:
                        self.district_data[clean_city_name] = {}
                    
                    districts_info = district_data.get('districts', {})
                    for district_name, district_info in districts_info.items():
                        patterns = district_info.get('patterns', {})
                        map_types = patterns.get('map_types', {})
                        
                        # Only store if has KH_2025 patterns
                        if 'KH_2025' in map_types:
                            clean_district_name = self.clean_district_name(district_name)
                            
                            self.district_data[clean_city_name][clean_district_name] = {
                                'original_name': district_name,
                                'district_info': district_info.get('district_info', {}),
                                'kh_2025_patterns': map_types['KH_2025']
                            }
                    
                    logger.info(f"📍 Loaded {len(self.district_data.get(clean_city_name, {}))} districts for {province_name}")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error loading district file {district_file}: {e}")
                    continue
            
            total_districts = sum(len(districts) for districts in self.district_data.values())
            logger.info(f"✅ Loaded district data: {len(self.district_data)} cities, {total_districts} districts with KH_2025")
            
        except Exception as e:
            logger.error(f"❌ Error loading district data: {e}")

    def clean_district_name(self, district_name):
        """Clean district name for folder creation - ENHANCED with proper district formatting"""
        original_name = district_name
        clean_name = district_name.lower().strip()
        
        # Vietnamese diacritics removal
        import unicodedata
        clean_name = unicodedata.normalize('NFD', clean_name)
        clean_name = ''.join(c for c in clean_name if unicodedata.category(c) != 'Mn')
        
        # Standardized district name mapping with proper format
        district_transformations = {
            # Quận patterns
            r'^quan\s+(\d+)$': r'quan-\1',                    # "Quận 1" -> "quan-1"
            r'^quan\s+(.+)$': r'quan-\1',                     # "Quận Bình Thạnh" -> "quan-binh-thanh"
            
            # Huyện patterns  
            r'^huyen\s+(.+)$': r'huyen-\1',                   # "Huyện Củ Chi" -> "huyen-cu-chi"
            
            # Thị xã patterns
            r'^thi\s+xa\s+(.+)$': r'thi-xa-\1',              # "Thị xã Thuận An" -> "thi-xa-thuan-an"
            
            # Thành phố patterns
            r'^thanh\s+pho\s+(.+)$': r'thanh-pho-\1',        # "Thành phố Thủ Đức" -> "thanh-pho-thu-duc"
            r'^tp\s+(.+)$': r'thanh-pho-\1',                 # "TP Thủ Đức" -> "thanh-pho-thu-duc"
        }
        
        # Apply transformations
        transformed = False
        for pattern, replacement in district_transformations.items():
            if re.match(pattern, clean_name):
                clean_name = re.sub(pattern, replacement, clean_name)
                transformed = True
                break
        
        # If no transformation applied, use the original cleaning logic
        if not transformed:
            # Remove common prefixes if they weren't handled above
            clean_name = re.sub(r'^(quan|huyen|thi xa|thanh pho|tp)\s+', '', clean_name)
        
        # Final cleanup
        clean_name = clean_name.replace(' ', '-')           # Spaces to hyphens
        clean_name = re.sub(r'[^\w-]', '', clean_name)      # Remove special chars except hyphens
        clean_name = re.sub(r'-+', '-', clean_name)         # Multiple hyphens to single
        clean_name = clean_name.strip('-')                  # Remove leading/trailing hyphens
        
        logger.debug(f"District name transformation: '{original_name}' -> '{clean_name}'")
        
        return clean_name

    def create_map_type_folder_structure(self, city_name, map_type, zoom_level, district_name=None):
        """Create folder structure - ENHANCED with district support for KH_2025"""
        clean_city_name = self.clean_city_name(city_name)
        map_config = MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])
        map_folder = map_config['folder_name']
        
        # NEW: Special handling for KH_2025 with district structure
        if map_type == 'KH_2025' and district_name:
            clean_district_name = self.clean_district_name(district_name)
            # Structure: downloaded_tiles/cities/<city>/kh-2025/<district>/<zoom>/
            city_path = Path(self.base_download_dir) / 'cities' / clean_city_name / map_folder / clean_district_name / str(zoom_level)
            logger.debug(f"📁 KH_2025 District structure: {city_path}")
        else:
            # Original structure for other map types: downloaded_tiles/cities/<city>/<map_type>/<zoom>/
            city_path = Path(self.base_download_dir) / 'cities' / clean_city_name / map_folder / str(zoom_level)
            logger.debug(f"📁 Standard structure: {city_path}")
        
        city_path.mkdir(parents=True, exist_ok=True)
        return str(city_path)

    def clean_city_name(self, city_name):
        """Clean city name for folder creation - ENHANCED with all Vietnamese provinces"""
        # Remove special characters and spaces
        clean_name = city_name.lower()
        clean_name = clean_name.replace(' ', '_')
        clean_name = clean_name.replace('tp_', '')
        clean_name = clean_name.replace('tp ', '')
        clean_name = clean_name.replace('-', '_')
        
        # Remove Vietnamese diacritics and normalize
        import unicodedata
        clean_name = unicodedata.normalize('NFD', clean_name)
        clean_name = ''.join(c for c in clean_name if unicodedata.category(c) != 'Mn')
        
        # Extended city mapping for ALL Vietnamese provinces/cities
        city_mapping = {
            # Major cities - All variations
            'ha_noi': 'hanoi',
            'hanoi': 'hanoi',
            'hà_nội': 'hanoi',
            'ho_chi_minh': 'hcm',
            'hcm': 'hcm',
            'saigon': 'hcm',
            'sài_gòn': 'hcm',
            'thành_phố_hồ_chí_minh': 'hcm',
            'da_nang': 'danang',
            'đà_nẵng': 'danang',
            'danang': 'danang',
            'hai_phong': 'haiphong',
            'hải_phòng': 'haiphong',
            'haiphong': 'haiphong',
            'can_tho': 'cantho',
            'cần_thơ': 'cantho',
            'cantho': 'cantho',
            
            # All Vietnamese provinces/cities with diacritics
            'dong_nai': 'dongnai',
            'đồng_nai': 'dongnai',
            'dongnai': 'dongnai',
            
            'ba_ria_vung_tau': 'baria_vungtau',
            'bà_rịa_vũng_tàu': 'baria_vungtau',
            'baria_vungtau': 'baria_vungtau',
            'vung_tau': 'baria_vungtau',
            'vũng_tàu': 'baria_vungtau',
            
            'an_giang': 'angiang',
            'ân_giang': 'angiang',
            'angiang': 'angiang',
            
            'bac_giang': 'bacgiang',
            'bắc_giang': 'bacgiang',
            'bacgiang': 'bacgiang',
            
            'bac_kan': 'backan',
            'bắc_kạn': 'backan',
            'backan': 'backan',
            
            'bac_lieu': 'baclieu',
            'bạc_liêu': 'baclieu',
            'baclieu': 'baclieu',
            
            'bac_ninh': 'bacninh',
            'bắc_ninh': 'bacninh',
            'bacninh': 'bacninh',
            
            'ben_tre': 'bentre',
            'bến_tre': 'bentre',
            'bentre': 'bentre',
            
            'binh_duong': 'binhduong',
            'bình_dương': 'binhduong',
            'binhduong': 'binhduong',
            
            'binh_phuoc': 'binhphuoc',
            'bình_phước': 'binhphuoc',
            'binhphuoc': 'binhphuoc',
            
            'binh_thuan': 'binhthuan',
            'bình_thuận': 'binhthuan',
            'binhthuan': 'binhthuan',
            
            'binh_dinh': 'binhdinh',
            'bình_định': 'binhdinh',
            'binhdinh': 'binhdinh',
            
            'ca_mau': 'camau',
            'cà_mau': 'camau',
            'camau': 'camau',
            
            'cao_bang': 'caobang',
            'cao_bằng': 'caobang',
            'caobang': 'caobang',
            
            'gia_lai': 'gialai',
            'gia_lai': 'gialai',
            'gialai': 'gialai',
            
            'ha_nam': 'hanam',
            'hà_nam': 'hanam',
            'hanam': 'hanam',
            
            'ha_giang': 'hagiang',
            'hà_giang': 'hagiang',
            'hagiang': 'hagiang',
            
            'ha_tinh': 'hatinh',
            'hà_tĩnh': 'hatinh',
            'hatinh': 'hatinh',
            
            'hai_duong': 'haiduong',
            'hải_dương': 'haiduong',
            'haiduong': 'haiduong',
            
            'hau_giang': 'haugiang',
            'hậu_giang': 'haugiang',
            'haugiang': 'haugiang',
            
            'hoa_binh': 'hoabinh',
            'hòa_bình': 'hoabinh',
            'hoabinh': 'hoabinh',
            
            'hung_yen': 'hungyen',
            'hưng_yên': 'hungyen',
            'hungyen': 'hungyen',
            
            'khanh_hoa': 'khanhhoa',
            'khánh_hòa': 'khanhhoa',
            'khanhhoa': 'khanhhoa',
            
            'kien_giang': 'kiengiang',
            'kiên_giang': 'kiengiang',
            'kiengiang': 'kiengiang',
            
            'kon_tum': 'kontum',
            'kon_tum': 'kontum',
            'kontum': 'kontum',
            
            'lai_chau': 'laichau',
            'lai_châu': 'laichau',
            'laichau': 'laichau',
            
            'lam_dong': 'lamdong',
            'lâm_đồng': 'lamdong',
            'lamdong': 'lamdong',
            
            'lang_son': 'langson',
            'lạng_sơn': 'langson',
            'langson': 'langson',
            
            'lao_cai': 'laocai',
            'lào_cai': 'laocai',
            'laocai': 'laocai',
            
            'long_an': 'longan',
            'long_an': 'longan',
            'longan': 'longan',
            
            'nam_dinh': 'namdinh',
            'nam_định': 'namdinh',
            'namdinh': 'namdinh',
            
            'nghe_an': 'nghean',
            'nghệ_an': 'nghean',
            'nghean': 'nghean',
            
            'ninh_binh': 'ninhbinh',
            'ninh_bình': 'ninhbinh',
            'ninhbinh': 'ninhbinh',
            
            'ninh_thuan': 'ninhthuan',
            'ninh_thuận': 'ninhthuan',
            'ninhthuan': 'ninhthuan',
            
            'phu_tho': 'phutho',
            'phú_thọ': 'phutho',
            'phutho': 'phutho',
            
            'phu_yen': 'phuyen',
            'phú_yên': 'phuyen',
            'phuyen': 'phuyen',
            
            'quang_binh': 'quangbinh',
            'quảng_bình': 'quangbinh',
            'quangbinh': 'quangbinh',
            
            'quang_nam': 'quangnam',
            'quảng_nam': 'quangnam',
            'quangnam': 'quangnam',
            
            'quang_ngai': 'quangngai',
            'quảng_ngãi': 'quangngai',
            'quangngai': 'quangngai',
            
            'quang_ninh': 'quangninh',
            'quảng_ninh': 'quangninh',
            'quangninh': 'quangninh',
            
            'quang_tri': 'quangtri',
            'quảng_trị': 'quangtri',
            'quangtri': 'quangtri',
            
            'soc_trang': 'soctrang',
            'sóc_trăng': 'soctrang',
            'soctrang': 'soctrang',
            
            'son_la': 'sonla',
            'sơn_la': 'sonla',
            'sonla': 'sonla',
            
            'tay_ninh': 'tayninh',
            'tây_ninh': 'tayninh',
            'tayninh': 'tayninh',
            
            'thai_binh': 'thaibinh',
            'thái_bình': 'thaibinh',
            'thaibinh': 'thaibinh',
            
            'thai_nguyen': 'thainguyen',
            'thái_nguyên': 'thainguyen',
            'thainguyen': 'thainguyen',
            
            'thanh_hoa': 'thanhhoa',
            'thanh_hóa': 'thanhhoa',
            'thanhhoa': 'thanhhoa',
            
            'thua_thien_hue': 'thuathienhue',
            'thừa_thiên_huế': 'thuathienhue',
            'thuathienhue': 'thuathienhue',
            'hue': 'thuathienhue',
            'huế': 'thuathienhue',
            
            'tien_giang': 'tiengiang',
            'tiền_giang': 'tiengiang',
            'tiengiang': 'tiengiang',
            
            'tra_vinh': 'travinh',
            'trà_vinh': 'travinh',
            'travinh': 'travinh',
            
            'tuyen_quang': 'tuyenquang',
            'tuyên_quang': 'tuyenquang',
            'tuyenquang': 'tuyenquang',
            
            'vinh_long': 'vinhlong',
            'vĩnh_long': 'vinhlong',
            'vinhlong': 'vinhlong',
            
            'vinh_phuc': 'vinhphuc',
            'vĩnh_phúc': 'vinhphuc',
            'vinhphuc': 'vinhphuc',
            
            'yen_bai': 'yenbai',
            'yên_bái': 'yenbai',
            'yenbai': 'yenbai',
            
            # Special cases with đ
            'dak_lak': 'daklak',
            'đak_lak': 'daklak',
            'đắk_lắk': 'daklak',
            'daklak': 'daklak',
            
            'dak_nong': 'daknong',
            'đak_nong': 'daknong',
            'đắk_nông': 'daknong',
            'daknong': 'daknong',
            
            'dien_bien': 'dienbien',
            'đien_bien': 'dienbien',
            'điện_biên': 'dienbien',
            'dienbien': 'dienbien',
            
            'dong_thap': 'dongthap',
            'đong_thap': 'dongthap',
            'đồng_tháp': 'dongthap',
            'dongthap': 'dongthap'
        }
        
        return city_mapping.get(clean_name, clean_name)

    def classify_map_type_from_url(self, url):
        """Classify map type from URL pattern"""
        url_lower = url.lower()
        
        # Check each map type pattern
        for map_type, config in MAP_TYPE_CONFIG.items():
            if map_type == 'UNKNOWN':
                continue
            
            for pattern in config['url_patterns']:
                if pattern in url_lower:
                    return map_type
        
        # Fallback classification
        if '2030' in url_lower:
            return 'QH_2030'
        elif '2025' in url_lower or '2024' in url_lower:
            return 'KH_2025'
        else:
            return 'UNKNOWN'

    def download_single_tile_with_map_type(self, tile_info, city_name, map_type, district_name=None):
        """Download single tile - ENHANCED with district support"""
        try:
            url = tile_info['url']
            zoom = tile_info['zoom']
            x = tile_info['x']
            y = tile_info['y']
            
            # Detect format
            if '.png' in url.lower():
                format_ext = 'png'
            elif '.jpg' in url.lower() or '.jpeg' in url.lower():
                format_ext = 'jpg'
            elif '.webp' in url.lower():
                format_ext = 'webp'
            else:
                format_ext = 'png'
            
            # Create folder structure with optional district
            folder_path = self.create_map_type_folder_structure(city_name, map_type, zoom, district_name)
            
            filename = f"{x}_{y}.{format_ext}"
            filepath = os.path.join(folder_path, filename)
            
            # Skip if exists
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                logger.debug(f"⏭️ File exists: {filename} ({file_size} bytes)")
                
                # Update stats
                with self.stats_lock:
                    if map_type not in self.stats['map_type_stats']:
                        self.stats['map_type_stats'][map_type] = {'attempted': 0, 'successful': 0, 'bytes': 0}
                    self.stats['map_type_stats'][map_type]['successful'] += 1
                    self.stats['map_type_stats'][map_type]['bytes'] += file_size
                
                return {
                    'success': True,
                    'filepath': filepath,
                    'size': file_size,
                    'tile_info': tile_info,
                    'map_type': map_type,
                    'district_name': district_name,  # NEW
                    'status': 'already_exists'
                }
            
            # Download tile
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                if any(img_type in content_type for img_type in ['image/', 'application/octet-stream']):
                    size = len(response.content)
                    
                    if size > 100:
                        # Save file
                        with open(filepath, 'wb') as f:
                            f.write(response.content)
                        
                        # Update stats
                        with self.stats_lock:
                            self.stats['total_successful'] += 1
                            self.stats['total_bytes'] += size
                            
                            if map_type not in self.stats['map_type_stats']:
                                self.stats['map_type_stats'][map_type] = {'attempted': 0, 'successful': 0, 'bytes': 0}
                            self.stats['map_type_stats'][map_type]['successful'] += 1
                            self.stats['map_type_stats'][map_type]['bytes'] += size
                        
                        district_log = f" -> {district_name}" if district_name else ""
                        logger.debug(f"✅ Downloaded: {filename} ({size} bytes) -> {map_type}{district_log}")
                        
                        return {
                            'success': True,
                            'filepath': filepath,
                            'size': size,
                            'tile_info': tile_info,
                            'content_type': content_type,
                            'map_type': map_type,
                            'district_name': district_name,  # NEW
                            'status': 'downloaded'
                        }
                
                # Invalid content
                with self.stats_lock:
                    self.stats['total_failed'] += 1
                    if map_type not in self.stats['map_type_stats']:
                        self.stats['map_type_stats'][map_type] = {'attempted': 0, 'successful': 0, 'bytes': 0}
                    self.stats['map_type_stats'][map_type]['attempted'] += 1
                
                return {
                    'success': False,
                    'reason': f'Invalid content type: {content_type}',
                    'tile_info': tile_info,
                    'map_type': map_type,
                    'district_name': district_name
                }
            else:
                # HTTP error
                with self.stats_lock:
                    self.stats['total_failed'] += 1
                    if map_type not in self.stats['map_type_stats']:
                        self.stats['map_type_stats'][map_type] = {'attempted': 0, 'successful': 0, 'bytes': 0}
                    self.stats['map_type_stats'][map_type]['attempted'] += 1
                
                return {
                    'success': False,
                    'reason': f'HTTP {response.status_code}',
                    'tile_info': tile_info,
                    'map_type': map_type,
                    'district_name': district_name
                }
                
        except Exception as e:
            with self.stats_lock:
                self.stats['total_failed'] += 1
                if map_type not in self.stats['map_type_stats']:
                    self.stats['map_type_stats'][map_type] = {'attempted': 0, 'successful': 0, 'bytes': 0}
                self.stats['map_type_stats'][map_type]['attempted'] += 1
            return {
                'success': False,
                'reason': f'Error: {str(e)}',
                'tile_info': tile_info,
                'map_type': map_type,
                'district_name': district_name
            }

    def download_tiles_batch_with_map_types(self, tile_urls, city_name, map_type, district_name=None):
        """Download batch of tiles - ENHANCED with district support"""
        if not tile_urls:
            return []
        
        map_display = MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])['display_name']
        district_log = f" - {district_name}" if district_name else ""
        logger.info(f"📥 Downloading {len(tile_urls)} tiles for {city_name}{district_log} - {map_display}")
        
        results = []
        
        # Download tiles in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_tile = {
                executor.submit(self.download_single_tile_with_map_type, tile_info, city_name, map_type, district_name): tile_info 
                for tile_info in tile_urls
            }
            
            for future in as_completed(future_to_tile):
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Log progress every 50 tiles
                    if len(results) % 50 == 0:
                        successful = len([r for r in results if r['success']])
                        logger.info(f"📊 Progress {map_display}{district_log}: {len(results)}/{len(tile_urls)} ({successful} successful)")
                        
                except Exception as e:
                    tile_info = future_to_tile[future]
                    logger.error(f"❌ Error processing tile {tile_info.get('x', '?')},{tile_info.get('y', '?')}: {e}")
                    results.append({
                        'success': False,
                        'reason': f'Processing error: {str(e)}',
                        'tile_info': tile_info,
                        'map_type': map_type,
                        'district_name': district_name
                    })
        
        # Update global stats
        successful = len([r for r in results if r['success']])
        failed = len([r for r in results if not r['success']])
        
        with self.stats_lock:
            self.stats['total_attempted'] += len(tile_urls)
        
        logger.info(f"📊 Batch complete {map_display}{district_log}: {successful}/{len(tile_urls)} successful")
        
        return results

    def crawl_pattern_for_city_and_map_type(self, pattern, city_coverage, city_name, map_type, district_name=None):
        """Crawl with district support for KH_2025"""
        all_tiles = []
        
        map_display = MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])['display_name']
        district_log = f" - {district_name}" if district_name else ""
        logger.info(f"🔍 {city_name}{district_log} - {map_display}")
        
        for zoom, coverage in city_coverage.items():
            logger.info(f"  Zoom {zoom}: X({coverage['x_min']}-{coverage['x_max']}), Y({coverage['y_min']}-{coverage['y_max']})")
            
            # Generate ALL coordinates in city coverage
            all_coordinates = []
            for x in range(coverage['x_min'], coverage['x_max'] + 1):
                for y in range(coverage['y_min'], coverage['y_max'] + 1):
                    all_coordinates.append((x, y))
            
            logger.info(f"📊 Trying {len(all_coordinates)} coordinates for zoom {zoom}")
            
            # Generate URLs for this zoom level
            zoom_urls = []
            for x, y in all_coordinates:
                url = pattern.replace('{z}', str(zoom))
                url = url.replace('{x}', str(x))
                url = url.replace('{y}', str(y))
                
                zoom_urls.append({
                    'url': url,
                    'zoom': zoom,
                    'x': x,
                    'y': y,
                    'pattern': pattern
                })
            
            # Process in batches with district support
            batch_size = 100
            zoom_successful = 0
            zoom_total = 0
            
            for i in range(0, len(zoom_urls), batch_size):
                batch = zoom_urls[i:i+batch_size]
                
                logger.info(f"📦 Processing {map_display}{district_log} batch {i//batch_size + 1}/{(len(zoom_urls)-1)//batch_size + 1}")
                
                # Download tiles with map type and district structure
                batch_results = self.download_tiles_batch_with_map_types(batch, city_name, map_type, district_name)
                
                # Count results
                successful_in_batch = len([r for r in batch_results if r.get('success')])
                zoom_successful += successful_in_batch
                zoom_total += len(batch)
                
                # Add successful tiles only
                all_tiles.extend([r for r in batch_results if r.get('success')])
                
                if successful_in_batch > 0:
                    logger.info(f"✅ Found {successful_in_batch}/{len(batch)} tiles in batch")
                
                # Short delay
                time.sleep(0.1)
            
            logger.info(f"📊 Zoom {zoom} final: {zoom_successful}/{zoom_total} tiles ({zoom_successful/zoom_total*100:.1f}%)")
        
        return all_tiles

    def crawl_multi_map_patterns(self, zoom_levels=[10, 12, 14], use_html_source=True, skip_existing=True, target_map_types=None):
        """Enhanced crawling with KH_2025 district support"""
        
        # COMPLETE Vietnam city coordinates (lat, lng, radius_km)
        city_coords = {
        # Major cities - Extra large radius
            'hanoi': (21.0285, 105.8542, 150),      # Hà Nội + vùng phụ cận
            'hcm': (10.8231, 106.6297, 200),       # HCM + toàn bộ vùng Đông Nam Bộ
            'danang': (16.0544563, 108.0717219, 120), # Đà Nẵng + vùng miền Trung
            'haiphong': (20.8449, 106.6881, 100),  # Hải Phòng + vùng ven biển
            'cantho': (10.0452, 105.7469, 120),    # Cần Thơ + ĐBSCL
            
            # All provinces - Large radius for complete coverage
            'dongnai': (11.0686, 107.1676, 150),
            'baria_vungtau': (10.5417, 107.2431, 100),
            'angiang': (10.3889, 105.4359, 120),
            'bacgiang': (21.2731, 106.1946, 100),
            'backan': (22.1474, 105.8348, 120),
            'baclieu': (9.2515, 105.7244, 100),
            'bacninh': (21.1861, 106.0763, 80),
            'bentre': (10.2433, 106.3756, 100),
            'binhduong': (11.3254, 106.4770, 120),
            'binhphuoc': (11.7511, 106.7234, 150),
            'binhthuan': (11.0904, 108.0721, 150),
            'binhdinh': (13.7757, 109.2219, 120),
            'camau': (9.1769, 105.1524, 150),       # Cà Mau - southernmost
            'caobang': (22.6666, 106.2639, 120),
            'gialai': (13.8078, 108.1094, 180),     # Gia Lai - tỉnh lớn
            'hanam': (20.5835, 105.9230, 80),
            'hagiang': (22.8025, 104.9784, 150),    # Hà Giang - northernmost
            'hatinh': (18.3560, 105.9069, 120),
            'haiduong': (20.9373, 106.3146, 100),  # Hải Dương - gần Hà Nội
            'haugiang': (9.7571, 105.6412, 100),
            'hoabinh': (20.8156, 105.3373, 150),
            'hungyen': (20.6464, 106.0511, 80),
            'khanhhoa': (12.2388, 109.1967, 120),
            'kiengiang': (10.0125, 105.0808, 200),  # Kiên Giang - có Phú Quốc
            'kontum': (14.3497, 108.0005, 150),
            'laichau': (22.3856, 103.4707, 150),
            'lamdong': (11.5753, 108.1429, 150),    # Lâm Đồng - cao nguyên
            'langson': (21.8537, 106.7610, 120),
            'laocai': (22.4809, 103.9755, 150),     # Lào Cai - có Sa Pa
            'longan': (10.6957, 106.2431, 100),
            'namdinh': (20.4341, 106.1675, 100),
            'nghean': (18.6745, 105.6905, 200),     # Nghệ An - tỉnh lớn nhất
            'ninhbinh': (20.2506, 105.9744, 100),
            'ninhthuan': (11.5645, 108.9899, 120),
            'phutho': (21.4208, 105.2045, 120),
            'phuyen': (13.0882, 109.0929, 100),
            'quangbinh': (17.4809, 106.6238, 150),
            'quangnam': (15.5394, 108.0191, 150),
            'quangngai': (15.1214, 108.8044, 120),
            'quangninh': (21.0064, 107.2925, 150),  # Quảng Ninh - có Hạ Long
            'quangtri': (16.7404, 107.1854, 100),
            'soctrang': (9.6002, 105.9800, 100),
            'sonla': (21.3256, 103.9188, 200),      # Sơn La - tỉnh lớn thứ 2
            'tayninh': (11.3100, 106.0989, 120),
            'thaibinh': (20.4500, 106.3400, 80),
            'thainguyen': (21.5944, 105.8480, 120),
            'thanhhoa': (19.8069, 105.7851, 180),   # Thanh Hóa - tỉnh lớn
            'thuathienhue': (16.4674, 107.5905, 120),
            'tiengiang': (10.4493, 106.3420, 100),
            'travinh': (9.9477, 106.3524, 100),
            'tuyenquang': (21.8267, 105.2280, 120),
            'vinhlong': (10.2397, 105.9571, 100),
            'vinhphuc': (21.3609, 105.6049, 100),
            'yenbai': (21.7168, 104.8986, 120),
            'daklak': (12.7100, 108.2378, 180),     # Đắk Lắk - tỉnh lớn Tây Nguyên
            'daknong': (12.2646, 107.6098, 150),
            'dienbien': (21.3847, 103.0175, 150),
            'dongthap': (10.4938, 105.6881, 120)
        }
        
        # Load patterns grouped by city and map type
        patterns_by_city_and_type = self.load_patterns_from_html_extractor(use_html_source)
        
        if not patterns_by_city_and_type:
            logger.error("❌ No patterns found! Run HTML extractor first.")
            return []

        # Filter target map types
        if target_map_types:
            logger.info(f"🎯 Target map types: {[MAP_TYPE_CONFIG[mt]['display_name'] for mt in target_map_types]}")
        else:
            target_map_types = list(MAP_TYPE_CONFIG.keys())
            target_map_types.remove('UNKNOWN')

        all_results = []
        
        for city_name, city_map_patterns in patterns_by_city_and_type.items():
            if city_name not in city_coords:
                logger.info(f"⚠️ Skipping {city_name} - coordinates not configured")
                continue
            
            lat, lng, radius_km = city_coords[city_name]
            logger.info(f"\n🏙️ CRAWLING CITY: {city_name.upper()}")
            logger.info(f"📍 Center: {lat}, {lng} (radius: {radius_km}km)")
            
            # Generate city coverage
            city_coverage = self.generate_city_tile_coverage(lat, lng, zoom_levels, radius_km)
            
            city_results = {
                'city': city_name,
                'coordinates': (lat, lng, radius_km),
                'coverage': city_coverage,
                'map_type_results': {},
                'total_tiles': 0,
                'successful_tiles': 0
            }
            
            # Process each map type for this city
            for map_type, patterns_list in city_map_patterns.items():
                if map_type not in target_map_types:
                    logger.info(f"⏭️ Skipping {MAP_TYPE_CONFIG.get(map_type, {}).get('display_name', map_type)} - not in target list")
                    continue
                
                map_display = MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])['display_name']
                logger.info(f"🗺️ Processing {map_display}: {len(patterns_list)} patterns")
                
                # NEW: Special handling for KH_2025 with districts
                if map_type == 'KH_2025' and city_name in self.district_data:
                    logger.info(f"🏘️ KH_2025 detected - using district-level structure")
                    
                    districts = self.district_data[city_name]
                    logger.info(f"📍 Found {len(districts)} districts with KH_2025 data")
                    
                    city_results['map_type_results'][map_type] = {
                        'map_type': map_type,
                        'display_name': map_display,
                        'patterns': patterns_list,
                        'districts': {},
                        'total_tiles': 0,
                        'successful_tiles': 0,
                        'folder_structure': f"downloaded_tiles/cities/{self.clean_city_name(city_name)}/{MAP_TYPE_CONFIG[map_type]['folder_name']}/<district>/<zoom>/"
                    }
                    
                    # Process each district
                    for district_name, district_info in districts.items():
                        logger.info(f"🏘️ Processing district: {district_info['original_name']}")
                        
                        # Check if this district already downloaded
                        if skip_existing:
                            already_downloaded, status_msg = self.check_city_district_map_type_downloaded(city_name, map_type, district_name)
                            if already_downloaded:
                                logger.info(f"⏭️ SKIPPING {district_info['original_name']}: {status_msg}")
                                continue
                        
                        # Use district-specific patterns if available, otherwise use city patterns
                        district_patterns = district_info.get('kh_2025_patterns', {}).get('tile_url')
                        if district_patterns:
                            district_pattern_list = [district_patterns]
                        else:
                            district_pattern_list = patterns_list
                        
                        district_tiles = []
                        
                        # Process each pattern for this district
                        for pattern in district_pattern_list:
                            logger.info(f"🚀 Crawling district pattern: {pattern}")
                            
                            # Crawl with district structure
                            pattern_tiles = self.crawl_pattern_for_city_and_map_type(
                                pattern, city_coverage, city_name, map_type, district_info['original_name']
                            )
                            
                            if pattern_tiles:
                                district_tiles.extend(pattern_tiles)
                        
                        if district_tiles:
                            successful_count = len([t for t in district_tiles if t.get('success')])
                            
                            city_results['map_type_results'][map_type]['districts'][district_name] = {
                                'district_name': district_info['original_name'],
                                'tiles': district_tiles,
                                'total_tiles': len(district_tiles),
                                'successful_tiles': successful_count,
                                'patterns': district_pattern_list
                            }
                            
                            city_results['map_type_results'][map_type]['total_tiles'] += len(district_tiles)
                            city_results['map_type_results'][map_type]['successful_tiles'] += successful_count
                            
                            logger.info(f"✅ {district_info['original_name']}: {successful_count}/{len(district_tiles)} tiles")
                        else:
                            logger.warning(f"⚠️ No tiles found for district {district_info['original_name']}")
                    
                    # Update city totals
                    city_results['total_tiles'] += city_results['map_type_results'][map_type]['total_tiles']
                    city_results['successful_tiles'] += city_results['map_type_results'][map_type]['successful_tiles']
                    
                else:
                    # EXISTING: Standard processing for other map types
                    # Check if this map type already exists
                    if skip_existing:
                        already_downloaded, status_msg = self.check_city_map_type_downloaded(city_name, map_type)
                        if already_downloaded:
                            logger.info(f"⏭️ SKIPPING {city_name} - {map_display}: {status_msg}")
                            continue
                    
                    map_type_tiles = []
                    
                    # Process each pattern for this map type
                    for pattern in patterns_list:
                        logger.info(f"🚀 Crawling pattern: {pattern}")
                        
                        # Crawl with standard structure
                        pattern_tiles = self.crawl_pattern_for_city_and_map_type(
                            pattern, city_coverage, city_name, map_type
                        )
                        
                        if pattern_tiles:
                            map_type_tiles.extend(pattern_tiles)
                    
                    if map_type_tiles:
                        successful_count = len([t for t in map_type_tiles if t.get('success')])
                        
                        city_results['map_type_results'][map_type] = {
                            'map_type': map_type,
                            'display_name': map_display,
                            'patterns': patterns_list,
                            'tiles': map_type_tiles,
                            'total_tiles': len(map_type_tiles),
                            'successful_tiles': successful_count,
                            'folder_structure': f"downloaded_tiles/cities/{self.clean_city_name(city_name)}/{MAP_TYPE_CONFIG[map_type]['folder_name']}/<zoom>/"
                        }
                        
                        city_results['total_tiles'] += len(map_type_tiles)
                        city_results['successful_tiles'] += successful_count
                        
                        logger.info(f"✅ {map_display}: {successful_count}/{len(map_type_tiles)} tiles")
                    else:
                        logger.warning(f"⚠️ No tiles found for {map_display}")
            
            if city_results['map_type_results']:
                all_results.append(city_results)
                logger.info(f"✅ {city_name} complete: {city_results['successful_tiles']}/{city_results['total_tiles']} total tiles")
            else:
                logger.warning(f"⚠️ No results for {city_name}")
        
        return all_results

    def check_city_district_map_type_downloaded(self, city_name, map_type, district_name):
        """Check if specific city + district + map type already downloaded"""
        clean_city_name = self.clean_city_name(city_name)
        clean_district_name = self.clean_district_name(district_name)
        map_folder = MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])['folder_name']
        
        # District structure: downloaded_tiles/cities/<city>/kh-2025/<district>/
        city_district_path = Path(self.base_download_dir) / 'cities' / clean_city_name / map_folder / clean_district_name
        
        if not city_district_path.exists():
            return False, "No district download folder found"
        
        # Check if any zoom folders exist with tiles
        zoom_folders = [d for d in city_district_path.iterdir() if d.is_dir() and d.name.isdigit()]
        
        if not zoom_folders:
            return False, "No zoom folders found in district"
        
        # Count total tiles
        total_tiles = 0
        for zoom_folder in zoom_folders:
            tile_files = list(zoom_folder.glob('*.*'))
            total_tiles += len(tile_files)
    
        if total_tiles == 0:
            return False, "District zoom folders exist but no tiles found"
    
        return True, f"Found {total_tiles:,} tiles in {len(zoom_folders)} zoom levels for district {district_name}"

    def deg2num(self, lat_deg, lon_deg, zoom):
        """Convert lat/lon to tile coordinates"""
        lat_rad = math.radians(lat_deg)
        n = 2.0 ** zoom
        x = int((lon_deg + 180.0) / 360.0 * n)
        y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return (x, y)

    def generate_city_tile_coverage(self, lat, lng, zoom_levels, radius_km=20):
        """Generate tile coverage for specific city - OPTIMIZED"""
        city_coverages = {}
        
        for zoom in zoom_levels:
            # Calculate tile coordinates for city center
            center_x, center_y = self.deg2num(lat, lng, zoom)
            
            # Calculate radius in tiles - more accurate calculation
            lat_rad = math.radians(lat)
            meters_per_pixel = 156543.03392 * math.cos(lat_rad) / (2 ** zoom)
            pixels_per_tile = 256
            meters_per_tile = meters_per_pixel * pixels_per_tile
            radius_tiles = int((radius_km * 1000) / meters_per_tile)
            
            # Ensure reasonable coverage
            radius_tiles = max(radius_tiles, 5)   # At least 5 tiles radius
            radius_tiles = min(radius_tiles, 50)  # Max 50 tiles radius to avoid too many tiles
            
            city_coverages[zoom] = {
                'center_x': center_x,
                'center_y': center_y,
                'x_min': center_x - radius_tiles,
                'x_max': center_x + radius_tiles,
                'y_min': center_y - radius_tiles,
                'y_max': center_y + radius_tiles,
                'radius_tiles': radius_tiles,
                'total_tiles': (2 * radius_tiles + 1) ** 2
            }
            
            logger.info(f"  Zoom {zoom}: Center({center_x},{center_y}), Radius={radius_tiles} tiles, Total={city_coverages[zoom]['total_tiles']} tiles")
        
        return city_coverages

    def crawl_pattern_for_city_and_map_type(self, pattern, city_coverage, city_name, map_type, district_name=None):
        """Exhaustive crawl with NEW multi-map folder structure"""
        all_tiles = []
        
        map_display = MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])['display_name']
        district_log = f" - {district_name}" if district_name else ""
        logger.info(f"🔍 {city_name}{district_log} - {map_display}")
        
        for zoom, coverage in city_coverage.items():
            logger.info(f"  Zoom {zoom}: X({coverage['x_min']}-{coverage['x_max']}), Y({coverage['y_min']}-{coverage['y_max']})")
            
            # Generate ALL coordinates in city coverage
            all_coordinates = []
            for x in range(coverage['x_min'], coverage['x_max'] + 1):
                for y in range(coverage['y_min'], coverage['y_max'] + 1):
                    all_coordinates.append((x, y))
            
            logger.info(f"📊 Trying {len(all_coordinates)} coordinates for zoom {zoom}")
            
            # Generate URLs for this zoom level
            zoom_urls = []
            for x, y in all_coordinates:
                url = pattern.replace('{z}', str(zoom))
                url = url.replace('{x}', str(x))
                url = url.replace('{y}', str(y))
                
                zoom_urls.append({
                    'url': url,
                    'zoom': zoom,
                    'x': x,
                    'y': y,
                    'pattern': pattern
                })
            
            # Process in batches with district support
            batch_size = 100
            zoom_successful = 0
            zoom_total = 0
            
            for i in range(0, len(zoom_urls), batch_size):
                batch = zoom_urls[i:i+batch_size]
                
                logger.info(f"📦 Processing {map_display}{district_log} batch {i//batch_size + 1}/{(len(zoom_urls)-1)//batch_size + 1}")
                
                # Download tiles with map type and district structure
                batch_results = self.download_tiles_batch_with_map_types(batch, city_name, map_type, district_name)
                
                # Count results
                successful_in_batch = len([r for r in batch_results if r.get('success')])
                zoom_successful += successful_in_batch
                zoom_total += len(batch)
                
                # Add successful tiles only
                all_tiles.extend([r for r in batch_results if r.get('success')])
                
                if successful_in_batch > 0:
                    logger.info(f"✅ Found {successful_in_batch}/{len(batch)} tiles in batch")
                
                # Short delay
                time.sleep(0.1)
            
            logger.info(f"📊 Zoom {zoom} final: {zoom_successful}/{zoom_total} tiles ({zoom_successful/zoom_total*100:.1f}%)")
        
        return all_tiles

    def load_patterns_from_html_extractor(self, use_html_reports=True):
        """Load patterns from HTML extractor results - ENHANCED for multi-map support"""
        if use_html_reports:
            return self.load_patterns_from_html_reports()
        else:
            return self.load_patterns_from_browser_crawl_reports()

    def load_patterns_from_html_reports(self):
        """Load patterns from HTML extractor output"""
        patterns_by_city_and_type = {}
        
        # Try comprehensive HTML report first
        html_report_path = 'output_html_patterns/reports/comprehensive_patterns_report.json'
        
        try:
            with open(html_report_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            location_breakdown = data.get('location_breakdown', {})
            
            logger.info(f"📋 Loading patterns from HTML extractor: {len(location_breakdown)} locations")
            
            for location_name, map_types in location_breakdown.items():
                clean_city_name = self.clean_city_name(location_name)
                
                if clean_city_name not in patterns_by_city_and_type:
                    patterns_by_city_and_type[clean_city_name] = {}
                
                for map_type_key, map_data in map_types.items():
                    # Get tile patterns
                    tile_pattern = map_data.get('tile_url')
                    tile_pattern_2030 = map_data.get('tile_url_2030')
                    
                    patterns = []
                    if tile_pattern:
                        patterns.append(tile_pattern)
                    if tile_pattern_2030:
                        patterns.append(tile_pattern_2030)
                    
                    if patterns:
                        # Classify map type more accurately if needed
                        classified_type = self.classify_map_type_from_key_and_url(map_type_key, patterns[0])
                        
                        if classified_type not in patterns_by_city_and_type[clean_city_name]:
                            patterns_by_city_and_type[clean_city_name][classified_type] = []
                        
                        patterns_by_city_and_type[clean_city_name][classified_type].extend(patterns)
                        
                        map_display = MAP_TYPE_CONFIG.get(classified_type, MAP_TYPE_CONFIG['UNKNOWN'])['display_name']
                        logger.info(f"📋 {location_name} - {map_display}: {len(patterns)} patterns")
            
            # Remove duplicates
            for city in patterns_by_city_and_type:
                for map_type in patterns_by_city_and_type[city]:
                    patterns_by_city_and_type[city][map_type] = list(set(patterns_by_city_and_type[city][map_type]))
            
            logger.info(f"✅ Loaded patterns from HTML extractor for {len(patterns_by_city_and_type)} cities")
            return patterns_by_city_and_type
            
        except FileNotFoundError:
            logger.warning(f"⚠️ HTML extractor report not found at {html_report_path}")
            
        # Fallback: Load from individual city files
        logger.info("📋 Trying individual HTML pattern files...")
        
        html_locations_dir = Path('output_html_patterns/locations')
        if html_locations_dir.exists():
            pattern_files = list(html_locations_dir.glob('*_patterns.json'))
            
            for pattern_file in pattern_files:
                try:
                    with open(pattern_file, 'r', encoding='utf-8') as f:
                        location_data = json.load(f)
                    
                    location_name = location_data['location_name']
                    clean_city_name = self.clean_city_name(location_name)
                    map_types = location_data.get('map_types', {})
                    
                    if clean_city_name not in patterns_by_city_and_type:
                        patterns_by_city_and_type[clean_city_name] = {}
                    
                    for map_type_key, map_data in map_types.items():
                        tile_pattern = map_data.get('tile_url')
                        if tile_pattern:
                            classified_type = self.classify_map_type_from_key_and_url(map_type_key, tile_pattern)
                            
                            if classified_type not in patterns_by_city_and_type[clean_city_name]:
                                patterns_by_city_and_type[clean_city_name][classified_type] = []
                            
                            patterns_by_city_and_type[clean_city_name][classified_type].append(tile_pattern)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error loading {pattern_file}: {e}")
                    continue
            
            logger.info(f"✅ Loaded patterns from individual files for {len(patterns_by_city_and_type)} cities")
        
        return patterns_by_city_and_type

    def classify_map_type_from_key_and_url(self, map_type_key, url):
        """Enhanced classification using both key and URL"""
        # First try key-based classification
        key_mapping = {
            'QH_2030': 'QH_2030',
            'KH_2025': 'KH_2025', 
            'QH_PHAN_KHU': 'QH_PHAN_KHU',
            'QH_KHAC': 'QH_KHAC'
        }
        
        if map_type_key in key_mapping:
            return key_mapping[map_type_key]
        
        # Fallback to URL-based classification
        return self.classify_map_type_from_url(url)

    def load_patterns_from_browser_crawl_reports(self):
        """Fallback: Load from old browser crawl reports"""
        patterns_by_city_and_type = {}
        
        # Load from browser crawl final report
        final_report_path = 'output_browser_crawl/reports/final_patterns_report.json'
        
        try:
            with open(final_report_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            all_patterns = data.get('tile_patterns', [])
            successful_locations = data.get('successful_locations', [])
            
            # Group patterns by city and map type
            for location in successful_locations:
                location_name = location['location_name']
                clean_city_name = self.clean_city_name(location_name)
                discovered_patterns = location.get('discovered_patterns', [])
                
                if clean_city_name not in patterns_by_city_and_type:
                    patterns_by_city_and_type[clean_city_name] = {}
                
                # Classify each pattern
                for pattern in discovered_patterns:
                    map_type = self.classify_map_type_from_url(pattern)
                    
                    if map_type not in patterns_by_city_and_type[clean_city_name]:
                        patterns_by_city_and_type[clean_city_name][map_type] = []
                    
                    patterns_by_city_and_type[clean_city_name][map_type].append(pattern)
            
            logger.info(f"✅ Loaded patterns from browser crawl for {len(patterns_by_city_and_type)} cities")
            return patterns_by_city_and_type
            
        except FileNotFoundError:
            logger.error(f"❌ No pattern sources found!")
            return {}

    def crawl_multi_map_patterns(self, zoom_levels=[10, 12, 14], use_html_source=True, skip_existing=True, target_map_types=None):
        """Enhanced crawling with multi-map support"""
        
        # COMPLETE Vietnam city coordinates (lat, lng, radius_km)
        city_coords = {
        # Major cities - Extra large radius
            'hanoi': (21.0285, 105.8542, 150),      # Hà Nội + vùng phụ cận
            'hcm': (10.8231, 106.6297, 200),       # HCM + toàn bộ vùng Đông Nam Bộ
            'danang': (16.0544563, 108.0717219, 120), # Đà Nẵng + vùng miền Trung
            'haiphong': (20.8449, 106.6881, 100),  # Hải Phòng + vùng ven biển
            'cantho': (10.0452, 105.7469, 120),    # Cần Thơ + ĐBSCL
            
            # All provinces - Large radius for complete coverage
            'dongnai': (11.0686, 107.1676, 150),
            'baria_vungtau': (10.5417, 107.2431, 100),
            'angiang': (10.3889, 105.4359, 120),
            'bacgiang': (21.2731, 106.1946, 100),
            'backan': (22.1474, 105.8348, 120),
            'baclieu': (9.2515, 105.7244, 100),
            'bacninh': (21.1861, 106.0763, 80),
            'bentre': (10.2433, 106.3756, 100),
            'binhduong': (11.3254, 106.4770, 120),
            'binhphuoc': (11.7511, 106.7234, 150),
            'binhthuan': (11.0904, 108.0721, 150),
            'binhdinh': (13.7757, 109.2219, 120),
            'camau': (9.1769, 105.1524, 150),       # Cà Mau - southernmost
            'caobang': (22.6666, 106.2639, 120),
            'gialai': (13.8078, 108.1094, 180),     # Gia Lai - tỉnh lớn
            'hanam': (20.5835, 105.9230, 80),
            'hagiang': (22.8025, 104.9784, 150),    # Hà Giang - northernmost
            'hatinh': (18.3560, 105.9069, 120),
            'haiduong': (20.9373, 106.3146, 100),  # Hải Dương - gần Hà Nội
            'haugiang': (9.7571, 105.6412, 100),
            'hoabinh': (20.8156, 105.3373, 150),
            'hungyen': (20.6464, 106.0511, 80),
            'khanhhoa': (12.2388, 109.1967, 120),
            'kiengiang': (10.0125, 105.0808, 200),  # Kiên Giang - có Phú Quốc
            'kontum': (14.3497, 108.0005, 150),
            'laichau': (22.3856, 103.4707, 150),
            'lamdong': (11.5753, 108.1429, 150),    # Lâm Đồng - cao nguyên
            'langson': (21.8537, 106.7610, 120),
            'laocai': (22.4809, 103.9755, 150),     # Lào Cai - có Sa Pa
            'longan': (10.6957, 106.2431, 100),
            'namdinh': (20.4341, 106.1675, 100),
            'nghean': (18.6745, 105.6905, 200),     # Nghệ An - tỉnh lớn nhất
            'ninhbinh': (20.2506, 105.9744, 100),
            'ninhthuan': (11.5645, 108.9899, 120),
            'phutho': (21.4208, 105.2045, 120),
            'phuyen': (13.0882, 109.0929, 100),
            'quangbinh': (17.4809, 106.6238, 150),
            'quangnam': (15.5394, 108.0191, 150),
            'quangngai': (15.1214, 108.8044, 120),
            'quangninh': (21.0064, 107.2925, 150),  # Quảng Ninh - có Hạ Long
            'quangtri': (16.7404, 107.1854, 100),
            'soctrang': (9.6002, 105.9800, 100),
            'sonla': (21.3256, 103.9188, 200),      # Sơn La - tỉnh lớn thứ 2
            'tayninh': (11.3100, 106.0989, 120),
            'thaibinh': (20.4500, 106.3400, 80),
            'thainguyen': (21.5944, 105.8480, 120),
            'thanhhoa': (19.8069, 105.7851, 180),   # Thanh Hóa - tỉnh lớn
            'thuathienhue': (16.4674, 107.5905, 120),
            'tiengiang': (10.4493, 106.3420, 100),
            'travinh': (9.9477, 106.3524, 100),
            'tuyenquang': (21.8267, 105.2280, 120),
            'vinhlong': (10.2397, 105.9571, 100),
            'vinhphuc': (21.3609, 105.6049, 100),
            'yenbai': (21.7168, 104.8986, 120),
            'daklak': (12.7100, 108.2378, 180),     # Đắk Lắk - tỉnh lớn Tây Nguyên
            'daknong': (12.2646, 107.6098, 150),
            'dienbien': (21.3847, 103.0175, 150),
            'dongthap': (10.4938, 105.6881, 120)
        }
        
        # Load patterns grouped by city and map type
        patterns_by_city_and_type = self.load_patterns_from_html_extractor(use_html_source)
        
        if not patterns_by_city_and_type:
            logger.error("❌ No patterns found! Run HTML extractor first.")
            return []

        # Filter target map types
        if target_map_types:
            logger.info(f"🎯 Target map types: {[MAP_TYPE_CONFIG[mt]['display_name'] for mt in target_map_types]}")
        else:
            target_map_types = list(MAP_TYPE_CONFIG.keys())
            target_map_types.remove('UNKNOWN')

        all_results = []
        
        for city_name, city_map_patterns in patterns_by_city_and_type.items():
            if city_name not in city_coords:
                logger.info(f"⚠️ Skipping {city_name} - coordinates not configured")
                continue
            
            lat, lng, radius_km = city_coords[city_name]
            logger.info(f"\n🏙️ CRAWLING CITY: {city_name.upper()}")
            logger.info(f"📍 Center: {lat}, {lng} (radius: {radius_km}km)")
            
            # Generate city coverage
            city_coverage = self.generate_city_tile_coverage(lat, lng, zoom_levels, radius_km)
            
            city_results = {
                'city': city_name,
                'coordinates': (lat, lng, radius_km),
                'coverage': city_coverage,
                'map_type_results': {},
                'total_tiles': 0,
                'successful_tiles': 0
            }
            
            # Process each map type for this city
            for map_type, patterns_list in city_map_patterns.items():
                if map_type not in target_map_types:
                    logger.info(f"⏭️ Skipping {MAP_TYPE_CONFIG.get(map_type, {}).get('display_name', map_type)} - not in target list")
                    continue
                
                map_display = MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])['display_name']
                logger.info(f"🗺️ Processing {map_display}: {len(patterns_list)} patterns")
                
                # NEW: Special handling for KH_2025 with districts
                if map_type == 'KH_2025' and city_name in self.district_data:
                    logger.info(f"🏘️ KH_2025 detected - using district-level structure")
                    
                    districts = self.district_data[city_name]
                    logger.info(f"📍 Found {len(districts)} districts with KH_2025 data")
                    
                    city_results['map_type_results'][map_type] = {
                        'map_type': map_type,
                        'display_name': map_display,
                        'patterns': patterns_list,
                        'districts': {},
                        'total_tiles': 0,
                        'successful_tiles': 0,
                        'folder_structure': f"downloaded_tiles/cities/{self.clean_city_name(city_name)}/{MAP_TYPE_CONFIG[map_type]['folder_name']}/<district>/<zoom>/"
                    }
                    
                    # Process each district
                    for district_name, district_info in districts.items():
                        logger.info(f"🏘️ Processing district: {district_info['original_name']}")
                        
                        # Check if this district already downloaded
                        if skip_existing:
                            already_downloaded, status_msg = self.check_city_district_map_type_downloaded(city_name, map_type, district_name)
                            if already_downloaded:
                                logger.info(f"⏭️ SKIPPING {district_info['original_name']}: {status_msg}")
                                continue
                        
                        # Use district-specific patterns if available, otherwise use city patterns
                        district_patterns = district_info.get('kh_2025_patterns', {}).get('tile_url')
                        if district_patterns:
                            district_pattern_list = [district_patterns]
                        else:
                            district_pattern_list = patterns_list
                        
                        district_tiles = []
                        
                        # Process each pattern for this district
                        for pattern in district_pattern_list:
                            logger.info(f"🚀 Crawling district pattern: {pattern}")
                            
                            # Crawl with district structure
                            pattern_tiles = self.crawl_pattern_for_city_and_map_type(
                                pattern, city_coverage, city_name, map_type, district_info['original_name']
                            )
                            
                            if pattern_tiles:
                                district_tiles.extend(pattern_tiles)
                        
                        if district_tiles:
                            successful_count = len([t for t in district_tiles if t.get('success')])
                            
                            city_results['map_type_results'][map_type]['districts'][district_name] = {
                                'district_name': district_info['original_name'],
                                'tiles': district_tiles,
                                'total_tiles': len(district_tiles),
                                'successful_tiles': successful_count,
                                'patterns': district_pattern_list
                            }
                            
                            city_results['map_type_results'][map_type]['total_tiles'] += len(district_tiles)
                            city_results['map_type_results'][map_type]['successful_tiles'] += successful_count
                            
                            logger.info(f"✅ {district_info['original_name']}: {successful_count}/{len(district_tiles)} tiles")
                        else:
                            logger.warning(f"⚠️ No tiles found for district {district_info['original_name']}")
                    
                    # Update city totals
                    city_results['total_tiles'] += city_results['map_type_results'][map_type]['total_tiles']
                    city_results['successful_tiles'] += city_results['map_type_results'][map_type]['successful_tiles']
                    
                else:
                    # EXISTING: Standard processing for other map types
                    # Check if this map type already exists
                    if skip_existing:
                        already_downloaded, status_msg = self.check_city_map_type_downloaded(city_name, map_type)
                        if already_downloaded:
                            logger.info(f"⏭️ SKIPPING {city_name} - {map_display}: {status_msg}")
                            continue
                    
                    map_type_tiles = []
                    
                    # Process each pattern for this map type
                    for pattern in patterns_list:
                        logger.info(f"🚀 Crawling pattern: {pattern}")
                        
                        # Crawl with standard structure
                        pattern_tiles = self.crawl_pattern_for_city_and_map_type(
                            pattern, city_coverage, city_name, map_type
                        )
                        
                        if pattern_tiles:
                            map_type_tiles.extend(pattern_tiles)
                    
                    if map_type_tiles:
                        successful_count = len([t for t in map_type_tiles if t.get('success')])
                        
                        city_results['map_type_results'][map_type] = {
                            'map_type': map_type,
                            'display_name': map_display,
                            'patterns': patterns_list,
                            'tiles': map_type_tiles,
                            'total_tiles': len(map_type_tiles),
                            'successful_tiles': successful_count,
                            'folder_structure': f"downloaded_tiles/cities/{self.clean_city_name(city_name)}/{MAP_TYPE_CONFIG[map_type]['folder_name']}/<zoom>/"
                        }
                        
                        city_results['total_tiles'] += len(map_type_tiles)
                        city_results['successful_tiles'] += successful_count
                        
                        logger.info(f"✅ {map_display}: {successful_count}/{len(map_type_tiles)} tiles")
                    else:
                        logger.warning(f"⚠️ No tiles found for {map_display}")
            
            if city_results['map_type_results']:
                all_results.append(city_results)
                logger.info(f"✅ {city_name} complete: {city_results['successful_tiles']}/{city_results['total_tiles']} total tiles")
            else:
                logger.warning(f"⚠️ No results for {city_name}")
        
        return all_results

    def check_city_map_type_downloaded(self, city_name, map_type):
        """Check if specific city + map type already downloaded"""
        clean_city_name = self.clean_city_name(city_name)
        map_folder = MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])['folder_name']
        
        city_map_path = Path(self.base_download_dir) / 'cities' / clean_city_name / map_folder
        
        if not city_map_path.exists():
            return False, "No download folder found"
        
        # Check if any zoom folders exist with tiles
        zoom_folders = [d for d in city_map_path.iterdir() if d.is_dir() and d.name.isdigit()]
        
        if not zoom_folders:
            return False, "No zoom folders found"
        
        # Count total tiles
        total_tiles = 0
        for zoom_folder in zoom_folders:
            tile_files = list(zoom_folder.glob('*.*'))
            total_tiles += len(tile_files)
    
        if total_tiles == 0:
            return False, "Zoom folders exist but no tiles found"
    
        return True, f"Found {total_tiles:,} tiles in {len(zoom_folders)} zoom levels"

    def generate_multi_map_report(self, results, start_time):
        """Generate comprehensive multi-map report"""
        elapsed_time = time.time() - start_time
        
        # Calculate totals by map type
        map_type_summary = {}
        total_cities = len(results)
        total_tiles = 0
        total_successful = 0
        total_size_mb = 0
        
        for result in results:
            for map_type, map_result in result['map_type_results'].items():
                if map_type not in map_type_summary:
                    map_type_summary[map_type] = {
                        'display_name': MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])['display_name'],
                        'cities': 0,
                        'total_tiles': 0,
                        'successful_tiles': 0,
                        'total_size_mb': 0
                    }
                
                map_type_summary[map_type]['cities'] += 1
                map_type_summary[map_type]['total_tiles'] += map_result['total_tiles']
                map_type_summary[map_type]['successful_tiles'] += map_result['successful_tiles']
                
                # Calculate size
                size_bytes = sum(t.get('size', 0) for t in map_result['tiles'] if t.get('success'))
                size_mb = size_bytes / 1024 / 1024
                map_type_summary[map_type]['total_size_mb'] += size_mb
                
                total_tiles += map_result['total_tiles']
                total_successful += map_result['successful_tiles']
                total_size_mb += size_mb
        
        report = {
            'crawl_type': 'Enhanced Multi-Map Type Crawl',
            'folder_structure': 'downloaded_tiles/cities/<city>/<map_type>/<zoom>/',
            'timestamp': datetime.now().isoformat(),
            'execution_time_seconds': elapsed_time,
            'execution_time_minutes': elapsed_time / 60,
            'summary': {
                'cities_processed': total_cities,
                'map_types_found': len(map_type_summary),
                'total_tiles_tested': total_tiles,
                'total_tiles_successful': total_successful,
                'overall_success_rate': total_successful / total_tiles * 100 if total_tiles > 0 else 0,
                'total_download_size_mb': total_size_mb,
            },
            'map_type_summary': map_type_summary,
            'city_results': results,
            'performance_stats': self.stats.copy()
        }
        
        # Save report
        report_file = f"{self.base_output_dir}/multi_map_crawl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_summary = f"""
# ENHANCED MULTI-MAP TYPE CRAWL REPORT
Generated: {report['timestamp']}
Duration: {elapsed_time/60:.1f} minutes

## 📁 NEW FOLDER STRUCTURE
downloaded_tiles/cities/<city>/<map_type>/<zoom>/
Example: downloaded_tiles/cities/hanoi/qh-2030/12/3249_1865.png

## 📊 OVERALL SUMMARY
• Cities processed: {total_cities}
• Map types found: {len(map_type_summary)}
• Total tiles tested: {total_tiles:,}
• Successful downloads: {total_successful:,}
• Success rate: {report['summary']['overall_success_rate']:.1f}%
• Total size: {total_size_mb:.1f} MB

## 🗺️ MAP TYPE BREAKDOWN
"""
        
        for map_type, summary in map_type_summary.items():
            success_rate = (summary['successful_tiles'] / summary['total_tiles'] * 100) if summary['total_tiles'] > 0 else 0
            text_summary += f"""
### {summary['display_name']} ({map_type})
• Cities: {summary['cities']}
• Tiles tested: {summary['total_tiles']:,}
• Successful: {summary['successful_tiles']:,} ({success_rate:.1f}%)
• Size: {summary['total_size_mb']:.1f} MB
• Folder: downloaded_tiles/cities/<city>/{MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])['folder_name']}/<zoom>/
"""
        
        text_summary += "\n## 🏙️ CITY BREAKDOWN\n"
        
        for result in results:
            text_summary += f"\n🏙️ {result['city'].upper()}\n"
            for map_type, map_result in result['map_type_results'].items():
                success_rate = (map_result['successful_tiles'] / map_result['total_tiles'] * 100) if map_result['total_tiles'] > 0 else 0
                text_summary += f"  • {map_result['display_name']}: {map_result['successful_tiles']:,}/{map_result['total_tiles']:,} ({success_rate:.1f}%)\n"
        
        text_file = f"{self.base_output_dir}/multi_map_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(text_summary)
        
        # Print summary
        print(f"\n🎉 MULTI-MAP CRAWL COMPLETE!")
        print("=" * 50)
        print(f"⏱️ Duration: {elapsed_time/60:.1f} minutes")
        print(f"🏙️ Cities: {total_cities}")
        print(f"🗺️ Map types: {len(map_type_summary)}")
        print(f"📊 Tiles tested: {total_tiles:,}")
        print(f"✅ Downloaded: {total_successful:,} ({report['summary']['overall_success_rate']:.1f}%)")
        print(f"💾 Size: {total_size_mb:.1f} MB")
        print(f"📁 Structure: downloaded_tiles/cities/<city>/<map_type>/<zoom>/")
        
        # Print map type breakdown
        for map_type, summary in map_type_summary.items():
            print(f"  🗺️ {summary['display_name']}: {summary['successful_tiles']:,} tiles ({summary['total_size_mb']:.1f} MB)")
        
        logger.info(f"📋 Reports saved:")
        logger.info(f"  JSON: {report_file}")
        logger.info(f"  Text: {text_file}")
        
        return report

def main():
    """Enhanced main function with multi-map support"""
    print("🚀 ENHANCED MULTI-MAP TILE DOWNLOADER v2.0")
    print("Downloads tiles for all map types with organized structure")
    print("📁 Structure: downloaded_tiles/cities/<city>/<map_type>/<zoom>/")
    print("=" * 60)
    
    # Source selection
    source_choice = input("Pattern source (1=HTML extractor, 2=Browser crawl, default=1): ").strip()
    use_html_source = source_choice != '2'
    
    if use_html_source:
        print("📋 Using HTML extractor results as pattern source")
    else:
        print("📋 Using browser crawl results as pattern source")
    
    # Map type selection
    print("\nAvailable map types:")
    for i, (key, config) in enumerate(MAP_TYPE_CONFIG.items(), 1):
        if key != 'UNKNOWN':
            print(f"{i}. {config['display_name']} -> {config['folder_name']}/")
    
    print("Select map types:")
    print("A. All map types")
    print("B. Only QH 2030")
    print("C. Only KH 2025") 
    print("D. QH 2030 + KH 2025")
    
    map_choice = input("Choose option (A/B/C/D, default=D): ").upper().strip()
    
    if map_choice == 'A':
        target_map_types = [k for k in MAP_TYPE_CONFIG.keys() if k != 'UNKNOWN']
    elif map_choice == 'B':
        target_map_types = ['QH_2030']
    elif map_choice == 'C':
        target_map_types = ['KH_2025']
    else:
        target_map_types = ['QH_2030', 'KH_2025']
    
    selected_names = [MAP_TYPE_CONFIG[mt]['display_name'] for mt in target_map_types]
    print(f"🎯 Selected: {', '.join(selected_names)}")
    
    # Skip existing option
    skip_choice = input("Skip existing downloads? (y/n, default=y): ").lower()
    skip_existing = skip_choice != 'n'
    
    # Download mode
    download_choice = input("Enable tile downloads? (y/n, default=y): ").lower()
    enable_download = download_choice != 'n'
    
    # Zoom selection
    print("\nZoom level options:")
    print("1. Light (10, 12) - ~2K tiles per city per map type")
    print("2. Standard (10, 12, 14) - ~30K tiles per city per map type") 
    print("3. Heavy (10, 11, 12, 13, 14, 15, 16, 17, 18) - ~500K tiles per city per map type")
    
    zoom_choice = input("Choose zoom levels (1/2/3, default=2): ").strip()
    
    if zoom_choice == '1':
        zoom_levels = [10, 12]
    elif zoom_choice == '3':
        zoom_levels = [10, 11, 12, 13, 14, 15, 16, 17, 18]
    else:
        zoom_levels = [10, 12, 14]
    
    print(f"🎯 Selected zoom levels: {zoom_levels}")
    
    # Show expected folder structure
    print(f"\n📁 Expected folder structure:")
    print("downloaded_tiles/")
    print("├── cities/")
    for city in ['hanoi', 'hcm', 'danang']:
        print(f"│   ├── {city}/")
        for map_type in target_map_types:
            folder_name = MAP_TYPE_CONFIG[map_type]['folder_name']
            print(f"│   │   ├── {folder_name}/")
            for zoom in zoom_levels[:2]:  # Show first 2 zoom levels
                print(f"│   │   │   ├── {zoom}/ (contains *.png files)")
    print("│   │   │   └── ...")
    print()
    
    # Initialize enhanced downloader
    downloader = EnhancedMultiMapTileDownloader(enable_download=enable_download)
    
    # Run enhanced crawl
    start_time = time.time()
    
    results = downloader.crawl_multi_map_patterns(
        zoom_levels=zoom_levels,
        use_html_source=use_html_source,
        skip_existing=skip_existing,
        target_map_types=target_map_types
    )
    
    if not results:
        logger.error("❌ No results from multi-map crawling")
        return
    
    # Generate comprehensive report
    report = downloader.generate_multi_map_report(results, start_time)
    
    if report:
        total_tiles = report['summary']['total_tiles_successful']
        total_size = report['summary']['total_download_size_mb']
        map_types_count = report['summary']['map_types_found']
        
        print(f"\n🎉 MULTI-MAP CRAWL COMPLETE!")
        print(f"📈 Downloaded {total_tiles:,} tiles ({total_size:.1f} MB)")
        print(f"🗺️ Processed {map_types_count} map types")
        print(f"📁 Check downloaded_tiles/cities/<city>/<map_type>/<zoom>/ for results")

if __name__ == "__main__":
    main()