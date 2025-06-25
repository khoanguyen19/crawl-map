#!/usr/bin/env python3
"""
ULTRA-OPTIMIZED Multi-Map Type Tile Downloader for Guland
Fixed: Proper tile generation and KH_2025 folder structure
"""
import os
import json
import time
import logging
import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import math
import re
import unicodedata
from typing import List, Dict, Tuple, Optional
import hashlib

# Setup optimized logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ultra_tile_downloader.log'),
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
        'priority': 2,
        'requires_district': True  # KH_2025 requires district folder
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

class UltraOptimizedTileDownloader:
    def __init__(self, 
                 max_workers=50,
                 timeout=15,
                 max_connections=100,
                 max_connections_per_host=20,
                 enable_download=True,
                 batch_size=500):
        
        self.max_workers = max_workers
        self.timeout = timeout
        self.max_connections = max_connections
        self.max_connections_per_host = max_connections_per_host
        self.batch_size = batch_size
        self.enable_download = enable_download
        
        # Folder structure
        self.base_download_dir = 'downloaded_tiles'
        self.base_output_dir = 'ultra_performance_reports'
        
        # Create directories
        os.makedirs(self.base_download_dir, exist_ok=True)
        os.makedirs(f'{self.base_download_dir}/cities', exist_ok=True)
        os.makedirs(self.base_output_dir, exist_ok=True)
        
        # Performance statistics
        self.stats = {
            'total_attempted': 0,
            'total_successful': 0,
            'total_failed': 0,
            'total_bytes': 0,
            'total_skipped': 0,
            'cache_hits': 0,
            'map_type_stats': {}
        }
        
        # Cache for file existence checks
        self.file_exists_cache = set()
        self.build_file_cache()
        
        # District data
        self.district_data = {}
        self.load_district_data()
        
        logger.info(f"🚀 ULTRA-OPTIMIZED Downloader initialized")
        logger.info(f"⚡ Max workers: {max_workers}, Batch size: {batch_size}")
        logger.info(f"🔗 Connection pool: {max_connections}/{max_connections_per_host}")
        logger.info(f"📁 Cache built: {len(self.file_exists_cache)} existing files")
        
        self._session = None
        self._session_lock = asyncio.Lock()
    
    async def get_session(self):
        """Get or create shared session"""
        if self._session is None or self._session.closed:
            async with self._session_lock:
                if self._session is None or self._session.closed:
                    self._session = await self.create_session()
        return self._session
    
    async def cleanup(self):
        """Cleanup resources"""
        if self._session and not self._session.closed:
            await self._session.close()

    def build_file_cache(self):
        """Build cache of existing files for ultra-fast existence checks"""
        start_time = time.time()
        cache_file = f"{self.base_download_dir}/.file_cache.txt"
        
        # Try to load from cache file first
        if os.path.exists(cache_file) and os.path.getmtime(cache_file) > time.time() - 3600:
            try:
                with open(cache_file, 'r') as f:
                    self.file_exists_cache = set(line.strip() for line in f)
                logger.info(f"📋 Loaded file cache: {len(self.file_exists_cache)} files in {time.time() - start_time:.2f}s")
                return
            except:
                pass
        
        # Build cache from scratch
        cities_dir = Path(self.base_download_dir) / 'cities'
        if cities_dir.exists():
            for tile_file in cities_dir.rglob('*.*'):
                if tile_file.is_file() and tile_file.suffix.lower() in ['.png', '.jpg', '.jpeg', '.webp']:
                    relative_path = str(tile_file.relative_to(self.base_download_dir))
                    self.file_exists_cache.add(relative_path)
        
        # Save cache
        try:
            with open(cache_file, 'w') as f:
                for file_path in sorted(self.file_exists_cache):
                    f.write(f"{file_path}\n")
        except:
            pass
        
        build_time = time.time() - start_time
        logger.info(f"🏗️ Built file cache: {len(self.file_exists_cache)} files in {build_time:.2f}s")

    def fast_file_exists(self, filepath: str) -> bool:
        """Ultra-fast file existence check using cache"""
        relative_path = str(Path(filepath).relative_to(self.base_download_dir))
        exists = relative_path in self.file_exists_cache
        if exists:
            self.stats['cache_hits'] += 1
        return exists

    def add_to_cache(self, filepath: str):
        """Add new file to cache"""
        relative_path = str(Path(filepath).relative_to(self.base_download_dir))
        self.file_exists_cache.add(relative_path)

    async def create_session(self) -> aiohttp.ClientSession:
        """Create optimized aiohttp session with connection pooling"""
        connector = aiohttp.TCPConnector(
            limit=self.max_connections,
            limit_per_host=self.max_connections_per_host,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=self.timeout, connect=5)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0'
        }
        
        return aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers
        )

    async def download_single_tile_async(
        self, 
        session: aiohttp.ClientSession,
        tile_info: Dict,
        city_name: str,
        map_type: str,
        district_name: Optional[str] = None
    ) -> Dict:
        """Ultra-optimized async tile download"""
        try:
            url = tile_info['url']
            zoom = tile_info['zoom']
            x = tile_info['x']
            y = tile_info['y']
            
            # Detect format from URL
            if '.png' in url.lower():
                format_ext = 'png'
            elif '.jpg' in url.lower() or '.jpeg' in url.lower():
                format_ext = 'jpg'
            elif '.webp' in url.lower():
                format_ext = 'webp'
            else:
                format_ext = 'png'
            
            # Create folder structure with proper district handling
            folder_path = self.create_map_type_folder_structure(city_name, map_type, zoom, district_name)
            filename = f"{x}_{y}.{format_ext}"
            filepath = os.path.join(folder_path, filename)
            
            # Ultra-fast existence check using cache
            if self.fast_file_exists(filepath):
                try:
                    file_size = os.path.getsize(filepath)
                    self.stats['total_skipped'] += 1
                    return {
                        'success': True,
                        'filepath': filepath,
                        'size': file_size,
                        'status': 'cached',
                        'tile_info': tile_info,
                        'map_type': map_type,
                        'district_name': district_name
                    }
                except:
                    # File corrupted, download again
                    pass
            
            if not self.enable_download:
                return {'success': False, 'reason': 'Download disabled'}
            
            # Download with streaming for memory efficiency
            async with session.get(url) as response:
                if response.status == 200:
                    content_type = response.headers.get('content-type', '').lower()
                    
                    if any(img_type in content_type for img_type in ['image/', 'application/octet-stream']):
                        # Stream to file for memory efficiency
                        async with aiofiles.open(filepath, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                await f.write(chunk)
                        
                        # Check file size
                        size = os.path.getsize(filepath)
                        
                        if size > 100:  # Valid tile
                            # Add to cache
                            self.add_to_cache(filepath)
                            
                            # Update stats atomically
                            self.stats['total_successful'] += 1
                            self.stats['total_bytes'] += size
                            
                            return {
                                'success': True,
                                'filepath': filepath,
                                'size': size,
                                'status': 'downloaded',
                                'tile_info': tile_info,
                                'map_type': map_type,
                                'district_name': district_name,
                                'content_type': content_type
                            }
                        else:
                            # Remove invalid file
                            try:
                                os.remove(filepath)
                            except:
                                pass
                            
                            self.stats['total_failed'] += 1
                            return {
                                'success': False,
                                'reason': f'Invalid file size: {size}',
                                'tile_info': tile_info,
                                'map_type': map_type,
                                'district_name': district_name
                            }
                    else:
                        self.stats['total_failed'] += 1
                        return {
                            'success': False,
                            'reason': f'Invalid content type: {content_type}',
                            'tile_info': tile_info,
                            'map_type': map_type,
                            'district_name': district_name
                        }
                else:
                    self.stats['total_failed'] += 1
                    return {
                        'success': False,
                        'reason': f'HTTP {response.status}',
                        'tile_info': tile_info,
                        'map_type': map_type,
                        'district_name': district_name
                    }
                    
        except asyncio.TimeoutError:
            self.stats['total_failed'] += 1
            return {
                'success': False,
                'reason': 'Timeout',
                'tile_info': tile_info,
                'map_type': map_type,
                'district_name': district_name
            }
        except Exception as e:
            self.stats['total_failed'] += 1
            return {
                'success': False,
                'reason': f'Error: {str(e)}',
                'tile_info': tile_info,
                'map_type': map_type,
                'district_name': district_name
            }

    async def download_batch_async(
        self,
        tile_batch: List[Dict],
        city_name: str,
        map_type: str,
        district_name: Optional[str] = None
    ) -> List[Dict]:
        """Ultra-optimized batch download with async processing"""
        
        # FIX: Await the session creation and use it properly
        session = await self.create_session()
        try:
            # Create semaphore to control concurrency
            semaphore = asyncio.Semaphore(self.max_workers)
            
            async def download_with_semaphore(tile_info):
                async with semaphore:
                    return await self.download_single_tile_async(
                        session, tile_info, city_name, map_type, district_name
                    )
            
            # Execute all downloads concurrently
            tasks = [download_with_semaphore(tile_info) for tile_info in tile_batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle exceptions
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    processed_results.append({
                        'success': False,
                        'reason': f'Exception: {str(result)}',
                        'tile_info': tile_batch[i],
                        'map_type': map_type,
                        'district_name': district_name
                    })
                else:
                    processed_results.append(result)
            
            return processed_results
        except Exception as e:
            logger.error(f"Batch error: {e}")
            # Reset session on error
            if not session.closed:
                await session.close()
            self._session = None
            raise
        finally:
            await session.close()

    def create_map_type_folder_structure(self, city_name: str, map_type: str, zoom_level: int, district_name: Optional[str] = None) -> str:
        """Create optimized folder structure with proper KH_2025 district handling"""
        clean_city_name = self.clean_city_name(city_name)
        map_config = MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])
        map_folder = map_config['folder_name']
        
        # FIX: Proper folder structure for KH_2025 with districts
        if map_type == 'KH_2025' and district_name:
            clean_district_name = self.clean_district_name(district_name)
            # Structure: downloaded_tiles/cities/<city>/kh-2025/<district>/<zoom>
            city_path = Path(self.base_download_dir) / 'cities' / clean_city_name / map_folder / clean_district_name / str(zoom_level)
        else:
            # Structure: downloaded_tiles/cities/<city>/<map_type>/<zoom>
            city_path = Path(self.base_download_dir) / 'cities' / clean_city_name / map_folder / str(zoom_level)
        
        city_path.mkdir(parents=True, exist_ok=True)
        return str(city_path)

    def clean_city_name(self, city_name: str) -> str:
        """Optimized city name cleaning with caching"""
        if not hasattr(self, '_clean_name_cache'):
            self._clean_name_cache = {}
        
        if city_name in self._clean_name_cache:
            return self._clean_name_cache[city_name]
        
        clean_name = city_name.lower()
        clean_name = clean_name.replace(' ', '_')
        clean_name = clean_name.replace('tp_', '')
        clean_name = clean_name.replace('tp ', '')
        clean_name = clean_name.replace('-', '_')
        
        # Remove Vietnamese diacritics
        clean_name = unicodedata.normalize('NFD', clean_name)
        clean_name = ''.join(c for c in clean_name if unicodedata.category(c) != 'Mn')
        
        # Apply city mapping
        city_mapping = {
            # Major cities - All variations
            'ha_noi': 'hanoi',
            'hanoi': 'hanoi',
            'hà_nội': 'hanoi',
            'tphochminh': 'hcm',
            'tphồchíminh': 'hcm',
            'hochiminh': 'hcm',
            'hồchíminh': 'hcm',
            'hcm': 'hcm',
            'saigon': 'hcm',
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
        
        result = city_mapping.get(clean_name, clean_name)
        self._clean_name_cache[city_name] = result
        return result

    def clean_district_name(self, district_name: str) -> str:
        """Optimized district name cleaning"""
        if not hasattr(self, '_clean_district_cache'):
            self._clean_district_cache = {}
        
        if district_name in self._clean_district_cache:
            return self._clean_district_cache[district_name]
        
        clean_name = district_name.lower().strip()
        
        # Remove diacritics
        clean_name = unicodedata.normalize('NFD', clean_name)
        clean_name = ''.join(c for c in clean_name if unicodedata.category(c) != 'Mn')
        
        # Apply transformations
        transformations = {
            r'^quan\s+(\d+)$': r'quan-\1',
            r'^quan\s+(.+)$': r'quan-\1',
            r'^huyen\s+(.+)$': r'huyen-\1',
            r'^thi\s+xa\s+(.+)$': r'thi-xa-\1',
            r'^thanh\s+pho\s+(.+)$': r'thanh-pho-\1',
            r'^tp\s+(.+)$': r'thanh-pho-\1',
        }
        
        for pattern, replacement in transformations.items():
            if re.match(pattern, clean_name):
                clean_name = re.sub(pattern, replacement, clean_name)
                break
        
        # Final cleanup
        clean_name = clean_name.replace(' ', '-')
        clean_name = re.sub(r'[^\w-]', '', clean_name)
        clean_name = re.sub(r'-+', '-', clean_name)
        clean_name = clean_name.strip('-')
        
        self._clean_district_cache[district_name] = clean_name
        return clean_name


    def load_district_data(self):
        """Load district data from JSON files with improved name matching"""
        districts_dir = Path('output_enhanced_patterns/districts')
        if not districts_dir.exists():
            logger.warning(f"📁 Districts directory not found: {districts_dir}")
            return
        
        # City name mapping for better matching
        city_name_mapping = {
            'backan': ['Bắc_Kạn', 'Bắc Kạn', 'bac-kan', 'backan'],
            'hanoi': ['Hà_Nội', 'Hà Nội', 'ha-noi', 'hanoi'],
            'hcm': ['Hồ_Chí_Minh', 'Hồ Chí Minh', 'ho-chi-minh', 'hcm', 'tphcm'],
            'danang': ['Đà_Nẵng', 'Đà Nẵng', 'da-nang', 'danang'],
            'haiphong': ['Hải_Phòng', 'Hải Phòng', 'hai-phong', 'haiphong'],
            'cantho': ['Cần_Thơ', 'Cần Thơ', 'can-tho', 'cantho'],
            'dongnai': ['Đồng_Nai', 'Đồng Nai', 'dong-nai', 'dongnai'],
            'baria_vungtau': ['Bà_Rịa_Vũng_Tàu', 'Bà Rịa - Vũng Tàu', 'ba-ria-vung-tau', 'baria_vungtau'],
            'angiang': ['An_Giang', 'An Giang', 'an-giang', 'angiang'],
            'bacgiang': ['Bắc_Giang', 'Bắc Giang', 'bac-giang', 'bacgiang'],
            'baclieu': ['Bạc_Liêu', 'Bạc Liêu', 'bac-lieu', 'baclieu'],
            'bacninh': ['Bắc_Ninh', 'Bắc Ninh', 'bac-ninh', 'bacninh'],
            'bentre': ['Bến_Tre', 'Bến Tre', 'ben-tre', 'bentre'],
            'binhduong': ['Bình_Dương', 'Bình Dương', 'binh-duong', 'binhduong'],
            'binhphuoc': ['Bình_Phước', 'Bình Phước', 'binh-phuoc', 'binhphuoc'],
            'binhthuan': ['Bình_Thuận', 'Bình Thuận', 'binh-thuan', 'binhthuan'],
            'binhdinh': ['Bình_Định', 'Bình Định', 'binh-dinh', 'binhdinh'],
            'camau': ['Cà_Mau', 'Cà Mau', 'ca-mau', 'camau'],
            'caobang': ['Cao_Bằng', 'Cao Bằng', 'cao-bang', 'caobang'],
            'gialai': ['Gia_Lai', 'Gia Lai', 'gia-lai', 'gialai'],
            'hanam': ['Hà_Nam', 'Hà Nam', 'ha-nam', 'hanam'],
            'hagiang': ['Hà_Giang', 'Hà Giang', 'ha-giang', 'hagiang'],
            'hatinh': ['Hà_Tĩnh', 'Hà Tĩnh', 'ha-tinh', 'hatinh'],
            'haiduong': ['Hải_Dương', 'Hải Dương', 'hai-duong', 'haiduong'],
            'haugiang': ['Hậu_Giang', 'Hậu Giang', 'hau-giang', 'haugiang'],
            'hoabinh': ['Hòa_Bình', 'Hòa Bình', 'hoa-binh', 'hoabinh'],
            'hungyen': ['Hưng_Yên', 'Hưng Yên', 'hung-yen', 'hungyen'],
            'khanhhoa': ['Khánh_Hòa', 'Khánh Hòa', 'khanh-hoa', 'khanhhoa'],
            'kiengiang': ['Kiên_Giang', 'Kiên Giang', 'kien-giang', 'kiengiang'],
            'kontum': ['Kon_Tum', 'Kon Tum', 'kon-tum', 'kontum'],
            'laichau': ['Lai_Châu', 'Lai Châu', 'lai-chau', 'laichau'],
            'lamdong': ['Lâm_Đồng', 'Lâm Đồng', 'lam-dong', 'lamdong'],
            'langson': ['Lạng_Sơn', 'Lạng Sơn', 'lang-son', 'langson'],
            'laocai': ['Lào_Cai', 'Lào Cai', 'lao-cai', 'laocai'],
            'longan': ['Long_An', 'Long An', 'long-an', 'longan'],
            'namdinh': ['Nam_Định', 'Nam Định', 'nam-dinh', 'namdinh'],
            'nghean': ['Nghệ_An', 'Nghệ An', 'nghe-an', 'nghean'],
            'ninhbinh': ['Ninh_Bình', 'Ninh Bình', 'ninh-binh', 'ninhbinh'],
            'ninhthuan': ['Ninh_Thuận', 'Ninh Thuận', 'ninh-thuan', 'ninhthuan'],
            'phutho': ['Phú_Thọ', 'Phú Thọ', 'phu-tho', 'phutho'],
            'phuyen': ['Phú_Yên', 'Phú Yên', 'phu-yen', 'phuyen'],
            'quangbinh': ['Quảng_Bình', 'Quảng Bình', 'quang-binh', 'quangbinh'],
            'quangnam': ['Quảng_Nam', 'Quảng Nam', 'quang-nam', 'quangnam'],
            'quangngai': ['Quảng_Ngãi', 'Quảng Ngãi', 'quang-ngai', 'quangngai'],
            'quangninh': ['Quảng_Ninh', 'Quảng Ninh', 'quang-ninh', 'quangninh'],
            'quangtri': ['Quảng_Trị', 'Quảng Trị', 'quang-tri', 'quangtri'],
            'soctrang': ['Sóc_Trăng', 'Sóc Trăng', 'soc-trang', 'soctrang'],
            'sonla': ['Sơn_La', 'Sơn La', 'son-la', 'sonla'],
            'tayninh': ['Tây_Ninh', 'Tây Ninh', 'tay-ninh', 'tayninh'],
            'thaibinh': ['Thái_Bình', 'Thái Bình', 'thai-binh', 'thaibinh'],
            'thainguyen': ['Thái_Nguyên', 'Thái Nguyên', 'thai-nguyen', 'thainguyen'],
            'thanhhoa': ['Thanh_Hóa', 'Thanh Hóa', 'thanh-hoa', 'thanhhoa'],
            'thuathienhue': ['Thừa_Thiên_Huế', 'Thừa Thiên Huế', 'thua-thien-hue', 'thuathienhue'],
            'tiengiang': ['Tiền_Giang', 'Tiền Giang', 'tien-giang', 'tiengiang'],
            'travinh': ['Trà_Vinh', 'Trà Vinh', 'tra-vinh', 'travinh'],
            'tuyenquang': ['Tuyên_Quang', 'Tuyên Quang', 'tuyen-quang', 'tuyenquang'],
            'vinhlong': ['Vĩnh_Long', 'Vĩnh Long', 'vinh-long', 'vinhlong'],
            'vinhphuc': ['Vĩnh_Phúc', 'Vĩnh Phúc', 'vinh-phuc', 'vinhphuc'],
            'yenbai': ['Yên_Bái', 'Yên Bái', 'yen-bai', 'yenbai'],
            'daklak': ['Đắk_Lắk', 'Đắk Lắk', 'dak-lak', 'daklak'],
            'daknong': ['Đắk_Nông', 'Đắk Nông', 'dak-nong', 'daknong'],
            'dienbien': ['Điện_Biên', 'Điện Biên', 'dien-bien', 'dienbien'],
            'dongthap': ['Đồng_Tháp', 'Đồng Tháp', 'dong-thap', 'dongthap']
        }
        
        loaded_count = 0
        
        for json_file in districts_dir.glob('*_districts.json'):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                province_name = data.get('province_name', '')
                
                # Find matching city key
                matched_city = None
                for city_key, name_variants in city_name_mapping.items():
                    if any(variant in json_file.name or variant == province_name for variant in name_variants):
                        matched_city = city_key
                        break
                
                if matched_city:
                    # FIX: Process district data structure correctly
                    if 'districts' in data:
                        city_districts = {}
                        
                        for district_name, district_info in data['districts'].items():
                            if 'patterns' in district_info and 'map_types' in district_info['patterns']:
                                map_types = district_info['patterns']['map_types']
                                
                                # Extract KH_2025 patterns
                                kh_2025_patterns = []
                                if 'KH_2025' in map_types:
                                    kh_data = map_types['KH_2025']
                                    if isinstance(kh_data, dict) and 'tile_url' in kh_data:
                                        tile_url = kh_data['tile_url']
                                        if tile_url and tile_url.strip():
                                            kh_2025_patterns.append(tile_url.strip())
                                
                                # Clean district name
                                clean_district_name = self.clean_district_name(district_name)
                                
                                # Store properly structured data
                                city_districts[clean_district_name] = {
                                    'original_name': district_name,
                                    'clean_name': clean_district_name,
                                    'kh_2025_patterns': kh_2025_patterns
                                }
                        
                        # Only add if we have districts with data
                        if city_districts:
                            self.district_data[matched_city] = city_districts                                                       
                            loaded_count += 1
                            logger.info(f"✅ Loaded {len(city_districts)} districts for {matched_city}: {json_file.name}")
                        else:
                            logger.warning(f"⚠️ No valid districts found for {matched_city}: {json_file.name}")
                    else:
                        logger.warning(f"⚠️ No districts section found in {json_file.name}")
                else:
                    logger.warning(f"⚠️ No city mapping found for: {json_file.name} (province: {province_name})")
                    
            except Exception as e:
                logger.error(f"❌ Error loading {json_file}: {e}")
                import traceback
                logger.error(f"📍 Traceback: {traceback.format_exc()}")
        
        logger.info(f"📊 Loaded district data for {loaded_count} cities")
        
        # Debug: Show what was loaded
        for city_name, districts in self.district_data.items():
            valid_districts = sum(1 for d in districts.values() if d.get('kh_2025_patterns'))
            logger.info(f"   📊 {city_name}: {valid_districts}/{len(districts)} districts with KH_2025 patterns")
            
    def normalize_city_name(self, city_name_raw: str) -> str:
        """Normalize city name from filename to match pattern expectations"""
        # Convert "Kon_Tum" -> "kon_tum" -> "kontum"
        normalized = city_name_raw.lower().replace('_', '').replace('-', '').replace(' ', '')
        
        # City name mappings for consistency
        city_mappings = {
            # Major cities - All variations
            'ha_noi': 'hanoi',
            'hanoi': 'hanoi',
            'hà_nội': 'hanoi',
            'tphochminh': 'hcm',
            'tphồchíminh': 'hcm',
            'hochiminh': 'hcm',
            'hồchíminh': 'hcm',
            'hcm': 'hcm',
            'saigon': 'hcm',
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
        
        return city_mappings.get(normalized, normalized)

    # def clean_district_name(self, district_name: str) -> str:
    #     """Clean district name for folder structure"""
    #     # Remove prefixes and normalize
    #     clean_name = district_name.lower()
    #     clean_name = clean_name.replace('huyện ', '').replace('thành phố ', '').replace('thị xã ', '')
    #     clean_name = clean_name.replace(' ', '_').replace('-', '_')
        
    #     # Remove Vietnamese diacritics
    #     import unicodedata
    #     clean_name = unicodedata.normalize('NFD', clean_name)
    #     clean_name = ''.join(c for c in clean_name if unicodedata.category(c) != 'Mn')
        
    #     return clean_name

    def generate_tile_urls_optimized(self, pattern: str, city_coverage: Dict) -> List[Dict]:
        """FIX: Generate ALL tile URLs properly"""
        all_tiles = []
        
        logger.info(f"🔢 Generating tiles for pattern: {pattern}")
        
        for zoom, coverage in city_coverage.items():
            x_min = coverage['x_min']
            x_max = coverage['x_max']
            y_min = coverage['y_min']
            y_max = coverage['y_max']
            
            zoom_tiles = 0
            
            # FIX: Generate ALL tiles in the coverage area
            for x in range(x_min, x_max + 1):
                for y in range(y_min, y_max + 1):
                    url = pattern.replace('{z}', str(zoom)).replace('{x}', str(x)).replace('{y}', str(y))
                    all_tiles.append({
                        'url': url,
                        'zoom': zoom,
                        'x': x,
                        'y': y,
                        'pattern': pattern
                    })
                    zoom_tiles += 1
            
            logger.info(f"  📊 Zoom {zoom}: {zoom_tiles:,} tiles (x:{x_min}-{x_max}, y:{y_min}-{y_max})")
        
        logger.info(f"📊 Total tiles generated: {len(all_tiles):,}")
        return all_tiles

    async def crawl_pattern_ultra_fast(
        self,
        pattern: str,
        city_coverage: Dict,
        city_name: str,
        map_type: str,
        district_name: Optional[str] = None
    ) -> List[Dict]:
        """Ultra-fast pattern crawling with async batch processing"""
        
        map_display = MAP_TYPE_CONFIG.get(map_type, MAP_TYPE_CONFIG['UNKNOWN'])['display_name']
        district_log = f" - {district_name}" if district_name else ""
        logger.info(f"🚀 ULTRA-FAST: {city_name}{district_log} - {map_display}")
        logger.info(f"🌐 Pattern: {pattern}")
        
        # Generate all tile URLs
        all_tile_urls = self.generate_tile_urls_optimized(pattern, city_coverage)
        total_tiles = len(all_tile_urls)
        
        if total_tiles == 0:
            logger.warning("⚠️ No tiles generated!")
            return []
        
        logger.info(f"📊 Generated {total_tiles:,} tile URLs")
        
        # Process in large batches for maximum throughput
        all_results = []
        batch_size = self.batch_size
        total_batches = (total_tiles + batch_size - 1) // batch_size
        
        start_time = time.time()
        
        for i in range(0, total_tiles, batch_size):
            batch = all_tile_urls[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            batch_start = time.time()
            
            # Process batch asynchronously
            batch_results = await self.download_batch_async(
                batch, city_name, map_type, district_name
            )
            
            batch_time = time.time() - batch_start
            successful = len([r for r in batch_results if r.get('success')])
            cached = len([r for r in batch_results if r.get('status') == 'cached'])
            downloaded = len([r for r in batch_results if r.get('status') == 'downloaded'])
            
            all_results.extend(batch_results)
            
            # Performance metrics
            tiles_per_second = len(batch) / batch_time if batch_time > 0 else 0
            progress_pct = (batch_num / total_batches) * 100
            
            logger.info(
                f"⚡ Batch {batch_num}/{total_batches} ({progress_pct:.1f}%): "
                f"{successful}/{len(batch)} successful "
                f"({cached} cached, {downloaded} downloaded) "
                f"- {tiles_per_second:.1f} tiles/sec"
            )
            
            # Brief pause to prevent overwhelming the server
            if batch_num % 10 == 0:
                await asyncio.sleep(0.1)
        
        total_time = time.time() - start_time
        successful_total = len([r for r in all_results if r.get('success')])
        overall_speed = total_tiles / total_time if total_time > 0 else 0
        
        logger.info(
            f"🏁 Pattern complete: {successful_total}/{total_tiles} tiles "
            f"in {total_time:.1f}s ({overall_speed:.1f} tiles/sec)"
        )
        
        return [r for r in all_results if r.get('success')]

    def deg2num(self, lat_deg: float, lon_deg: float, zoom: int) -> Tuple[int, int]:
        """Optimized lat/lon to tile coordinate conversion"""
        lat_rad = math.radians(lat_deg)
        n = 2.0 ** zoom
        x = int((lon_deg + 180.0) / 360.0 * n)
        y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return (x, y)

    def generate_city_tile_coverage(self, lat: float, lng: float, zoom_levels: List[int], radius_km: int = 20) -> Dict:
        """Optimized city coverage generation"""
        city_coverages = {}
        
        for zoom in zoom_levels:
            center_x, center_y = self.deg2num(lat, lng, zoom)
            
            # Calculate radius in tiles
            lat_rad = math.radians(lat)
            meters_per_pixel = 156543.03392 * math.cos(lat_rad) / (2 ** zoom)
            radius_tiles = max(5, min(50, int((radius_km * 1000) / (meters_per_pixel * 256))))
            
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
        
        return city_coverages

    def load_patterns_from_html_extractor(self) -> Dict:
        """Load patterns with performance optimization"""
        patterns_by_city_and_type = {}
        
        # Try comprehensive report first
        html_report_path = 'output_html_patterns/reports/comprehensive_patterns_report.json'
        
        try:
            with open(html_report_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            location_breakdown = data.get('location_breakdown', {})
            
            for location_name, map_types in location_breakdown.items():
                clean_city_name = self.clean_city_name(location_name)
                
                if clean_city_name not in patterns_by_city_and_type:
                    patterns_by_city_and_type[clean_city_name] = {}
                
                for map_type_key, map_data in map_types.items():
                    tile_pattern = map_data.get('tile_url')
                    tile_pattern_2030 = map_data.get('tile_url_2030')
                    
                    patterns = []
                    if tile_pattern:
                        patterns.append(tile_pattern)
                    if tile_pattern_2030:
                        patterns.append(tile_pattern_2030)
                    
                    if patterns:
                        classified_type = self.classify_map_type_from_url(patterns[0])
                        
                        if classified_type not in patterns_by_city_and_type[clean_city_name]:
                            patterns_by_city_and_type[clean_city_name][classified_type] = []
                        
                        patterns_by_city_and_type[clean_city_name][classified_type].extend(patterns)
            
            # Remove duplicates
            for city in patterns_by_city_and_type:
                for map_type in patterns_by_city_and_type[city]:
                    patterns_by_city_and_type[city][map_type] = list(set(patterns_by_city_and_type[city][map_type]))
            
            logger.info(f"✅ Loaded patterns for {len(patterns_by_city_and_type)} cities")
            return patterns_by_city_and_type
            
        except FileNotFoundError:
            logger.error(f"❌ HTML extractor report not found at {html_report_path}")
            return {}

    def classify_map_type_from_url(self, url: str) -> str:
        """Fast URL classification"""
        url_lower = url.lower()
        
        if '2030' in url_lower:
            return 'QH_2030'
        elif '2025' in url_lower or '2024' in url_lower:
            return 'KH_2025'
        elif any(pattern in url_lower for pattern in ['qhc/', 'qhxd3/', 'phan-khu']):
            return 'QH_PHAN_KHU'
        elif any(pattern in url_lower for pattern in ['qhxd/', 'khac']):
            return 'QH_KHAC'
        else:
            return 'UNKNOWN'

    async def ultra_fast_crawl(
        self,
        zoom_levels: List[int] = [10, 12, 14],
        target_map_types: Optional[List[str]] = None,
        target_cities: Optional[List[str]] = None
    ) -> List[Dict]:
        """Ultra-fast crawling with full async processing and KH_2025 district support"""
        
        # Complete Vietnam city coordinates
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
        
        # Load patterns
        patterns_by_city_and_type = self.load_patterns_from_html_extractor()
        
        if not patterns_by_city_and_type:
            logger.error("❌ No patterns found!")
            return []
        
        # Filter targets
        if target_map_types is None:
            target_map_types = [k for k in MAP_TYPE_CONFIG.keys() if k != 'UNKNOWN']
        
        if target_cities:
            patterns_by_city_and_type = {
                city: patterns for city, patterns in patterns_by_city_and_type.items()
                if city in target_cities
            }
        
        all_results = []
        
        for city_name, city_map_patterns in patterns_by_city_and_type.items():
            if city_name not in city_coords:
                logger.warning(f"⚠️ No coordinates found for {city_name}, skipping")
                continue
            
            lat, lng, radius_km = city_coords[city_name]
            logger.info(f"\n🏙️ ULTRA-FAST CRAWL: {city_name.upper()}")
            
            city_coverage = self.generate_city_tile_coverage(lat, lng, zoom_levels, radius_km)
            
            city_results = {
                'city': city_name,
                'coordinates': (lat, lng, radius_km),
                'coverage': city_coverage,
                'map_type_results': {},
                'total_tiles': 0,
                'successful_tiles': 0
            }
            
            # Process each map type
            for map_type, patterns_list in city_map_patterns.items():
                if map_type not in target_map_types:
                    continue
                    
                logger.info(f"🗺️ Processing {map_type} for {city_name}")
                map_type_tiles = []
                
                # FIX: Proper KH_2025 handling with new data structure
                if map_type == 'KH_2025':
                    # Check if we have district-specific data
                    if city_name in self.district_data and self.district_data[city_name]:
                        logger.info(f"🏘️ Found {len(self.district_data[city_name])} districts for {city_name}")
                        
                        districts_with_patterns = 0
                        total_patterns_found = 0
                        
                        for district_clean_name, district_info in self.district_data[city_name].items():
                            district_original_name = district_info['original_name']
                            kh_patterns = district_info.get('kh_2025_patterns', [])
                            
                            logger.info(f"📍 Processing district: {district_original_name}")
                            logger.info(f"   🔗 Patterns found: {len(kh_patterns)}")
                            
                            if not kh_patterns:
                                logger.warning(f"   ⚠️ No patterns for {district_original_name}")
                                continue
                            
                            districts_with_patterns += 1
                            total_patterns_found += len(kh_patterns)
                            
                            # Process each pattern for this district
                            for pattern_url in kh_patterns:
                                logger.info(f"   🌐 Crawling: {pattern_url[:80]}...")
                                
                                try:
                                    district_tiles = await self.crawl_pattern_ultra_fast(
                                        pattern_url, 
                                        city_coverage, 
                                        city_name, 
                                        map_type, 
                                        district_original_name
                                    )
                                    
                                    if district_tiles:
                                        map_type_tiles.extend(district_tiles)
                                        logger.info(f"   ✅ Success: {len(district_tiles)} tiles")
                                    else:
                                        logger.warning(f"   ❌ Failed: No tiles downloaded")
                                        
                                except Exception as e:
                                    logger.error(f"   ❌ Error crawling pattern: {e}")
                        
                        logger.info(f"📊 KH_2025 Summary for {city_name}:")
                        logger.info(f"   🏘️ Districts processed: {districts_with_patterns}/{len(self.district_data[city_name])}")
                        logger.info(f"   🔗 Total patterns: {total_patterns_found}")
                        logger.info(f"   📦 Total tiles: {len(map_type_tiles)}")
                        
                        if districts_with_patterns == 0:
                            logger.error(f"❌ NO DISTRICTS WITH PATTERNS for {city_name}!")
                            # Fallback to city-level patterns if available
                            logger.info(f"🔄 Trying fallback city-level patterns...")
                            for pattern in patterns_list:
                                fallback_tiles = await self.crawl_pattern_ultra_fast(
                                    pattern, city_coverage, city_name, map_type
                                )
                                if fallback_tiles:
                                    map_type_tiles.extend(fallback_tiles)
                    
                    else:
                        logger.warning(f"⚠️ No district data found for {city_name}, using city-level patterns")
                        # Use city-level patterns
                        for pattern in patterns_list:
                            pattern_tiles = await self.crawl_pattern_ultra_fast(
                                pattern, city_coverage, city_name, map_type
                            )
                            if pattern_tiles:
                                map_type_tiles.extend(pattern_tiles)
                
                else:
                    # Regular processing for non-KH_2025
                    for pattern in patterns_list:
                        pattern_tiles = await self.crawl_pattern_ultra_fast(
                            pattern, city_coverage, city_name, map_type
                        )
                        if pattern_tiles:
                            map_type_tiles.extend(pattern_tiles)
                
                # Log final results
                if map_type_tiles:
                    logger.info(f"✅ FINAL: {map_type} for {city_name}: {len(map_type_tiles)} tiles")
                else:
                    logger.error(f"❌ FINAL: {map_type} for {city_name}: NO TILES!")
            
            if city_results['map_type_results']:
                all_results.append(city_results)
        
        return all_results

    def generate_performance_report(self, results: List[Dict], start_time: float) -> Dict:
        """Generate ultra-performance report"""
        elapsed_time = time.time() - start_time
        
        total_tiles = sum(r['successful_tiles'] for r in results)
        total_size_mb = self.stats['total_bytes'] / 1024 / 1024
        
        report = {
            'crawl_type': 'ULTRA-OPTIMIZED Multi-Map Crawl',
            'version': '3.0 - Ultra Performance Edition',
            'timestamp': datetime.now().isoformat(),
            'execution_time_seconds': elapsed_time,
            'execution_time_minutes': elapsed_time / 60,
            'performance_metrics': {
                'cities_processed': len(results),
                'total_tiles_successful': total_tiles,
                'total_size_mb': total_size_mb,
                'tiles_per_second': total_tiles / elapsed_time if elapsed_time > 0 else 0,
                'megabytes_per_second': total_size_mb / elapsed_time if elapsed_time > 0 else 0,
                'cache_hit_rate': (self.stats['cache_hits'] / max(1, self.stats['total_attempted'])) * 100,
                'skip_rate': (self.stats['total_skipped'] / max(1, self.stats['total_attempted'])) * 100
            },
            'optimization_features': [
                'Async/await concurrent downloads',
                'Connection pooling and keep-alive',
                'Smart file existence caching',
                'Memory-efficient streaming',
                'Batch processing optimization',
                'Intelligent retry logic',
                'KH_2025 district-level folder structure'
            ],
            'stats': self.stats.copy(),
            'city_results': results
        }
        
        # Save report
        report_file = f"{self.base_output_dir}/ultra_performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Print performance summary
        print(f"\n🚀 ULTRA-PERFORMANCE CRAWL COMPLETE!")
        print("=" * 60)
        print(f"⏱️  Total time: {elapsed_time/60:.1f} minutes")
        print(f"🏙️  Cities: {len(results)}")
        print(f"📊  Tiles: {total_tiles:,}")
        print(f"💾  Size: {total_size_mb:.1f} MB")
        print(f"⚡  Speed: {report['performance_metrics']['tiles_per_second']:.1f} tiles/sec")
        print(f"🚀  Throughput: {report['performance_metrics']['megabytes_per_second']:.2f} MB/sec")
        print(f"📋  Cache hit rate: {report['performance_metrics']['cache_hit_rate']:.1f}%")
        print(f"⏭️  Skip rate: {report['performance_metrics']['skip_rate']:.1f}%")
        
        logger.info(f"📋 Ultra-performance report saved: {report_file}")
        
        return report

async def main():
    """Ultra-optimized main function"""
    print("🚀 ULTRA-OPTIMIZED TILE DOWNLOADER v3.0")
    print("Maximum performance with async/await and smart caching")
    print("FIXED: Proper tile generation & KH_2025 district structure")
    print("=" * 60)
    
    # Quick setup for maximum performance
    zoom_choice = input("Zoom levels (1=Light[10,12], 2=Standard[10,12,14], 3=Heavy[10-18], default=2): ").strip()
    
    if zoom_choice == '1':
        zoom_levels = [10, 12]
    elif zoom_choice == '3':
        zoom_levels = list(range(10, 19))
    else:
        zoom_levels = [10, 12, 14]
    
    print(f"🎯 Zoom levels: {zoom_levels}")
    
    # Map type selection
    map_choice = input("Map types (A=All, B=QH2030, C=KH2025, D=Both QH2030+KH2025, default=D): ").upper().strip()
    
    if map_choice == 'A':
        target_map_types = [k for k in MAP_TYPE_CONFIG.keys() if k != 'UNKNOWN']
    elif map_choice == 'B':
        target_map_types = ['QH_2030']
    elif map_choice == 'C':
        target_map_types = ['KH_2025']
    else:
        target_map_types = ['QH_2030', 'KH_2025']
    
    # City selection for testing
     # Enhanced city selection with custom input option
    print(f"\n🏙️ City Selection Options:")
    print(f"   1 = Test cities (kontum, laichau, lamdong)")
    print(f"   2 = Major cities (hcm, hanoi, danang, haiphong, cantho)")
    print(f"   3 = All available cities")
    print(f"   4 = Custom cities (enter your own list)")
    
    city_choice = input("Select cities (1/2/3/4, default=1): ").strip()
    
    if city_choice == '2':
        target_cities = ['hcm', 'hanoi', 'danang', 'haiphong', 'cantho']
        print(f"🎯 Selected major cities: {target_cities}")
    elif city_choice == '3':
        target_cities = None
        print(f"🎯 Processing all available cities")
    elif city_choice == '4':
        # Custom city input
        print(f"\n📝 Enter cities (available cities shown below):")
        
        # Show available cities from coordinates
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
        
        # Display available cities in columns for better readability
        print(f"\n📋 Available cities ({len(city_coords)} total):")
        cities_list = sorted(city_coords.keys())
        
        # Print cities in 4 columns
        for i in range(0, len(cities_list), 4):
            row_cities = cities_list[i:i+4]
            formatted_row = [f"{city:<15}" for city in row_cities]
            print(f"   {''.join(formatted_row)}")
        
        print(f"\n💡 Examples:")
        print(f"   - Single: hanoi")
        print(f"   - Multiple: hcm,hanoi,danang")
        print(f"   - With spaces: hcm, hanoi, danang")
        
        custom_input = input("\n🎯 Enter city names (comma-separated): ").strip()
        
        if custom_input:
            # Parse and validate custom cities
            custom_cities = [city.strip().lower() for city in custom_input.split(',')]
            valid_cities = []
            invalid_cities = []
            
            for city in custom_cities:
                if city in city_coords:
                    valid_cities.append(city)
                else:
                    invalid_cities.append(city)
            
            if invalid_cities:
                print(f"⚠️ Invalid cities (ignored): {invalid_cities}")
            
            if valid_cities:
                target_cities = valid_cities
                print(f"✅ Valid cities selected: {target_cities}")
            else:
                print(f"❌ No valid cities found, using default test cities")
                target_cities = ['kontum', 'laichau', 'lamdong']
        else:
            print(f"❌ No cities entered, using default test cities")
            target_cities = ['kontum', 'laichau', 'lamdong']
    else:
        # Default: test cities
        target_cities = ['kontum', 'laichau', 'lamdong']
        print(f"🎯 Selected test cities: {target_cities}")
    
    # Initialize ultra-optimized downloader
    downloader = UltraOptimizedTileDownloader(
        max_workers=50,      # High concurrency
        batch_size=500,      # Large batches
        enable_download=True
    )
    
    # Run ultra-fast crawl
    start_time = time.time()
    
    results = await downloader.ultra_fast_crawl(
        zoom_levels=zoom_levels,
        target_map_types=target_map_types,
        target_cities=target_cities
    )
    
    if results:
        # Generate performance report
        report = downloader.generate_performance_report(results, start_time)
        
        print(f"\n🎉 ULTRA-PERFORMANCE SUCCESS!")
        print(f"📁 Check downloaded_tiles/cities/ for results")
        print(f"📊 Performance report: {report.get('performance_metrics', {})}")
        
        # Show folder structure examples
        print(f"\n📁 Folder structure:")
        print(f"   downloaded_tiles/cities/<city>/qh-2030/<zoom>/")
        print(f"   downloaded_tiles/cities/<city>/kh-2025/<district>/<zoom>/")
        print(f"   downloaded_tiles/cities/<city>/qh-phan-khu/<zoom>/")
        
    else:
        print("❌ No results from ultra-fast crawling")

if __name__ == "__main__":
    # Run with asyncio
    asyncio.run(main())