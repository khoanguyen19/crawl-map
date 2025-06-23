#!/usr/bin/env python3
"""
Digital Ocean Spaces Uploader
Uploads city tiles to Digital Ocean Spaces with resume capability and progress tracking

Author: AI Assistant
Version: 1.0
"""

import os
import json
import boto3
import hashlib
import logging
import mimetypes
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError, NoCredentialsError
from tqdm import tqdm
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spaces_upload.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DigitalOceanSpacesUploader:
    def __init__(self, access_key, secret_key, endpoint_url, bucket_name, region='sgp1'):
        """
        Initialize Digital Ocean Spaces uploader
        
        Args:
            access_key: DO Spaces access key
            secret_key: DO Spaces secret key
            endpoint_url: DO Spaces endpoint (e.g., 'https://sgp1.digitaloceanspaces.com')
            bucket_name: Your bucket/space name
            region: Region code (default: sgp1)
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.bucket_name = bucket_name
        self.region = region
        
        # Initialize boto3 client for DO Spaces
        try:
            self.s3_client = boto3.client(
                's3',
                region_name=region,
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            
            # Test connection
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"✅ Connected to Digital Ocean Spaces: {bucket_name}")
            
        except NoCredentialsError:
            logger.error("❌ Invalid credentials provided")
            raise
        except ClientError as e:
            logger.error(f"❌ Error connecting to Spaces: {e}")
            raise
        
        # Upload statistics
        self.stats = {
            'total_files': 0,
            'uploaded_files': 0,
            'skipped_files': 0,
            'failed_files': 0,
            'total_bytes': 0,
            'uploaded_bytes': 0,
            'start_time': None,
            'end_time': None
        }
        
        # Create resume state file
        self.resume_file = 'upload_resume_state.json'
        self.uploaded_files = self.load_resume_state()

    def load_resume_state(self):
        """Load resume state from file"""
        try:
            if os.path.exists(self.resume_file):
                with open(self.resume_file, 'r') as f:
                    data = json.load(f)
                    logger.info(f"📋 Loaded resume state: {len(data)} files already uploaded")
                    return set(data)
        except Exception as e:
            logger.warning(f"⚠️ Could not load resume state: {e}")
        
        return set()

    def save_resume_state(self):
        """Save current upload state for resume"""
        try:
            with open(self.resume_file, 'w') as f:
                json.dump(list(self.uploaded_files), f, indent=2)
        except Exception as e:
            logger.warning(f"⚠️ Could not save resume state: {e}")

    def get_file_info(self, file_path):
        """Get file information including size and MD5 hash"""
        try:
            stat = os.stat(file_path)
            size = stat.st_size
            
            # Calculate MD5 hash for integrity check
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            
            return {
                'size': size,
                'md5': hash_md5.hexdigest(),
                'modified_time': stat.st_mtime
            }
        except Exception as e:
            logger.error(f"❌ Error getting file info for {file_path}: {e}")
            return None

    def file_exists_in_spaces(self, s3_key):
        """Check if file already exists in Spaces"""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.warning(f"⚠️ Error checking file existence: {e}")
                return False

    def upload_single_file(self, local_path, s3_key, file_info=None):
        """Upload a single file to Spaces with public ACL - WORKING VERSION"""
        try:
            # Skip if already uploaded (resume functionality)
            resume_key = f"{s3_key}:{file_info['md5'] if file_info else 'unknown'}"
            if resume_key in self.uploaded_files:
                logger.debug(f"⏭️ Skipping already uploaded: {s3_key}")
                self.stats['skipped_files'] += 1
                return {
                    'success': True,
                    'skipped': True,
                    'file': s3_key,
                    'size': file_info['size'] if file_info else 0
                }
            
            # Check if file exists in Spaces (additional safety check)
            if self.file_exists_in_spaces(s3_key):
                logger.debug(f"⏭️ File exists in Spaces: {s3_key}")
                self.uploaded_files.add(resume_key)
                self.stats['skipped_files'] += 1
                return {
                    'success': True,
                    'skipped': True,
                    'file': s3_key,
                    'size': file_info['size'] if file_info else 0
                }
            
            # Get file info if not provided
            if not file_info:
                file_info = self.get_file_info(local_path)
                if not file_info:
                    return {
                        'success': False,
                        'error': 'Could not get file info',
                        'file': s3_key
                    }
            
            # Determine content type
            content_type, _ = mimetypes.guess_type(local_path)
            if not content_type:
                if local_path.lower().endswith('.png'):
                    content_type = 'image/png'
                elif local_path.lower().endswith(('.jpg', '.jpeg')):
                    content_type = 'image/jpeg'
                elif local_path.lower().endswith('.webp'):
                    content_type = 'image/webp'
                else:
                    content_type = 'application/octet-stream'
            
            # 🆕 WORKING METHOD: Upload with public ACL and optimization headers
            extra_args = {
                'ACL': 'public-read',                    # ✅ MAKE FILES PUBLICLY ACCESSIBLE
                'ContentType': content_type,             # ✅ PROPER CONTENT TYPE  
                'CacheControl': 'max-age=31536000, public',  # ✅ CACHE 1 YEAR
                'ContentDisposition': 'inline',          # ✅ DISPLAY IN BROWSER
                'Metadata': {
                    'original-name': os.path.basename(local_path),
                    'upload-time': datetime.now().isoformat(),
                    'md5-hash': file_info['md5'],
                    'tile-type': 'map-tile',
                    'public-access': 'enabled'           # ✅ MARK AS PUBLIC
                }
            }
            
            # 🚀 UPLOAD WITH PUBLIC ACL (tested and working!)
            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            # Mark as uploaded
            self.uploaded_files.add(resume_key)
            self.stats['uploaded_files'] += 1
            self.stats['uploaded_bytes'] += file_info['size']
            
            # Generate public URLs
            direct_url = f"https://{self.bucket_name}.{self.region}.digitaloceanspaces.com/{s3_key}"
            cdn_url = f"https://{self.bucket_name}.{self.region}.cdn.digitaloceanspaces.com/{s3_key}"
            
            logger.debug(f"✅ Uploaded PUBLIC: {s3_key} ({file_info['size']} bytes)")
            logger.debug(f"🔗 Direct URL: {direct_url}")
            
            return {
                'success': True,
                'file': s3_key,
                'size': file_info['size'],
                'content_type': content_type,
                'public_url': direct_url,
                'cdn_url': cdn_url,
                'acl_public': True
            }
            
        except Exception as e:
            logger.error(f"❌ Error uploading {local_path}: {e}")
            self.stats['failed_files'] += 1
            return {
                'success': False,
                'error': str(e),
                'file': s3_key
            }

    def scan_directory(self, local_dir, s3_prefix=''):
        """Scan directory and prepare file list for upload"""
        files_to_upload = []
        
        logger.info(f"🔍 Scanning directory: {local_dir}")
        
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                local_path = os.path.join(root, file)
                
                # Skip hidden files and logs
                if file.startswith('.') or file.endswith('.log'):
                    continue
                
                # Create S3 key maintaining directory structure
                rel_path = os.path.relpath(local_path, local_dir)
                if s3_prefix:
                    s3_key = os.path.join(s3_prefix, rel_path).replace('\\', '/')
                else:
                    s3_key = rel_path.replace('\\', '/')
                
                # Get file info
                file_info = self.get_file_info(local_path)
                if file_info:
                    files_to_upload.append({
                        'local_path': local_path,
                        's3_key': s3_key,
                        'file_info': file_info
                    })
                    self.stats['total_files'] += 1
                    self.stats['total_bytes'] += file_info['size']
        
        logger.info(f"📊 Found {len(files_to_upload)} files to process")
        logger.info(f"📊 Total size: {self.stats['total_bytes'] / 1024 / 1024:.1f} MB")
        
        return files_to_upload

    def upload_directory(self, local_dir, s3_prefix='', max_workers=5):
        """Upload entire directory to Spaces with parallel processing"""
        
        logger.info(f"🚀 Starting upload to Digital Ocean Spaces")
        logger.info(f"📁 Local directory: {local_dir}")
        logger.info(f"🪣 Bucket: {self.bucket_name}")
        logger.info(f"📂 S3 prefix: {s3_prefix if s3_prefix else '(none - direct to bucket root)'}")
        logger.info(f"👥 Max workers: {max_workers}")
        
        self.stats['start_time'] = time.time()
        
        # Scan directory
        files_to_upload = self.scan_directory(local_dir, s3_prefix)
        
        if not files_to_upload:
            logger.warning("⚠️ No files found to upload")
            return
        
        # Upload files in parallel
        logger.info(f"📤 Starting parallel upload...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create progress bar
            with tqdm(total=len(files_to_upload), desc="Uploading files", unit="file") as pbar:
                
                # Submit all upload tasks
                future_to_file = {
                    executor.submit(
                        self.upload_single_file,
                        file_data['local_path'],
                        file_data['s3_key'],
                        file_data['file_info']
                    ): file_data for file_data in files_to_upload
                }
                
                # Process completed uploads
                for future in as_completed(future_to_file):
                    file_data = future_to_file[future]
                    try:
                        result = future.result()
                        
                        if result['success']:
                            if result.get('skipped'):
                                pbar.set_postfix(status=f"Skipped: {result['file']}")
                            else:
                                pbar.set_postfix(status=f"Uploaded: {result['file']}")
                        else:
                            pbar.set_postfix(status=f"Failed: {result['file']}")
                            logger.error(f"❌ Upload failed: {result['file']} - {result.get('error', 'Unknown error')}")
                        
                        pbar.update(1)
                        
                        # Save resume state periodically
                        if (self.stats['uploaded_files'] + self.stats['skipped_files']) % 100 == 0:
                            self.save_resume_state()
                            
                    except Exception as e:
                        logger.error(f"❌ Task error for {file_data['s3_key']}: {e}")
                        pbar.update(1)
        
        # Final save of resume state
        self.save_resume_state()
        
        self.stats['end_time'] = time.time()
        self.generate_upload_report()

    def generate_upload_report(self):
        """Generate upload completion report"""
        elapsed_time = self.stats['end_time'] - self.stats['start_time']
        
        # Calculate rates
        upload_rate_mbps = (self.stats['uploaded_bytes'] / 1024 / 1024) / elapsed_time if elapsed_time > 0 else 0
        files_per_second = self.stats['uploaded_files'] / elapsed_time if elapsed_time > 0 else 0
        
        report = {
            'upload_summary': {
                'total_files': self.stats['total_files'],
                'uploaded_files': self.stats['uploaded_files'],
                'skipped_files': self.stats['skipped_files'],
                'failed_files': self.stats['failed_files'],
                'success_rate': (self.stats['uploaded_files'] / self.stats['total_files'] * 100) if self.stats['total_files'] > 0 else 0
            },
            'data_transfer': {
                'total_size_mb': self.stats['total_bytes'] / 1024 / 1024,
                'uploaded_size_mb': self.stats['uploaded_bytes'] / 1024 / 1024,
                'upload_rate_mbps': upload_rate_mbps
            },
            'performance': {
                'elapsed_time_seconds': elapsed_time,
                'elapsed_time_minutes': elapsed_time / 60,
                'files_per_second': files_per_second,
                'start_time': datetime.fromtimestamp(self.stats['start_time']).isoformat(),
                'end_time': datetime.fromtimestamp(self.stats['end_time']).isoformat()
            },
            'spaces_info': {
                'bucket': self.bucket_name,
                'endpoint': self.endpoint_url,
                'region': self.region
            }
        }
        
        # Save report
        report_file = f"upload_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Print summary
        print(f"\n🎉 UPLOAD COMPLETE!")
        print("=" * 50)
        print(f"⏱️ Duration: {elapsed_time/60:.1f} minutes")
        print(f"📁 Total files: {self.stats['total_files']:,}")
        print(f"✅ Uploaded: {self.stats['uploaded_files']:,}")
        print(f"⏭️ Skipped: {self.stats['skipped_files']:,}")
        print(f"❌ Failed: {self.stats['failed_files']:,}")
        print(f"📊 Success rate: {report['upload_summary']['success_rate']:.1f}%")
        print(f"💾 Total size: {report['data_transfer']['total_size_mb']:.1f} MB")
        print(f"📈 Upload rate: {upload_rate_mbps:.1f} MB/s")
        print(f"🪣 Destination: {self.bucket_name}")
        print(f"📋 Report saved: {report_file}")
        
        if self.stats['failed_files'] > 0:
            print(f"\n⚠️ {self.stats['failed_files']} files failed to upload")
            print("Check the log file for details: spaces_upload.log")
        
        logger.info(f"📋 Upload report saved: {report_file}")

    def cleanup_resume_state(self):
        """Clean up resume state file after successful upload"""
        try:
            if os.path.exists(self.resume_file):
                os.remove(self.resume_file)
                logger.info("🧹 Cleaned up resume state file")
        except Exception as e:
            logger.warning(f"⚠️ Could not clean up resume state: {e}")

    def list_spaces_contents(self, prefix='', max_keys=1000):
        """List contents of Spaces bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' in response:
                contents = response['Contents']
                logger.info(f"📋 Found {len(contents)} objects in Spaces")
                
                for obj in contents[:10]:  # Show first 10
                    size_mb = obj['Size'] / 1024 / 1024
                    print(f"  📄 {obj['Key']} ({size_mb:.2f} MB)")
                
                if len(contents) > 10:
                    print(f"  ... and {len(contents) - 10} more files")
                
                return contents
            else:
                logger.info("📋 No objects found in Spaces")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error listing Spaces contents: {e}")
            return []

    def check_city_exists_in_spaces(self, city_name, s3_prefix=''):
        """
        Check if a city folder already exists in Spaces
        
        Args:
            city_name: Name of the city (e.g., 'hanoi', 'hcmc')
            s3_prefix: S3 prefix if any
            
        Returns:
            dict: {
                'exists': bool,
                'file_count': int,
                'total_size': int,
                'sample_files': list
            }
        """
        try:
            # Construct the city prefix
            if s3_prefix:
                city_prefix = f"{s3_prefix}/{city_name}/"
            else:
                city_prefix = f"{city_name}/"
            
            logger.info(f"🔍 Checking if city exists: {city_prefix}")
            
            # List objects with city prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=city_prefix,
                MaxKeys=1000  # Adjust based on expected files per city
            )
            
            if 'Contents' not in response:
                return {
                    'exists': False,
                    'file_count': 0,
                    'total_size': 0,
                    'sample_files': []
                }
            
            contents = response['Contents']
            total_size = sum(obj['Size'] for obj in contents)
            sample_files = [obj['Key'] for obj in contents[:5]]  # First 5 files as sample
            
            return {
                'exists': True,
                'file_count': len(contents),
                'total_size': total_size,
                'sample_files': sample_files
            }
            
        except Exception as e:
            logger.error(f"❌ Error checking city existence: {e}")
            return {
                'exists': False,
                'file_count': 0,
                'total_size': 0,
                'sample_files': []
            }

    def scan_cities_in_local_directory(self, local_dir):
        """
        Scan local directory to identify individual city folders
        
        Args:
            local_dir: Local directory path
            
        Returns:
            list: List of city folder names
        """
        cities = []
        
        if not os.path.exists(local_dir):
            logger.error(f"❌ Local directory not found: {local_dir}")
            return cities
        
        # Assume city folders are direct subdirectories
        for item in os.listdir(local_dir):
            item_path = os.path.join(local_dir, item)
            if os.path.isdir(item_path):
                cities.append(item)
        
        logger.info(f"🏙️ Found {len(cities)} cities in local directory: {', '.join(cities)}")
        return cities

    def filter_existing_cities(self, local_dir, s3_prefix='', skip_existing=True):
        """
        Filter out cities that already exist in Spaces
        
        Args:
            local_dir: Local directory containing city folders
            s3_prefix: S3 prefix if any
            skip_existing: If True, skip cities that already exist
            
        Returns:
            dict: {
                'cities_to_upload': list,
                'existing_cities': list,
                'city_status': dict
            }
        """
        cities = self.scan_cities_in_local_directory(local_dir)
        cities_to_upload = []
        existing_cities = []
        city_status = {}
        
        print(f"\n🔍 CHECKING CITY EXISTENCE IN SPACES")
        print("=" * 40)
        
        for city in cities:
            city_info = self.check_city_exists_in_spaces(city, s3_prefix)
            city_status[city] = city_info
            
            if city_info['exists']:
                existing_cities.append(city)
                size_mb = city_info['total_size'] / 1024 / 1024
                print(f"✅ {city}: EXISTS ({city_info['file_count']} files, {size_mb:.1f} MB)")
                if not skip_existing:
                    cities_to_upload.append(city)
            else:
                cities_to_upload.append(city)
                print(f"🆕 {city}: NOT FOUND - will upload")
        
        print(f"\n📊 SUMMARY:")
        print(f"  🏙️ Total cities found: {len(cities)}")
        print(f"  ✅ Already exist: {len(existing_cities)}")
        print(f"  📤 To upload: {len(cities_to_upload)}")
        
        if existing_cities and skip_existing:
            print(f"\n⏭️ SKIPPING: {', '.join(existing_cities)}")
        
        return {
            'cities_to_upload': cities_to_upload,
            'existing_cities': existing_cities,
            'city_status': city_status
        }

    def upload_directory_with_city_filter(self, local_dir, s3_prefix='', max_workers=5, skip_existing_cities=True):
        """
        Upload directory but skip cities that already exist in Spaces
        
        Args:
            local_dir: Local directory to upload
            s3_prefix: S3 prefix
            max_workers: Number of parallel workers
            skip_existing_cities: Whether to skip existing cities
        """
        
        logger.info(f"🚀 Starting upload with city filtering")
        logger.info(f"📁 Local directory: {local_dir}")
        logger.info(f"🪣 Bucket: {self.bucket_name}")
        logger.info(f"📂 S3 prefix: {s3_prefix if s3_prefix else '(none - direct to bucket root)'}")
        logger.info(f"🏙️ Skip existing cities: {skip_existing_cities}")
        
        self.stats['start_time'] = time.time()
        
        # Filter cities
        filter_result = self.filter_existing_cities(local_dir, s3_prefix, skip_existing_cities)
        
        if not filter_result['cities_to_upload']:
            logger.warning("⚠️ No cities to upload (all may already exist)")
            return
        
        # Scan only the cities we want to upload
        files_to_upload = []
        
        for city in filter_result['cities_to_upload']:
            city_path = os.path.join(local_dir, city)
            logger.info(f"🔍 Scanning city: {city}")
            
            city_files = self.scan_directory(city_path, os.path.join(s3_prefix, city) if s3_prefix else city)
            files_to_upload.extend(city_files)
        
        if not files_to_upload:
            logger.warning("⚠️ No files found to upload")
            return
        
        # Continue with normal upload process
        logger.info(f"📤 Starting parallel upload of {len(files_to_upload)} files...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with tqdm(total=len(files_to_upload), desc="Uploading files", unit="file") as pbar:
                
                future_to_file = {
                    executor.submit(
                        self.upload_single_file,
                        file_data['local_path'],
                        file_data['s3_key'],
                        file_data['file_info']
                    ): file_data for file_data in files_to_upload
                }
                
                for future in as_completed(future_to_file):
                    file_data = future_to_file[future]
                    try:
                        result = future.result()
                        
                        if result['success']:
                            if result.get('skipped'):
                                pbar.set_postfix(status=f"Skipped: {result['file']}")
                            else:
                                pbar.set_postfix(status=f"Uploaded: {result['file']}")
                        else:
                            pbar.set_postfix(status=f"Failed: {result['file']}")
                            logger.error(f"❌ Upload failed: {result['file']} - {result.get('error', 'Unknown error')}")
                        
                        pbar.update(1)
                        
                        if (self.stats['uploaded_files'] + self.stats['skipped_files']) % 100 == 0:
                            self.save_resume_state()
                            
                    except Exception as e:
                        logger.error(f"❌ Task error for {file_data['s3_key']}: {e}")
                        pbar.update(1)
        
        self.save_resume_state()
        self.stats['end_time'] = time.time()
        self.generate_upload_report()
        
        # Show final city status
        print(f"\n🏙️ FINAL CITY STATUS:")
        print("=" * 25)
        for city, status in filter_result['city_status'].items():
            if city in filter_result['cities_to_upload']:
                print(f"  📤 {city}: UPLOADED")
            else:
                size_mb = status['total_size'] / 1024 / 1024
                print(f"  ⏭️ {city}: SKIPPED ({status['file_count']} files, {size_mb:.1f} MB)")

    def interactive_city_selection(self, local_dir, s3_prefix=''):
        """
        Interactive mode to let user choose which cities to upload
        """
        filter_result = self.filter_existing_cities(local_dir, s3_prefix, skip_existing=False)
        
        if not filter_result['existing_cities']:
            print("🆕 No existing cities found - all will be uploaded")
            return filter_result['cities_to_upload']
        
        print(f"\n🤔 INTERACTIVE CITY SELECTION:")
        print("=" * 35)
        print("Found existing cities in Spaces. Choose action:")
        print("1. ⏭️ Skip all existing cities (recommended)")
        print("2. 🔄 Re-upload all cities (overwrite)")
        print("3. 🎯 Select specific cities to upload")
        
        while True:
            choice = input("\nChoose option (1/2/3, default: 1): ").strip()
            
            if choice == '' or choice == '1':
                return filter_result['cities_to_upload']
            
            elif choice == '2':
                print("⚠️ This will re-upload ALL cities (including existing ones)")
                confirm = input("Are you sure? (y/n): ").lower()
                if confirm == 'y':
                    return self.scan_cities_in_local_directory(local_dir)
                else:
                    continue
            
            elif choice == '3':
                selected_cities = []
                all_cities = self.scan_cities_in_local_directory(local_dir)
                
                print(f"\n🎯 SELECT CITIES TO UPLOAD:")
                for i, city in enumerate(all_cities, 1):
                    status = filter_result['city_status'][city]
                    if status['exists']:
                        size_mb = status['total_size'] / 1024 / 1024
                        print(f"{i:2d}. {city} (EXISTS: {status['file_count']} files, {size_mb:.1f} MB)")
                    else:
                        print(f"{i:2d}. {city} (NEW)")
                
                selection = input(f"\nEnter city numbers (1-{len(all_cities)}, comma-separated): ").strip()
                try:
                    indices = [int(x.strip()) - 1 for x in selection.split(',')]
                    selected_cities = [all_cities[i] for i in indices if 0 <= i < len(all_cities)]
                    
                    if selected_cities:
                        print(f"✅ Selected cities: {', '.join(selected_cities)}")
                        return selected_cities
                    else:
                        print("❌ No valid cities selected")
                        continue
                except ValueError:
                    print("❌ Invalid input format")
                    continue
            
            else:
                print("❌ Invalid choice. Please enter 1, 2, or 3")
                continue

def load_config():
    """Load configuration from file or environment variables"""
    config = {}
    
    # Try to load from config file
    config_file = 'spaces_config.json'
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            logger.info(f"📋 Loaded config from {config_file}")
        except Exception as e:
            logger.warning(f"⚠️ Could not load config file: {e}")
    
    # Override with environment variables if available
    config['access_key'] = os.getenv('DO_SPACES_ACCESS_KEY', config.get('access_key'))
    config['secret_key'] = os.getenv('DO_SPACES_SECRET_KEY', config.get('secret_key'))
    config['endpoint_url'] = os.getenv('DO_SPACES_ENDPOINT', config.get('endpoint_url'))
    config['bucket_name'] = os.getenv('DO_SPACES_BUCKET', config.get('bucket_name'))
    config['region'] = os.getenv('DO_SPACES_REGION', config.get('region', 'sgp1'))
    
    return config

def create_sample_config():
        """Create a sample configuration file"""
        sample_config = {
            "access_key": "YOUR_DO_SPACES_ACCESS_KEY",
            "secret_key": "YOUR_DO_SPACES_SECRET_KEY",
            "endpoint_url": "https://sgp1.digitaloceanspaces.com",
            "bucket_name": "your-bucket-name",
            "region": "sgp1"
        }
        
        config_file = 'spaces_config.json'
        with open(config_file, 'w') as f:
            json.dump(sample_config, f, indent=2)
        
        print(f"📝 Created sample config file: {config_file}")
        print("Please edit this file with your actual Digital Ocean Spaces credentials")

def main():
    print("🚀 DIGITAL OCEAN SPACES UPLOADER")
    print("Upload city tiles to Digital Ocean Spaces")
    print("=" * 50)
    
    # Load configuration
    config = load_config()
    
    # Check if we have required config
    required_keys = ['access_key', 'secret_key', 'endpoint_url', 'bucket_name']
    missing_keys = [key for key in required_keys if not config.get(key)]
    
    if missing_keys:
        print(f"❌ Missing configuration: {', '.join(missing_keys)}")
        print("\nOptions:")
        print("1. Create a spaces_config.json file with your credentials")
        print("2. Set environment variables:")
        print("   DO_SPACES_ACCESS_KEY")
        print("   DO_SPACES_SECRET_KEY") 
        print("   DO_SPACES_ENDPOINT")
        print("   DO_SPACES_BUCKET")
        print("   DO_SPACES_REGION (optional)")
        
        create_config = input("\nCreate sample config file? (y/n): ").lower()
        if create_config == 'y':
            create_sample_config()
        return
    
    print(f"🪣 Bucket: {config['bucket_name']}")
    print(f"🌐 Endpoint: {config['endpoint_url']}")
    print(f"🗺️ Region: {config['region']}")
    
    # Get directory to upload
    default_dir = "downloaded_tiles/cities"
    local_dir = input(f"\nLocal directory to upload (default: {default_dir}): ").strip()
    if not local_dir:
        local_dir = default_dir
    
    if not os.path.exists(local_dir):
        print(f"❌ Directory not found: {local_dir}")
        return
    
    # Get S3 prefix with clear options
    print(f"\n📂 S3 PREFIX CONFIGURATION:")
    print("=" * 30)
    print("1. 📁 Use prefix: 'guland-tiles' (recommended)")
    print("   → Result: bucket/guland-tiles/hanoi/qh-2030/12/tile.png")
    print("2. 🚫 No prefix: direct to bucket root")
    print("   → Result: bucket/hanoi/qh-2030/12/tile.png")
    print("3. ✏️ Custom prefix")
    print("   → Result: bucket/your-prefix/hanoi/qh-2030/12/tile.png")
    
    while True:
        prefix_choice = input("\nChoose option (1/2/3, default: 1): ").strip()
        
        if prefix_choice == '' or prefix_choice == '1':
            s3_prefix = 'guland-tiles'
            print(f"✅ Using prefix: '{s3_prefix}'")
            break
        elif prefix_choice == '2':
            s3_prefix = ''
            print("✅ No prefix selected - uploading directly to bucket root")
            print("⚠️  WARNING: Files will be at bucket root level")
            break
        elif prefix_choice == '3':
            custom_prefix = input("Enter custom prefix (or leave empty for no prefix): ").strip()
            s3_prefix = custom_prefix
            if s3_prefix:
                print(f"✅ Using custom prefix: '{s3_prefix}'")
            else:
                print("✅ No prefix selected - uploading directly to bucket root")
            break
        else:
            print("❌ Invalid choice. Please enter 1, 2, or 3")
            continue
    
    # 🆕 NEW: City existence checking options
    print(f"\n🏙️ CITY EXISTENCE CHECKING:")
    print("=" * 30)
    print("1. 🚀 Auto-skip existing cities (recommended)")
    print("   → Automatically skip cities that already exist in Spaces")
    print("2. 🤔 Interactive selection")
    print("   → Let me choose which cities to upload")
    print("3. 🔄 Upload all (ignore existing)")
    print("   → Upload everything, overwrite existing files")
    print("4. ⚡ Classic mode (no city filtering)")
    print("   → Original behavior - check individual files only")
    
    city_mode = ''
    while True:
        city_choice = input("\nChoose city handling mode (1/2/3/4, default: 1): ").strip()
        
        if city_choice == '' or city_choice == '1':
            city_mode = 'auto_skip'
            print("✅ Auto-skip mode: Will skip cities that already exist")
            break
        elif city_choice == '2':
            city_mode = 'interactive'
            print("✅ Interactive mode: You'll choose which cities to upload")
            break
        elif city_choice == '3':
            city_mode = 'upload_all'
            print("✅ Upload all mode: Will upload everything (may overwrite)")
            break
        elif city_choice == '4':
            city_mode = 'classic'
            print("✅ Classic mode: Original file-by-file checking")
            break
        else:
            print("❌ Invalid choice. Please enter 1, 2, 3, or 4")
            continue
    
    # Get max workers
    max_workers_input = input("\nMax parallel uploads (default: 5): ").strip()
    try:
        max_workers = int(max_workers_input) if max_workers_input else 5
    except ValueError:
        max_workers = 5
    
    # Show final configuration
    print(f"\n📋 FINAL UPLOAD CONFIGURATION:")
    print("=" * 35)
    print(f"  📁 Local directory: {local_dir}")
    print(f"  🪣 Bucket: {config['bucket_name']}")
    if s3_prefix:
        print(f"  📂 S3 prefix: {s3_prefix}")
        print(f"  🔗 Example URL: https://{config['bucket_name']}.{config['region']}.digitaloceanspaces.com/{s3_prefix}/hanoi/qh-2030/12/tile.png")
    else:
        print(f"  📂 S3 prefix: (none - direct to root)")
        print(f"  🔗 Example URL: https://{config['bucket_name']}.{config['region']}.digitaloceanspaces.com/hanoi/qh-2030/12/tile.png")
    print(f"  👥 Max workers: {max_workers}")
    print(f"  🏙️ City mode: {city_mode}")
    
    confirm = input("\n✅ Proceed with upload? (y/n): ").lower()
    if confirm != 'y':
        print("❌ Upload cancelled")
        return
    
    try:
        # Initialize uploader
        print("\n🔧 Initializing uploader...")
        uploader = DigitalOceanSpacesUploader(
            access_key=config['access_key'],
            secret_key=config['secret_key'],
            endpoint_url=config['endpoint_url'],
            bucket_name=config['bucket_name'],
            region=config['region']
        )
        
        # Choose upload method based on city mode
        print("🚀 Starting upload process...")
        
        if city_mode == 'classic':
            # Original behavior
            uploader.upload_directory(local_dir, s3_prefix, max_workers)
            
        elif city_mode == 'auto_skip':
            # Auto-skip existing cities
            uploader.upload_directory_with_city_filter(
                local_dir, s3_prefix, max_workers, skip_existing_cities=True
            )
            
        elif city_mode == 'upload_all':
            # Upload all cities (no filtering)
            uploader.upload_directory_with_city_filter(
                local_dir, s3_prefix, max_workers, skip_existing_cities=False
            )
            
        elif city_mode == 'interactive':
            # Interactive city selection
            selected_cities = uploader.interactive_city_selection(local_dir, s3_prefix)
            
            if not selected_cities:
                print("❌ No cities selected for upload")
                return
            
            # Create temporary filtered directory structure
            # Or modify the upload process to only handle selected cities
            print(f"📤 Uploading selected cities: {', '.join(selected_cities)}")
            
            # Use the city filter method with custom city list
            files_to_upload = []
            for city in selected_cities:
                city_path = os.path.join(local_dir, city)
                if os.path.exists(city_path):
                    city_files = uploader.scan_directory(
                        city_path, 
                        os.path.join(s3_prefix, city) if s3_prefix else city
                    )
                    files_to_upload.extend(city_files)
            
            if files_to_upload:
                # Manual upload process for selected cities
                uploader.stats['start_time'] = time.time()
                uploader.stats['total_files'] = len(files_to_upload)
                uploader.stats['total_bytes'] = sum(f['file_info']['size'] for f in files_to_upload)
                
                print(f"📤 Starting upload of {len(files_to_upload)} files...")
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    with tqdm(total=len(files_to_upload), desc="Uploading files", unit="file") as pbar:
                        
                        future_to_file = {
                            executor.submit(
                                uploader.upload_single_file,
                                file_data['local_path'],
                                file_data['s3_key'],
                                file_data['file_info']
                            ): file_data for file_data in files_to_upload
                        }
                        
                        for future in as_completed(future_to_file):
                            file_data = future_to_file[future]
                            try:
                                result = future.result()
                                
                                if result['success']:
                                    if result.get('skipped'):
                                        pbar.set_postfix(status=f"Skipped: {result['file']}")
                                    else:
                                        pbar.set_postfix(status=f"Uploaded: {result['file']}")
                                else:
                                    pbar.set_postfix(status=f"Failed: {result['file']}")
                                
                                pbar.update(1)
                                
                                if (uploader.stats['uploaded_files'] + uploader.stats['skipped_files']) % 100 == 0:
                                    uploader.save_resume_state()
                                    
                            except Exception as e:
                                logger.error(f"❌ Task error for {file_data['s3_key']}: {e}")
                                pbar.update(1)
                
                uploader.save_resume_state()
                uploader.stats['end_time'] = time.time()
                uploader.generate_upload_report()
        
        # Optional: Clean up resume state on successful completion
        if uploader.stats['failed_files'] == 0:
            cleanup = input("\n🧹 Clean up resume state file? (y/n): ").lower()
            if cleanup == 'y':
                uploader.cleanup_resume_state()
        
        # Optional: List uploaded files
        list_files = input("\n📋 List uploaded files? (y/n): ").lower()
        if list_files == 'y':
            uploader.list_spaces_contents(s3_prefix if s3_prefix else '')
        
    except Exception as e:
        logger.error(f"❌ Upload failed: {e}")
        print(f"❌ Upload failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()