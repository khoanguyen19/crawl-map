#!/usr/bin/env python3
"""
Digital Ocean Spaces ACL Fixer
Fixes missing public ACL permissions for uploaded objects

This script scans your Digital Ocean Spaces bucket and ensures all objects
have proper public-read ACL permissions. Useful when uploads had ACL issues.

Author: AI Assistant
Version: 1.0
"""

import os
import json
import boto3
import logging
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError, NoCredentialsError
from tqdm import tqdm
import argparse
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('spaces_acl_fix.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DigitalOceanSpacesACLFixer:
    def __init__(self, access_key, secret_key, endpoint_url, bucket_name, region='sgp1'):
        """
        Initialize Digital Ocean Spaces ACL Fixer
        
        Args:
            access_key: DO Spaces access key
            secret_key: DO Spaces secret key
            endpoint_url: DO Spaces endpoint URL
            bucket_name: Target bucket name
            region: DO Spaces region
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.bucket_name = bucket_name
        self.region = region
        
        # Initialize boto3 client
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
        
        # Statistics tracking
        self.stats = {
            'total_objects': 0,
            'checked_objects': 0,
            'public_objects': 0,
            'private_objects': 0,
            'fixed_objects': 0,
            'failed_objects': 0,
            'start_time': None,
            'end_time': None,
            'total_size_bytes': 0,
            'fixed_size_bytes': 0
        }
        
        # Rate limiting
        self.api_call_count = 0
        self.last_api_reset = time.time()
        self.max_api_calls_per_second = 100  # Conservative limit
        
        # Resume state
        self.resume_file = 'acl_fix_resume_state.json'
        self.processed_objects = self.load_resume_state()

    def load_resume_state(self):
        """Load resume state from file"""
        try:
            if os.path.exists(self.resume_file):
                with open(self.resume_file, 'r') as f:
                    data = json.load(f)
                    processed = set(data.get('processed_objects', []))
                    logger.info(f"📋 Loaded resume state: {len(processed)} objects already processed")
                    return processed
        except Exception as e:
            logger.warning(f"⚠️ Could not load resume state: {e}")
        
        return set()

    def save_resume_state(self):
        """Save resume state for recovery"""
        try:
            resume_data = {
                'processed_objects': list(self.processed_objects),
                'stats': self.stats.copy(),
                'timestamp': datetime.now().isoformat()
            }
            
            with open(self.resume_file, 'w') as f:
                json.dump(resume_data, f, indent=2)
                
        except Exception as e:
            logger.warning(f"⚠️ Could not save resume state: {e}")

    def rate_limit_check(self):
        """Implement rate limiting to avoid throttling"""
        current_time = time.time()
        
        # Reset counter every second
        if current_time - self.last_api_reset >= 1.0:
            self.api_call_count = 0
            self.last_api_reset = current_time
        
        # If approaching limit, sleep briefly
        if self.api_call_count >= self.max_api_calls_per_second:
            sleep_time = 1.0 - (current_time - self.last_api_reset)
            if sleep_time > 0:
                time.sleep(sleep_time)
                self.api_call_count = 0
                self.last_api_reset = time.time()
        
        self.api_call_count += 1

    def check_object_acl(self, object_key):
        """
        Check if object has public-read ACL
        
        Returns:
            dict: {
                'is_public': bool,
                'grantees': list,
                'error': str or None
            }
        """
        try:
            self.rate_limit_check()
            
            # Get object ACL
            response = self.s3_client.get_object_acl(
                Bucket=self.bucket_name,
                Key=object_key
            )
            
            grants = response.get('Grants', [])
            is_public = False
            grantees = []
            
            # Check for public read permission
            for grant in grants:
                grantee = grant.get('Grantee', {})
                permission = grant.get('Permission', '')
                
                # Store grantee info
                grantee_info = {
                    'type': grantee.get('Type', ''),
                    'permission': permission
                }
                
                if grantee.get('URI'):
                    grantee_info['uri'] = grantee['URI']
                if grantee.get('DisplayName'):
                    grantee_info['display_name'] = grantee['DisplayName']
                
                grantees.append(grantee_info)
                
                # Check if this grant gives public read access
                if (grantee.get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers' and 
                    permission == 'READ'):
                    is_public = True
            
            return {
                'is_public': is_public,
                'grantees': grantees,
                'error': None
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            return {
                'is_public': False,
                'grantees': [],
                'error': f"ClientError {error_code}: {e.response['Error']['Message']}"
            }
        except Exception as e:
            return {
                'is_public': False,
                'grantees': [],
                'error': f"Unexpected error: {str(e)}"
            }

    def set_object_public_acl(self, object_key):
        """
        Set object to have public-read ACL
        
        Returns:
            dict: {
                'success': bool,
                'error': str or None
            }
        """
        try:
            self.rate_limit_check()
            
            # Set public-read ACL
            self.s3_client.put_object_acl(
                Bucket=self.bucket_name,
                Key=object_key,
                ACL='public-read'
            )
            
            logger.debug(f"✅ Fixed ACL for: {object_key}")
            
            return {
                'success': True,
                'error': None
            }
            
        except ClientError as e:
            error_msg = f"ClientError {e.response['Error']['Code']}: {e.response['Error']['Message']}"
            logger.error(f"❌ Failed to fix ACL for {object_key}: {error_msg}")
            return {
                'success': False,
                'error': error_msg
            }
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"❌ Failed to fix ACL for {object_key}: {error_msg}")
            return {
                'success': False,
                'error': error_msg
            }

    def process_single_object(self, obj_info, dry_run=False):
        """
        Process a single object - check and fix ACL if needed
        
        Args:
            obj_info: Object info from list_objects_v2
            dry_run: If True, only check without fixing
            
        Returns:
            dict: Processing result
        """
        object_key = obj_info['Key']
        object_size = obj_info.get('Size', 0)
        
        # Skip if already processed (resume functionality)
        if object_key in self.processed_objects:
            logger.debug(f"⏭️ Skipping already processed: {object_key}")
            return {
                'object_key': object_key,
                'action': 'skipped',
                'already_processed': True,
                'size': object_size
            }
        
        # Check current ACL
        acl_check = self.check_object_acl(object_key)
        
        if acl_check['error']:
            logger.warning(f"⚠️ Could not check ACL for {object_key}: {acl_check['error']}")
            return {
                'object_key': object_key,
                'action': 'error_checking',
                'error': acl_check['error'],
                'size': object_size
            }
        
        # Update stats
        self.stats['checked_objects'] += 1
        
        if acl_check['is_public']:
            # Already public
            self.stats['public_objects'] += 1
            self.processed_objects.add(object_key)
            
            logger.debug(f"✅ Already public: {object_key}")
            return {
                'object_key': object_key,
                'action': 'already_public',
                'size': object_size
            }
        else:
            # Needs fixing
            self.stats['private_objects'] += 1
            
            if dry_run:
                logger.info(f"🔍 [DRY RUN] Would fix ACL for: {object_key}")
                return {
                    'object_key': object_key,
                    'action': 'would_fix',
                    'current_grantees': acl_check['grantees'],
                    'size': object_size
                }
            else:
                # Actually fix the ACL
                fix_result = self.set_object_public_acl(object_key)
                
                if fix_result['success']:
                    self.stats['fixed_objects'] += 1
                    self.stats['fixed_size_bytes'] += object_size
                    self.processed_objects.add(object_key)
                    
                    logger.info(f"🔧 Fixed ACL for: {object_key}")
                    return {
                        'object_key': object_key,
                        'action': 'fixed',
                        'size': object_size
                    }
                else:
                    self.stats['failed_objects'] += 1
                    
                    logger.error(f"❌ Failed to fix ACL for {object_key}: {fix_result['error']}")
                    return {
                        'object_key': object_key,
                        'action': 'failed_fix',
                        'error': fix_result['error'],
                        'size': object_size
                    }

    def list_all_objects(self, prefix='', max_keys=None):
        """
        List all objects in bucket with optional prefix filter
        
        Args:
            prefix: Object key prefix to filter by
            max_keys: Maximum number of objects to return (None for all)
            
        Returns:
            list: List of object info dictionaries
        """
        logger.info(f"🔍 Listing objects with prefix: '{prefix}'")
        
        all_objects = []
        continuation_token = None
        page_count = 0
        
        try:
            while True:
                page_count += 1
                
                # Prepare list_objects_v2 parameters
                list_params = {
                    'Bucket': self.bucket_name,
                    'Prefix': prefix,
                    'MaxKeys': min(1000, max_keys - len(all_objects)) if max_keys else 1000
                }
                
                if continuation_token:
                    list_params['ContinuationToken'] = continuation_token
                
                # Rate limiting
                self.rate_limit_check()
                
                # List objects
                response = self.s3_client.list_objects_v2(**list_params)
                
                # Add objects from this page
                page_objects = response.get('Contents', [])
                all_objects.extend(page_objects)
                
                # Update total size
                page_size = sum(obj.get('Size', 0) for obj in page_objects)
                self.stats['total_size_bytes'] += page_size
                
                logger.debug(f"📄 Page {page_count}: {len(page_objects)} objects ({page_size/1024/1024:.1f} MB)")
                
                # Check if we should continue
                if not response.get('IsTruncated', False):
                    break
                    
                if max_keys and len(all_objects) >= max_keys:
                    all_objects = all_objects[:max_keys]
                    break
                
                continuation_token = response.get('NextContinuationToken')
                
                # Safety break for very large buckets
                if page_count > 10000:  # More than 10M objects
                    logger.warning("⚠️ Reached safety limit of 10M objects. Consider using prefix filtering.")
                    break
        
        except Exception as e:
            logger.error(f"❌ Error listing objects: {e}")
            raise
        
        self.stats['total_objects'] = len(all_objects)
        logger.info(f"📊 Found {len(all_objects):,} objects ({self.stats['total_size_bytes']/1024/1024:.1f} MB total)")
        
        return all_objects

    def fix_acl_batch(self, objects, max_workers=5, dry_run=False):
        """
        Fix ACL for a batch of objects using parallel processing
        
        Args:
            objects: List of object info dictionaries
            max_workers: Number of parallel workers
            dry_run: If True, only check without fixing
        """
        if not objects:
            logger.warning("⚠️ No objects to process")
            return
        
        mode_text = "DRY RUN" if dry_run else "FIXING"
        logger.info(f"🚀 Starting {mode_text} for {len(objects):,} objects with {max_workers} workers")
        
        # Process objects in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with tqdm(total=len(objects), desc=f"{mode_text} ACLs", unit="object") as pbar:
                
                # Submit all tasks
                future_to_object = {
                    executor.submit(self.process_single_object, obj, dry_run): obj 
                    for obj in objects
                }
                
                # Process completed tasks
                for future in as_completed(future_to_object):
                    obj_info = future_to_object[future]
                    
                    try:
                        result = future.result()
                        
                        # Update progress bar with meaningful status
                        action = result.get('action', 'unknown')
                        object_key = result.get('object_key', 'unknown')
                        
                        if action == 'already_public':
                            pbar.set_postfix(status=f"✅ {os.path.basename(object_key)}")
                        elif action == 'fixed':
                            pbar.set_postfix(status=f"🔧 {os.path.basename(object_key)}")
                        elif action == 'would_fix':
                            pbar.set_postfix(status=f"🔍 {os.path.basename(object_key)}")
                        elif action == 'failed_fix':
                            pbar.set_postfix(status=f"❌ {os.path.basename(object_key)}")
                        elif action == 'skipped':
                            pbar.set_postfix(status=f"⏭️ {os.path.basename(object_key)}")
                        
                        pbar.update(1)
                        
                        # Save resume state periodically
                        if len(self.processed_objects) % 100 == 0:
                            self.save_resume_state()
                            
                    except Exception as e:
                        logger.error(f"❌ Task error for {obj_info.get('Key', 'unknown')}: {e}")
                        pbar.update(1)
        
        # Final save of resume state
        self.save_resume_state()

    def fix_bucket_acl(self, prefix='', max_workers=5, dry_run=False, max_objects=None):
        """
        Main method to fix ACL for entire bucket or filtered objects
        
        Args:
            prefix: Object key prefix to filter by
            max_workers: Number of parallel workers
            dry_run: If True, only check without fixing
            max_objects: Maximum number of objects to process
        """
        logger.info(f"🚀 Starting ACL fix process")
        logger.info(f"🪣 Bucket: {self.bucket_name}")
        logger.info(f"📂 Prefix: '{prefix}' (empty = all objects)")
        logger.info(f"👥 Workers: {max_workers}")
        logger.info(f"🧪 Dry run: {dry_run}")
        logger.info(f"📊 Max objects: {max_objects if max_objects else 'All'}")
        
        self.stats['start_time'] = time.time()
        
        # List all objects
        print("\n🔍 LISTING OBJECTS...")
        objects = self.list_all_objects(prefix, max_objects)
        
        if not objects:
            logger.warning("⚠️ No objects found to process")
            return
        
        # Show preview
        self.show_object_preview(objects, prefix)
        
        # Process objects
        print(f"\n🔧 PROCESSING OBJECTS...")
        self.fix_acl_batch(objects, max_workers, dry_run)
        
        # Generate report
        self.stats['end_time'] = time.time()
        self.generate_acl_fix_report(dry_run)

    def show_object_preview(self, objects, prefix):
        """Show preview of objects to be processed"""
        print(f"\n📋 OBJECT PREVIEW:")
        print("=" * 40)
        
        # Show first few objects
        for i, obj in enumerate(objects[:10]):
            key = obj['Key']
            size_mb = obj.get('Size', 0) / 1024 / 1024
            modified = obj.get('LastModified', 'Unknown')
            
            # Try to parse structure
            key_parts = key.replace(prefix, '').strip('/').split('/')
            if len(key_parts) >= 3:
                city = key_parts[0] if key_parts[0] else 'unknown'
                map_type = key_parts[1] if len(key_parts) > 1 else 'unknown'
                print(f"  📄 {os.path.basename(key)} ({city}/{map_type}) - {size_mb:.2f} MB")
            else:
                print(f"  📄 {key} - {size_mb:.2f} MB")
        
        if len(objects) > 10:
            print(f"  ... and {len(objects) - 10:,} more objects")
        
        total_size_mb = sum(obj.get('Size', 0) for obj in objects) / 1024 / 1024
        print(f"\n📊 TOTAL: {len(objects):,} objects ({total_size_mb:.1f} MB)")

    def generate_acl_fix_report(self, dry_run=False):
        """Generate comprehensive report of ACL fix process"""
        elapsed_time = self.stats['end_time'] - self.stats['start_time']
        
        # Calculate rates
        objects_per_second = self.stats['checked_objects'] / elapsed_time if elapsed_time > 0 else 0
        
        # Create report
        report = {
            'session_info': {
                'timestamp': datetime.now().isoformat(),
                'bucket': self.bucket_name,
                'region': self.region,
                'dry_run': dry_run,
                'duration_seconds': elapsed_time,
                'duration_minutes': elapsed_time / 60
            },
            'summary': {
                'total_objects': self.stats['total_objects'],
                'checked_objects': self.stats['checked_objects'],
                'public_objects': self.stats['public_objects'],
                'private_objects': self.stats['private_objects'],
                'fixed_objects': self.stats['fixed_objects'],
                'failed_objects': self.stats['failed_objects'],
                'public_percentage': (self.stats['public_objects'] / self.stats['checked_objects'] * 100) if self.stats['checked_objects'] > 0 else 0,
                'fix_success_rate': (self.stats['fixed_objects'] / self.stats['private_objects'] * 100) if self.stats['private_objects'] > 0 else 0
            },
            'data_info': {
                'total_size_bytes': self.stats['total_size_bytes'],
                'total_size_mb': self.stats['total_size_bytes'] / 1024 / 1024,
                'fixed_size_bytes': self.stats['fixed_size_bytes'],
                'fixed_size_mb': self.stats['fixed_size_bytes'] / 1024 / 1024
            },
            'performance': {
                'objects_per_second': objects_per_second,
                'start_time': datetime.fromtimestamp(self.stats['start_time']).isoformat(),
                'end_time': datetime.fromtimestamp(self.stats['end_time']).isoformat(),
                'api_calls_estimated': self.api_call_count
            }
        }
        
        # Save report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_type = 'dry_run' if dry_run else 'fix'
        report_file = f"spaces_acl_{report_type}_report_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Print summary
        self.print_acl_fix_summary(report, dry_run)
        
        logger.info(f"📋 Report saved: {report_file}")

    def print_acl_fix_summary(self, report, dry_run):
        """Print concise summary to console"""
        mode = "DRY RUN COMPLETE" if dry_run else "ACL FIX COMPLETE"
        
        print(f"\n🎉 {mode}!")
        print("=" * 50)
        print(f"⏱️ Duration: {report['session_info']['duration_minutes']:.1f} minutes")
        print(f"📁 Total objects: {report['summary']['total_objects']:,}")
        print(f"🔍 Checked: {report['summary']['checked_objects']:,}")
        print(f"✅ Already public: {report['summary']['public_objects']:,} ({report['summary']['public_percentage']:.1f}%)")
        print(f"🔒 Private found: {report['summary']['private_objects']:,}")
        
        if dry_run:
            print(f"🔍 Would fix: {report['summary']['private_objects']:,} objects")
            print(f"💾 Would affect: {report['data_info']['total_size_mb']:.1f} MB")
        else:
            print(f"🔧 Fixed: {report['summary']['fixed_objects']:,}")
            print(f"❌ Failed: {report['summary']['failed_objects']:,}")
            print(f"📊 Fix success: {report['summary']['fix_success_rate']:.1f}%")
            print(f"💾 Fixed size: {report['data_info']['fixed_size_mb']:.1f} MB")
        
        print(f"📈 Processing rate: {report['performance']['objects_per_second']:.1f} objects/sec")
        
        if report['summary']['failed_objects'] > 0:
            print(f"\n⚠️ {report['summary']['failed_objects']} objects failed to fix")
            print("Check the log file for details: spaces_acl_fix.log")

    def cleanup_resume_state(self):
        """Clean up resume state file after successful completion"""
        try:
            if os.path.exists(self.resume_file):
                os.remove(self.resume_file)
                logger.info("🧹 Cleaned up resume state file")
        except Exception as e:
            logger.warning(f"⚠️ Could not clean up resume state: {e}")

    def analyze_acl_issues(self, prefix='', sample_size=100):
        """
        Analyze ACL issues in the bucket by sampling objects
        
        Args:
            prefix: Object key prefix to filter by
            sample_size: Number of objects to sample for analysis
        """
        logger.info(f"🔍 Analyzing ACL issues with sample size: {sample_size}")
        
        # List objects (limited sample)
        objects = self.list_all_objects(prefix, sample_size)
        
        if not objects:
            logger.warning("⚠️ No objects found for analysis")
            return
        
        print(f"\n📊 ACL ANALYSIS REPORT:")
        print("=" * 40)
        print(f"📁 Sample size: {len(objects):,} objects")
        print(f"📂 Prefix: '{prefix}' (empty = all objects)")
        
        # Analyze sample
        public_count = 0
        private_count = 0
        error_count = 0
        grantee_patterns = {}
        
        print(f"\n🔍 Analyzing sample objects...")
        
        with tqdm(total=len(objects), desc="Analyzing ACLs", unit="object") as pbar:
            for obj in objects:
                acl_check = self.check_object_acl(obj['Key'])
                
                if acl_check['error']:
                    error_count += 1
                elif acl_check['is_public']:
                    public_count += 1
                else:
                    private_count += 1
                    
                    # Analyze grantee patterns
                    for grantee in acl_check['grantees']:
                        grantee_key = f"{grantee.get('type', 'unknown')}:{grantee.get('permission', 'unknown')}"
                        grantee_patterns[grantee_key] = grantee_patterns.get(grantee_key, 0) + 1
                
                pbar.update(1)
        
        # Print analysis results
        print(f"\n📈 ANALYSIS RESULTS:")
        print(f"✅ Public objects: {public_count:,} ({public_count/len(objects)*100:.1f}%)")
        print(f"🔒 Private objects: {private_count:,} ({private_count/len(objects)*100:.1f}%)")
        print(f"❌ Error checking: {error_count:,} ({error_count/len(objects)*100:.1f}%)")
        
        if private_count > 0:
            print(f"\n🔍 PRIVATE OBJECT GRANTEE PATTERNS:")
            for pattern, count in sorted(grantee_patterns.items(), key=lambda x: x[1], reverse=True):
                print(f"  • {pattern}: {count} objects")
            
            print(f"\n💡 RECOMMENDATION:")
            print(f"Run ACL fix to make {private_count:,} objects public")
            print(f"Estimated time: {private_count / 50:.1f} minutes (at 50 objects/sec)")
        else:
            print(f"\n✅ ALL SAMPLED OBJECTS ARE PUBLIC!")
            print(f"No ACL fixes needed for this prefix")


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

def parse_command_line_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Digital Ocean Spaces ACL Fixer - Fix missing public ACL permissions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --analyze                         # Analyze current ACL status
  %(prog)s --dry-run                         # Preview what would be fixed
  %(prog)s --prefix guland-tiles/hanoi       # Fix ACL for specific prefix
  %(prog)s --max-objects 1000               # Process only first 1000 objects
  %(prog)s --workers 10                     # Use 10 parallel workers
        """
    )
    
    parser.add_argument('--prefix',
                       default='',
                       help='Object key prefix to filter (e.g., guland-tiles/hanoi)')
    
    parser.add_argument('--max-objects',
                       type=int,
                       help='Maximum number of objects to process')
    
    parser.add_argument('--workers',
                       type=int, default=5,
                       help='Number of parallel workers (default: 5)')
    
    parser.add_argument('--dry-run',
                       action='store_true',
                       help='Preview what would be fixed without actually fixing')
    
    parser.add_argument('--analyze',
                       action='store_true',
                       help='Analyze current ACL status with sampling')
    
    parser.add_argument('--sample-size',
                       type=int, default=100,
                       help='Sample size for analysis (default: 100)')
    
    return parser.parse_args()

def interactive_mode():
    """Interactive mode for user-friendly operation"""
    print("🔧 DIGITAL OCEAN SPACES ACL FIXER")
    print("Fix missing public ACL permissions for uploaded objects")
    print("=" * 60)
    
    # Load configuration
    config = load_config()
    
    # Check if we have required config
    required_keys = ['access_key', 'secret_key', 'endpoint_url', 'bucket_name']
    missing_keys = [key for key in required_keys if not config.get(key)]
    
    if missing_keys:
        print(f"❌ Missing configuration: {', '.join(missing_keys)}")
        print("\nPlease ensure you have:")
        print("1. 📝 spaces_config.json file with credentials")
        print("2. 🔧 Or environment variables set")
        return
    
    print(f"✅ Configuration loaded:")
    print(f"  🪣 Bucket: {config['bucket_name']}")
    print(f"  🌐 Endpoint: {config['endpoint_url']}")
    print(f"  🗺️ Region: {config['region']}")
    
    # Choose operation mode
    print(f"\n🔧 OPERATION MODE:")
    print("=" * 20)
    print("1. 🔍 Analyze ACL status (recommended first)")
    print("2. 🧪 Dry run (preview fixes)")
    print("3. 🔧 Fix ACL (actual repair)")
    print("4. 🎯 Fix specific prefix only")
    
    while True:
        mode_choice = input("\nChoose operation (1/2/3/4, default: 1): ").strip()
        
        if mode_choice == '' or mode_choice == '1':
            operation_mode = 'analyze'
            break
        elif mode_choice == '2':
            operation_mode = 'dry_run'
            break
        elif mode_choice == '3':
            operation_mode = 'fix'
            break
        elif mode_choice == '4':
            operation_mode = 'prefix_fix'
            break
        else:
            print("❌ Invalid choice")
            continue
    
    # Get prefix if needed
    prefix = ''
    if operation_mode in ['prefix_fix']:
        print(f"\n📂 PREFIX CONFIGURATION:")
        print("Examples:")
        print("  • guland-tiles/hanoi (all maps for Hanoi)")
        print("  • guland-tiles/hanoi/qh-2030 (only QH 2030 for Hanoi)")
        print("  • guland-tiles (all uploaded tiles)")
        print("  • (empty for entire bucket)")
        
        prefix = input("\nEnter prefix (or leave empty for all): ").strip()
        
        if prefix:
            print(f"🎯 Will process objects with prefix: '{prefix}'")
        else:
            print("🎯 Will process ALL objects in bucket")
    
    # Get additional options for non-analyze modes
    max_objects = None
    max_workers = 5
    
    if operation_mode != 'analyze':
        # Max objects limit
        max_objects_input = input("\nMax objects to process (default: all): ").strip()
        if max_objects_input:
            try:
                max_objects = int(max_objects_input)
                print(f"📊 Will process maximum {max_objects:,} objects")
            except ValueError:
                print("⚠️ Invalid number, will process all objects")
        
        # Worker count
        workers_input = input("Parallel workers (1-20, default: 5): ").strip()
        try:
            max_workers = int(workers_input) if workers_input else 5
            max_workers = max(1, min(20, max_workers))
        except ValueError:
            max_workers = 5
    
    # Show final configuration
    print(f"\n📋 FINAL CONFIGURATION:")
    print("=" * 30)
    print(f"  🔧 Operation: {operation_mode.upper()}")
    print(f"  🪣 Bucket: {config['bucket_name']}")
    if prefix:
        print(f"  📂 Prefix: '{prefix}'")
    else:
        print(f"  📂 Prefix: (all objects)")
    
    if operation_mode != 'analyze':
        print(f"  📊 Max objects: {max_objects if max_objects else 'All'}")
        print(f"  👥 Workers: {max_workers}")
    
    # Confirm operation
    if operation_mode == 'analyze':
        confirm_msg = "Proceed with ACL analysis?"
    elif operation_mode == 'dry_run':
        confirm_msg = "Proceed with dry run?"
    elif operation_mode == 'fix':
        confirm_msg = "⚠️ Proceed with ACTUAL ACL fixing? This will modify your bucket!"
    else:
        confirm_msg = "⚠️ Proceed with ACTUAL ACL fixing for this prefix?"
    
    confirm = input(f"\n✅ {confirm_msg} (y/n): ").lower()
    if confirm != 'y':
        print("❌ Operation cancelled")
        return
    
    try:
        # Initialize ACL fixer
        print("\n🔧 Initializing ACL fixer...")
        fixer = DigitalOceanSpacesACLFixer(
            access_key=config['access_key'],
            secret_key=config['secret_key'],
            endpoint_url=config['endpoint_url'],
            bucket_name=config['bucket_name'],
            region=config['region']
        )
        
        # Execute operation
        if operation_mode == 'analyze':
            print("🔍 Starting ACL analysis...")
            fixer.analyze_acl_issues(prefix, sample_size=100)
        else:
            dry_run = operation_mode in ['dry_run']
            print(f"🚀 Starting ACL {'preview' if dry_run else 'fix'}...")
            
            fixer.fix_bucket_acl(
                prefix=prefix,
                max_workers=max_workers,
                dry_run=dry_run,
                max_objects=max_objects
            )
            
            # Cleanup option for successful fixes
            if not dry_run and fixer.stats.get('failed_objects', 0) == 0:
                cleanup = input("\n🧹 Clean up resume state file? (y/n): ").lower()
                if cleanup == 'y':
                    fixer.cleanup_resume_state()
        
    except Exception as e:
        logger.error(f"❌ Operation failed: {e}")
        print(f"❌ Operation failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main entry point"""
    args = parse_command_line_args()
    
    # Check if running in CLI mode
    cli_mode = any([
        args.prefix, args.max_objects, args.dry_run, 
        args.analyze, args.workers != 5
    ])
    
    if cli_mode:
        # CLI mode
        print("🔧 Digital Ocean Spaces ACL Fixer (CLI Mode)")
        print("=" * 50)
        
        config = load_config()
        required_keys = ['access_key', 'secret_key', 'endpoint_url', 'bucket_name']
        missing_keys = [key for key in required_keys if not config.get(key)]
        
        if missing_keys:
            print(f"❌ Missing configuration: {', '.join(missing_keys)}")
            return
        
        try:
            # Initialize fixer
            fixer = DigitalOceanSpacesACLFixer(
                access_key=config['access_key'],
                secret_key=config['secret_key'],
                endpoint_url=config['endpoint_url'],
                bucket_name=config['bucket_name'],
                region=config['region']
            )
            
            # Execute based on arguments
            if args.analyze:
                fixer.analyze_acl_issues(args.prefix, args.sample_size)
            else:
                fixer.fix_bucket_acl(
                    prefix=args.prefix,
                    max_workers=args.workers,
                    dry_run=args.dry_run,
                    max_objects=args.max_objects
                )
            
        except Exception as e:
            logger.error(f"❌ Operation failed: {e}")
            print(f"❌ Operation failed: {e}")
            return
    else:
        # Interactive mode
        interactive_mode()

if __name__ == "__main__":
    main()