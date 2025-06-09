#!/usr/bin/env python3
"""
Run Best Solution for Guland Crawling
Automatically tests and selects the best working approach

Usage: python run_best_solution.py [--test-only] [--force-browser]
"""

import argparse
import sys
import os
import time
import subprocess
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GulandSolutionRunner:
    def __init__(self):
        self.available_solutions = [
            {
                'name': 'Simple Request',
                'script': 'simple_test.py',
                'description': 'Direct API calls (fastest)',
                'test_function': self.test_simple_approach
            },
            {
                'name': 'Fixed Session Management',
                'script': 'fixed_geocoding_crawler.py',
                'description': 'Session + CSRF tokens (medium)',
                'test_function': self.test_session_approach
            },
            {
                'name': 'Browser Automation',
                'script': 'browser_crawler.py',
                'description': 'Real browser (slowest but most reliable)',
                'test_function': self.test_browser_approach
            }
        ]
        
        self.working_solution = None
    
    def test_simple_approach(self):
        """Test if simple requests work"""
        logger.info("🧪 Testing simple request approach...")
        
        try:
            import requests
            
            response = requests.post(
                'https://guland.vn/map/geocoding',
                data={
                    'lat': '16.0544563',
                    'lng': '108.0717219',
                    'path': 'soi-quy-hoach/da-nang'
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 1:
                    logger.info("✅ Simple approach works!")
                    return True
            
            logger.info(f"❌ Simple approach failed: HTTP {response.status_code}")
            return False
            
        except Exception as e:
            logger.info(f"❌ Simple approach error: {e}")
            return False
    
    def test_session_approach(self):
        """Test if session management works"""
        logger.info("🧪 Testing session management approach...")
        
        try:
            import requests
            from bs4 import BeautifulSoup
            
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            # Visit homepage
            homepage = session.get('https://guland.vn/', timeout=10)
            if homepage.status_code != 200:
                return False
            
            # Get CSRF token
            soup = BeautifulSoup(homepage.text, 'html.parser')
            csrf_token = None
            csrf_meta = soup.find('meta', {'name': 'csrf-token'})
            if csrf_meta:
                csrf_token = csrf_meta.get('content')
            
            # Visit planning page
            planning = session.get('https://guland.vn/soi-quy-hoach/da-nang', timeout=10)
            if planning.status_code != 200:
                return False
            
            # Update headers
            session.headers.update({
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': 'https://guland.vn/soi-quy-hoach/da-nang'
            })
            
            if csrf_token:
                session.headers.update({'X-CSRF-TOKEN': csrf_token})
            
            # Test API call
            form_data = {
                'lat': '16.0544563',
                'lng': '108.0717219',
                'path': 'soi-quy-hoach/da-nang'
            }
            
            if csrf_token:
                form_data['_token'] = csrf_token
            
            time.sleep(2)
            
            response = session.post(
                'https://guland.vn/map/geocoding',
                data=form_data,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 1:
                    logger.info("✅ Session management works!")
                    return True
            
            logger.info(f"❌ Session approach failed: HTTP {response.status_code}")
            return False
            
        except Exception as e:
            logger.info(f"❌ Session approach error: {e}")
            return False
    
    def test_browser_approach(self):
        """Test if browser automation is available"""
        logger.info("🧪 Testing browser automation availability...")
        
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.get("https://google.com")
            driver.quit()
            
            logger.info("✅ Browser automation available!")
            return True
            
        except Exception as e:
            logger.info(f"❌ Browser automation not available: {e}")
            return False
    
    def find_working_solution(self):
        """Test all solutions and find the best working one"""
        logger.info("🔍 FINDING BEST WORKING SOLUTION")
        logger.info("=" * 50)
        
        for solution in self.available_solutions:
            logger.info(f"\n📋 Testing: {solution['name']}")
            logger.info(f"   Description: {solution['description']}")
            
            try:
                if solution['test_function']():
                    logger.info(f"✅ {solution['name']} works!")
                    self.working_solution = solution
                    return solution
                else:
                    logger.info(f"❌ {solution['name']} failed")
            except Exception as e:
                logger.info(f"❌ {solution['name']} error: {e}")
        
        logger.error("❌ No working solution found!")
        return None
    
    def run_solution(self, solution, test_only=False):
        """Run the selected solution"""
        if test_only:
            logger.info(f"✅ Best solution: {solution['name']}")
            logger.info(f"📄 Would run: {solution['script']}")
            return True
        
        logger.info(f"🚀 RUNNING: {solution['name']}")
        logger.info("=" * 50)
        
        script_name = solution['script']
        
        if not os.path.exists(script_name):
            logger.error(f"❌ Script {script_name} not found!")
            return False
        
        try:
            # Run the script
            logger.info(f"▶️ Executing: python {script_name}")
            result = subprocess.run([sys.executable, script_name], 
                                  capture_output=False, text=True)
            
            if result.returncode == 0:
                logger.info(f"✅ {solution['name']} completed successfully!")
                return True
            else:
                logger.error(f"❌ {solution['name']} failed with exit code {result.returncode}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error running {solution['name']}: {e}")
            return False
    
    def run_best_solution(self, test_only=False, force_browser=False):
        """Main runner function"""
        print("🎯 GULAND BEST SOLUTION RUNNER")
        print("Automatically finds and runs the best working approach")
        print("=" * 70)
        
        if force_browser:
            # Force browser solution
            browser_solution = next(s for s in self.available_solutions if 'browser' in s['script'])
            logger.info("🤖 Forcing browser automation approach...")
            
            if self.test_browser_approach():
                self.working_solution = browser_solution
            else:
                logger.error("❌ Browser automation not available!")
                return False
        else:
            # Find best working solution
            self.working_solution = self.find_working_solution()
        
        if not self.working_solution:
            print("\n❌ NO WORKING SOLUTION FOUND!")
            print("\n💡 TROUBLESHOOTING:")
            print("1. Check internet connection")
            print("2. Verify Guland.vn is accessible")
            print("3. Install missing dependencies:")
            print("   pip install requests beautifulsoup4 selenium")
            print("4. Try different network/VPN")
            print("5. Try force browser mode:")
            print("   python run_best_solution.py --force-browser")
            return False
        
        # Run the solution
        success = self.run_solution(self.working_solution, test_only)
        
        if success:
            print(f"\n🎉 SUCCESS!")
            print(f"✅ Used approach: {self.working_solution['name']}")
            print(f"📄 Script: {self.working_solution['script']}")
            
            if not test_only:
                print(f"\n📁 Check output directories for results:")
                print("   • output_fixed_geocoding/ (if session approach)")
                print("   • output_browser_crawl/ (if browser approach)")
                print("   • Check logs for detailed information")
        else:
            print(f"\n❌ FAILED!")
            print(f"Could not run {self.working_solution['name']}")
        
        return success

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Run best Guland crawling solution')
    parser.add_argument('--test-only', action='store_true', 
                       help='Only test approaches, don\'t run full crawl')
    parser.add_argument('--force-browser', action='store_true',
                       help='Force browser automation approach')
    
    args = parser.parse_args()
    
    runner = GulandSolutionRunner()
    
    try:
        success = runner.run_best_solution(
            test_only=args.test_only,
            force_browser=args.force_browser
        )
        
        if success:
            if args.test_only:
                print("\n🎯 TO RUN FULL CRAWL:")
                print(f"python {runner.working_solution['script']}")
        else:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()