#!/usr/bin/env python3
"""
Map Interaction Module for Guland Crawler
Handles all map-related interactions and manipulations

Author: AI Assistant
Version: 1.0
"""
import time
import random
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

logger = logging.getLogger(__name__)

class MapInteractionHandler:
    def __init__(self, driver):
        self.driver = driver
    
    def set_map_zoom(self, target_zoom):
        """Set map to specific zoom level via JavaScript"""
        logger.info(f"🎯 Setting map zoom to {target_zoom}")
        
        try:
            js_script = f"""
            function setMapZoom() {{
                var mapInstances = [
                    window.map,
                    window.mapInstance, 
                    window.leafletMap,
                    document.querySelector('.leaflet-container')?._leaflet_map
                ];
                
                for (var i = 0; i < mapInstances.length; i++) {{
                    var mapInstance = mapInstances[i];
                    if (mapInstance && mapInstance.setZoom) {{
                        console.log('Setting zoom to {target_zoom} on instance', i);
                        mapInstance.setZoom({target_zoom});
                        return true;
                    }}
                }}
                
                return false;
            }}
            
            return setMapZoom();
            """
            
            result = self.driver.execute_script(js_script)
            if result:
                logger.info(f"✅ Successfully set zoom to {target_zoom}")
                return True
            else:
                logger.warning(f"⚠️ Could not set zoom via JavaScript")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error setting zoom: {e}")
            return False

    def simulate_zoom_interaction(self, target_zoom):
        """Simulate user zoom interaction to reach target zoom"""
        logger.info(f"🖱️ Simulating zoom interaction to level {target_zoom}")
        
        try:
            map_container = self.driver.find_element(By.CSS_SELECTOR, 
                '#map, .map-container, [class*="map"], canvas, .leaflet-container')
            
            actions = ActionChains(self.driver)
            actions.move_to_element(map_container).perform()
            
            # Start from a known zoom (usually around 13-15)
            current_estimated_zoom = 14
            
            if target_zoom > current_estimated_zoom:
                # Zoom in
                zoom_steps = target_zoom - current_estimated_zoom
                for _ in range(zoom_steps):
                    self.driver.execute_script("""
                        arguments[0].dispatchEvent(new WheelEvent('wheel', {
                            deltaY: -100,
                            bubbles: true,
                            cancelable: true
                        }));
                    """, map_container)
                    time.sleep(0.5)
            else:
                # Zoom out
                zoom_steps = current_estimated_zoom - target_zoom
                for _ in range(zoom_steps):
                    self.driver.execute_script("""
                        arguments[0].dispatchEvent(new WheelEvent('wheel', {
                            deltaY: 100,
                            bubbles: true,
                            cancelable: true
                        }));
                    """, map_container)
                    time.sleep(0.5)
            
            logger.info(f"✅ Zoom interaction completed for level {target_zoom}")
            
        except Exception as e:
            logger.error(f"❌ Error in zoom interaction: {e}")

    def comprehensive_map_coverage(self, zoom_level, duration_seconds):
        """Comprehensive map coverage using grid pattern"""
        logger.info(f"🗺️ Starting comprehensive coverage at zoom {zoom_level} for {duration_seconds}s")
        
        try:
            # Find map container with better error handling
            map_container = None
            selectors_to_try = [
                '#map', '.map-container', '[class*="map"]', 
                'canvas', '.leaflet-container', '.leaflet-map-pane'
            ]
            
            for selector in selectors_to_try:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements and elements[0].is_displayed():
                        map_container = elements[0]
                        logger.info(f"🎯 Found map container: {selector}")
                        break
                except:
                    continue
            
            if not map_container:
                logger.error("❌ No valid map container found")
                return []
            
            # Scroll map into view
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", map_container)
            time.sleep(1)
            
            map_size = map_container.size
            logger.info(f"📏 Map size: {map_size['width']}x{map_size['height']}")
            
            # Validate map size
            if map_size['width'] < 200 or map_size['height'] < 200:
                logger.warning(f"⚠️ Map too small for coverage: {map_size}")
                return []
            
            actions = ActionChains(self.driver)
            
            # FIXED: Simpler coverage pattern to avoid out of bounds
            coverage_actions = [
                'center_pan_up', 'center_pan_down', 'center_pan_left', 'center_pan_right',
                'zoom_in_center', 'zoom_out_center', 'small_drag_center'
            ]
            
            start_time = time.time()
            action_count = 0
            max_actions = 20  # FIXED: Limit total actions to prevent infinite loop
            
            while (time.time() - start_time < duration_seconds and 
                action_count < max_actions):
                
                action_count += 1
                action_type = coverage_actions[(action_count - 1) % len(coverage_actions)]
                
                logger.info(f"📍 Coverage action {action_count}/{max_actions}: {action_type}")
                
                try:
                    if action_type == 'center_pan_up':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.5)
                        actions.click_and_hold().move_by_offset(0, -50).release().perform()
                        
                    elif action_type == 'center_pan_down':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.5)
                        actions.click_and_hold().move_by_offset(0, 50).release().perform()
                        
                    elif action_type == 'center_pan_left':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.5)
                        actions.click_and_hold().move_by_offset(-50, 0).release().perform()
                        
                    elif action_type == 'center_pan_right':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.5)
                        actions.click_and_hold().move_by_offset(50, 0).release().perform()
                        
                    elif action_type == 'zoom_in_center':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.2)
                        self.driver.execute_script("""
                            arguments[0].dispatchEvent(new WheelEvent('wheel', {
                                deltaY: -100,
                                bubbles: true,
                                cancelable: true
                            }));
                        """, map_container)
                        
                    elif action_type == 'zoom_out_center':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.2)
                        self.driver.execute_script("""
                            arguments[0].dispatchEvent(new WheelEvent('wheel', {
                                deltaY: 100,
                                bubbles: true,
                                cancelable: true
                            }));
                        """, map_container)
                        
                    elif action_type == 'small_drag_center':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.5)
                        # Small random drag from center
                        offset_x = random.randint(-30, 30)
                        offset_y = random.randint(-30, 30)
                        actions.click_and_hold().move_by_offset(offset_x, offset_y).release().perform()
                    
                    # Wait between actions
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as action_error:
                    logger.warning(f"⚠️ Action {action_type} failed: {action_error}")
                    # Continue to next action
                    continue
            
            logger.info(f"✅ Completed {action_count} coverage actions")
            return action_count
            
        except Exception as e:
            logger.error(f"❌ Error in comprehensive coverage: {e}")
            return 0

    def detect_city_boundaries(self, location_name):
        """Detect city boundaries for complete coverage"""
        logger.info(f"🌍 Detecting boundaries for {location_name}")
        
        try:
            # Get current map bounds via JavaScript
            js_script = """
            function getCityBounds() {
                var mapInstances = [
                    window.map,
                    window.mapInstance, 
                    window.leafletMap,
                    document.querySelector('.leaflet-container')?._leaflet_map
                ];
                
                for (var i = 0; i < mapInstances.length; i++) {
                    var mapInstance = mapInstances[i];
                    if (mapInstance && mapInstance.getBounds) {
                        var bounds = mapInstance.getBounds();
                        return {
                            northeast: {
                                lat: bounds.getNorthEast().lat,
                                lng: bounds.getNorthEast().lng
                            },
                            southwest: {
                                lat: bounds.getSouthWest().lat,
                                lng: bounds.getSouthWest().lng
                            },
                            center: {
                                lat: mapInstance.getCenter().lat,
                                lng: mapInstance.getCenter().lng
                            },
                            zoom: mapInstance.getZoom()
                        };
                    }
                }
                return null;
            }
            
            return getCityBounds();
            """
            
            bounds = self.driver.execute_script(js_script)
            
            if bounds:
                logger.info(f"🌍 City bounds detected:")
                logger.info(f"  NE: {bounds['northeast']}")
                logger.info(f"  SW: {bounds['southwest']}")
                logger.info(f"  Center: {bounds['center']}")
                logger.info(f"  Current zoom: {bounds['zoom']}")
                return bounds
            else:
                logger.warning("⚠️ Could not detect city bounds")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error detecting boundaries: {e}")
            return None

    def calculate_tile_coverage_needed(self, bounds, zoom_level):
        """Calculate how many tiles needed for full coverage"""
        if not bounds:
            return None
        
        import math
        
        def deg2num(lat_deg, lon_deg, zoom):
            lat_rad = math.radians(lat_deg)
            n = 2.0 ** zoom
            xtile = int((lon_deg + 180.0) / 360.0 * n)
            ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
            return (xtile, ytile)
        
        # Calculate tile coordinates for corners
        ne_tile = deg2num(bounds['northeast']['lat'], bounds['northeast']['lng'], zoom_level)
        sw_tile = deg2num(bounds['southwest']['lat'], bounds['southwest']['lng'], zoom_level)
        
        # Calculate coverage area
        tile_count_x = abs(ne_tile[0] - sw_tile[0]) + 1
        tile_count_y = abs(ne_tile[1] - sw_tile[1]) + 1
        total_tiles = tile_count_x * tile_count_y
        
        logger.info(f"📊 Zoom {zoom_level} coverage calculation:")
        logger.info(f"  X tiles: {tile_count_x} (from {sw_tile[0]} to {ne_tile[0]})")
        logger.info(f"  Y tiles: {tile_count_y} (from {ne_tile[1]} to {sw_tile[1]})")
        logger.info(f"  Total tiles needed: {total_tiles}")
        
        return {
            'zoom': zoom_level,
            'x_range': (sw_tile[0], ne_tile[0]),
            'y_range': (ne_tile[1], sw_tile[1]),
            'x_count': tile_count_x,
            'y_count': tile_count_y,
            'total_tiles': total_tiles
        }

    def get_safe_coordinates(self, map_container):
        """Get safe coordinates within map bounds"""
        try:
            map_size = map_container.size
            map_rect = map_container.rect
            
            # Ensure map is visible and has reasonable size
            if map_size['width'] < 100 or map_size['height'] < 100:
                logger.warning(f"⚠️ Map too small: {map_size['width']}x{map_size['height']}")
                return None, None
            
            # Calculate safe area with margins
            margin = 80
            safe_x = margin + (map_size['width'] - 2 * margin) // 2
            safe_y = margin + (map_size['height'] - 2 * margin) // 2
            
            # Validate coordinates are within bounds
            if safe_x < margin or safe_x > map_size['width'] - margin:
                safe_x = map_size['width'] // 2
            if safe_y < margin or safe_y > map_size['height'] - margin:
                safe_y = map_size['height'] // 2
                
            logger.info(f"📍 Safe coordinates: ({safe_x}, {safe_y}) within {map_size['width']}x{map_size['height']}")
            return safe_x, safe_y
            
        except Exception as e:
            logger.error(f"❌ Error getting safe coordinates: {e}")
            return None, None

    def safe_move_to_element_with_offset(self, actions, element, x, y):
        """Safely move to element with offset validation"""
        try:
            element_size = element.size
            
            # Validate offsets are within element bounds
            if x < 0 or x > element_size['width'] or y < 0 or y > element_size['height']:
                logger.warning(f"⚠️ Invalid offset ({x}, {y}) for element size {element_size}")
                # Use center of element as fallback
                x = element_size['width'] // 2
                y = element_size['height'] // 2
            
            actions.move_to_element_with_offset(element, x, y).perform()
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Safe move failed: {e}")
            # Fallback to center of element
            try:
                actions.move_to_element(element).perform()
                return True
            except:
                return False

    def simulate_map_interaction(self, location_name, duration_seconds=45):
        """Simulate realistic map interaction to trigger tile loading"""
        logger.info(f"🗺️ Simulating map interaction for {location_name} ({duration_seconds}s)...")
        
        try:
            # Wait for map to load completely
            time.sleep(8)
            
            # Ensure proper window size first
            self.driver.maximize_window()
            time.sleep(1)
            
            # Get viewport dimensions
            viewport_width = self.driver.execute_script("return window.innerWidth")
            viewport_height = self.driver.execute_script("return window.innerHeight")
            logger.info(f"📐 Viewport size: {viewport_width}x{viewport_height}")
            
            # Find map container
            map_container = None
            possible_selectors = [
                '#map',
                '.map-container',
                '[class*="map"]',
                '[id*="map"]',
                'canvas',
                '.leaflet-container',
                '.leaflet-map-pane'
            ]
            
            for selector in possible_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        map_container = elements[0]
                        logger.info(f"🎯 Found map container: {selector}")
                        
                        # Scroll element into view
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", map_container)
                        time.sleep(1)
                        break
                except:
                    continue
            
            if not map_container:
                logger.warning("⚠️ Could not find map container, using body")
                map_container = self.driver.find_element(By.TAG_NAME, "body")
            
            actions = ActionChains(self.driver)
            
            # Get map container size and position
            map_rect = map_container.rect
            map_size = map_container.size
            logger.info(f"📏 Map container size: {map_size['width']}x{map_size['height']}")
            logger.info(f"📍 Map position: x={map_rect['x']}, y={map_rect['y']}")
            
            # Calculate safe interaction area with margins
            margin = 50
            safe_width = max(100, map_size['width'] - 2 * margin)
            safe_height = max(100, map_size['height'] - 2 * margin)
            
            # Start time
            start_time = time.time()
            interaction_count = 0
            
            while time.time() - start_time < duration_seconds:
                interaction_count += 1
                logger.info(f"🎮 Interaction #{interaction_count}")
                
                # Random interaction type with better distribution
                interaction_type = random.choice([
                    'scroll_zoom_in', 'scroll_zoom_out', 'double_click', 
                    'click_zoom', 'pan_drag', 'mouse_wheel', 'keyboard_zoom'
                ])
                
                try:
                    if interaction_type == 'scroll_zoom_in':
                        logger.info("🔍 Scroll zoom in")
                        # Move to center of map
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.2)
                        self.driver.execute_script("""
                            arguments[0].dispatchEvent(new WheelEvent('wheel', {
                                deltaY: -100,
                                bubbles: true,
                                cancelable: true
                            }));
                        """, map_container)
                        
                    elif interaction_type == 'scroll_zoom_out':
                        logger.info("🔍 Scroll zoom out")
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.2)
                        self.driver.execute_script("""
                            arguments[0].dispatchEvent(new WheelEvent('wheel', {
                                deltaY: 100,
                                bubbles: true,
                                cancelable: true
                            }));
                        """, map_container)
                        
                    elif interaction_type == 'pan_drag':
                        logger.info("↔️ Pan drag")
                        # Safe coordinates within map bounds
                        start_x = random.randint(margin, margin + safe_width // 2)
                        start_y = random.randint(margin, margin + safe_height // 2)
                        
                        # Limited offset to stay in bounds
                        max_offset = min(safe_width, safe_height) // 4
                        offset_x = random.randint(-max_offset, max_offset)
                        offset_y = random.randint(-max_offset, max_offset)
                        
                        logger.info(f"   Drag from offset ({start_x}, {start_y}) by ({offset_x}, {offset_y})")
                        
                        actions.move_to_element_with_offset(map_container, start_x, start_y)\
                            .click_and_hold()\
                            .move_by_offset(offset_x, offset_y)\
                            .release()\
                            .perform()
                        
                    elif interaction_type == 'double_click':
                        logger.info("👆 Double click zoom")
                        # Safe click position
                        click_x = random.randint(margin, margin + safe_width // 2)
                        click_y = random.randint(margin, margin + safe_height // 2)
                        
                        logger.info(f"   Double click at offset ({click_x}, {click_y})")
                        
                        actions.move_to_element_with_offset(map_container, click_x, click_y)\
                            .double_click()\
                            .perform()
                        
                    elif interaction_type == 'click_zoom':
                        logger.info("👆 Click and zoom")
                        actions.move_to_element(map_container).click().perform()
                        time.sleep(0.5)
                        
                        # Use JavaScript to simulate zoom
                        self.driver.execute_script("""
                            // Try common map zoom methods
                            if (window.map && window.map.zoomIn) {
                                window.map.zoomIn();
                            } else if (window.mapInstance && window.mapInstance.zoomIn) {
                                window.mapInstance.zoomIn();
                            }
                        """)
                        
                    elif interaction_type == 'keyboard_zoom':
                        logger.info("⌨️ Keyboard zoom")
                        actions.move_to_element(map_container).click().perform()
                        time.sleep(0.5)
                        
                        # Use arrow keys and other keys that actually exist
                        zoom_key = random.choice([
                            Keys.ARROW_UP, Keys.ARROW_DOWN, 
                            Keys.ARROW_LEFT, Keys.ARROW_RIGHT,
                            '+', '-', '='
                        ])
                        actions.send_keys(zoom_key).perform()
                        
                    elif interaction_type == 'mouse_wheel':
                        logger.info("🖱️ Mouse wheel")
                        # Safe wheel position
                        wheel_x = random.randint(margin, margin + safe_width // 2)
                        wheel_y = random.randint(margin, margin + safe_height // 2)
                        
                        logger.info(f"   Mouse wheel at offset ({wheel_x}, {wheel_y})")
                        
                        actions.move_to_element_with_offset(map_container, wheel_x, wheel_y).perform()
                        time.sleep(0.2)
                        
                        # Multiple wheel events
                        for _ in range(random.randint(1, 3)):
                            direction = random.choice([-1, 1])
                            self.driver.execute_script(f"""
                                arguments[0].dispatchEvent(new WheelEvent('wheel', {{
                                    deltaY: {direction * 120},
                                    bubbles: true,
                                    cancelable: true
                                }}));
                            """, map_container)
                            time.sleep(0.2)
                    
                    # Wait between interactions to let tiles load
                    wait_time = random.uniform(3, 7)
                    logger.info(f"⏳ Waiting {wait_time:.1f}s for tiles to load...")
                    time.sleep(wait_time)
                    
                except Exception as interaction_error:
                    logger.warning(f"⚠️ Interaction error: {interaction_error}")
                    # Continue with next interaction
                    continue
            
            logger.info(f"✅ Completed {interaction_count} map interactions")
            
            # Final wait for any pending tile requests
            logger.info("⏳ Final wait for tile loading...")
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"❌ Error during map interaction: {e}")
            
    def trigger_tile_loading(self):
        """Trigger tile loading using JavaScript"""
        logger.info("🚀 Triggering tile loading via JavaScript...")
        
        try:
            # Script to trigger map events that should load tiles
            js_script = """
            // Function to trigger map updates
            function triggerMapUpdates() {
                console.log('Triggering map tile loading...');
                
                // Method 1: Trigger resize events
                window.dispatchEvent(new Event('resize'));
                
                // Method 2: Try to access common map instances
                var mapInstances = [
                    window.map,
                    window.mapInstance, 
                    window.leafletMap,
                    window.L,
                    document.querySelector('.leaflet-container')?._leaflet_map
                ];
                
                mapInstances.forEach(function(mapInstance, index) {
                    if (mapInstance) {
                        console.log('Found map instance:', index);
                        
                        try {
                            // Try different zoom operations
                            if (mapInstance.setZoom) {
                                var currentZoom = mapInstance.getZoom ? mapInstance.getZoom() : 15;
                                mapInstance.setZoom(currentZoom + 1);
                                setTimeout(function() {
                                    mapInstance.setZoom(currentZoom);
                                }, 1000);
                            }
                            
                            // Try pan operations
                            if (mapInstance.panBy) {
                                mapInstance.panBy([50, 50]);
                                setTimeout(function() {
                                    mapInstance.panBy([-50, -50]);
                                }, 1000);
                            }
                            
                            // Try invalidate size
                            if (mapInstance.invalidateSize) {
                                mapInstance.invalidateSize();
                            }
                            
                        } catch (e) {
                            console.log('Error manipulating map instance:', e);
                        }
                    }
                });
                
                // Method 3: Force refresh of map layers
                var canvases = document.querySelectorAll('canvas');
                canvases.forEach(function(canvas) {
                    if (canvas.getContext) {
                        try {
                            var ctx = canvas.getContext('2d');
                            // Trigger redraw
                            canvas.style.transform = 'scale(1.001)';
                            setTimeout(function() {
                                canvas.style.transform = '';
                            }, 100);
                        } catch (e) {
                            console.log('Canvas manipulation error:', e);
                        }
                    }
                });
                
                return 'Tile loading triggered';
            }
            
            return triggerMapUpdates();
            """
            
            result = self.driver.execute_script(js_script)
            logger.info(f"🚀 JavaScript trigger result: {result}")
            
            # Wait for any triggered requests
            time.sleep(3)
            
        except Exception as e:
            logger.error(f"❌ Error triggering tile loading: {e}")