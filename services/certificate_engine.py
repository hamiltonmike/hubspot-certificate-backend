"""
Certificate Engine - Insurance Certificate Generation
Created: 2025-11-18
Migrated from standalone generator

Handles:
- Data fetching from HubSpot
- Validation (structural, data completeness, consistency, business rules)
- Device grouping and formatting
- Field assembly with transformations
- Certificate number generation

Configuration embedded from document_engine_config.json
"""

import re
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
import requests


class CertificateEngine:
    """Generates insurance certificates from HubSpot data"""
    
    def __init__(self, hubspot_token: str, portal_id: str, 
                 system_type_id: str, agreement_type_id: str):
        """Initialize with HubSpot credentials and type IDs"""
        self.HUBSPOT_TOKEN = hubspot_token
        self.PORTAL_ID = portal_id
        self.SYSTEM_TYPE_ID = system_type_id
        self.AGREEMENT_TYPE_ID = agreement_type_id
        self.DEVICE_TYPE = "2-34947969"
        
        # Load embedded configuration
        self._load_config()
    
    def _load_config(self):
        """Load field mappings and device grouping rules (embedded)"""
        # This will be filled in Part 2
        self.field_mappings = {}
        self.device_grouping = {}
        self.equipment_subtype_mappings = {}
    
    def get_headers(self):
        """Get HubSpot API headers"""
        return {
            'Authorization': f'Bearer {self.HUBSPOT_TOKEN}',
            'Content-Type': 'application/json'
        }
    
    def transform_value(self, value: Any, transform: str, context: Dict = None) -> Any:
        """Apply transformation to value"""
        if value is None:
            return ""
        
        if transform == "NONE":
            return str(value)
        
        elif transform == "CONCAT_SPACE":
            if isinstance(value, list):
                return " ".join(str(v) for v in value if v)
            return str(value)
        
        elif transform == "UPPER":
            return str(value).upper()
        
        elif transform == "DATETIME_FORMAT":
            if isinstance(value, str):
                try:
                    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    return dt.strftime("%B %d, %Y %I:%M %p").replace(" 0", " ")
                except:
                    return str(value)
            return str(value)
        
        elif transform == "TIMESTAMP_FORMAT":
            dt = datetime.now()
            day = dt.day
            if 10 <= day % 100 <= 20:
                suffix = 'th'
            else:
                suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
            month_name = dt.strftime("%B")
            year = dt.year
            time_str = dt.strftime("%I:%M%p").lstrip('0').lower()
            result = f"{month_name} {day}{suffix}, {year} at {time_str}"
            print(f"DEBUG TIMESTAMP_FORMAT generated: {result}", flush=True)
            return result
        
        elif transform == "DEVICE_COUNT":
            if not isinstance(value, list):
                return 0
            return len(value)
        
        elif transform == "EXTRACT_ITEM_NUMBER":
            match = re.search(r'\[(\d+)\]', str(value))
            return match.group(1) if match else ""
        
        elif transform == "EXTRACT_NAME":
            result = re.sub(r'\s*\[\d+\].*$', '', str(value))
            return result.strip()
        
        elif transform == "DEVICE_ARRAY":
            if not isinstance(value, list):
                return []
            return value
        
        elif transform == "COMM_PATH_LABEL":
            if not value:
                return ""
            value_str = str(value).strip().lower()
            code_mappings = {
                '01': 'BLINK mesh radio', '02': 'cellular via Alarmnet',
                '03': 'cellular via DSC Integration Bridge', '04': 'analog telephone line',
                '05': 'cellular connection', '06': 'cellular via alarm.com',
                '07': 'internet via Alarmnet', '08': 'internet via DSC',
                '09': 'BLINK mesh radio', '13': 'BLINK mesh radio'
            }
            if value_str in code_mappings:
                return code_mappings[value_str]
            if 'blink' in value_str:
                return 'BLINK mesh radio'
            elif 'gsm' in value_str or 'cellular' in value_str:
                return 'cellular connection'
            elif 'phone' in value_str or 'telephone' in value_str:
                return 'analog telephone line'
            return str(value)
        
        elif transform == "COMM_PATH_PHRASE":
            if not isinstance(value, list) or len(value) < 2:
                return ""
            path1 = str(value[0] or '').strip().lower()
            path2 = str(value[1] or '').strip().lower()
            if not path1 and not path2:
                return ""
            wireless = ['blink', 'gsm', 'cellular']
            path1_wireless = any(w in path1 for w in wireless)
            path2_wireless = any(w in path2 for w in wireless)
            if path1_wireless and path2_wireless:
                return "Redundant Wireless"
            if path1_wireless or path2_wireless:
                return "GSM Only"
            return ""
        
        return str(value)
    
    def generate_certificate_number(self, site_id: str, system_id: str) -> str:
        """
        Generate certificate number: SITE_ID-COUNTER
        Example: 11482697572-161
        Increments certificate_counter on system object
        """
        headers = self.get_headers()
        
        # Get current counter
        system_url = f"https://api.hubapi.com/crm/v3/objects/{self.SYSTEM_TYPE_ID}/{system_id}"
        response = requests.get(system_url, headers=headers, 
                               params={'properties': 'certificate_counter'})
        
        if response.status_code != 200:
            raise Exception(f"Failed to get system: {response.status_code}")
        
        system = response.json()
        current_counter = system.get('properties', {}).get('certificate_counter', '0')
        
        try:
            counter_int = int(float(current_counter))
        except (ValueError, TypeError):
            counter_int = 0
        
        new_counter = counter_int + 1
        cert_number = f"{site_id}-{new_counter:03d}"
        
        # Update counter
        update_url = f"https://api.hubapi.com/crm/v3/objects/{self.SYSTEM_TYPE_ID}/{system_id}"
        update_data = {'properties': {'certificate_counter': new_counter}}
        
        response = requests.patch(update_url, headers=headers, json=update_data)
        if response.status_code not in [200, 201]:
            raise Exception(f"Failed to update counter: {response.text}")
        
        print(f"✓ Certificate number: {cert_number}")
        return cert_number
    
    def fetch_hubspot_data(self, agreement_id: str, system_id: str, site_id: str) -> Dict[str, Any]:
        """Fetch all required data from HubSpot"""
        headers = self.get_headers()
        data = {}
        
        # Fetch Agreement
        agreement_url = f"https://api.hubapi.com/crm/v3/objects/{self.AGREEMENT_TYPE_ID}/{agreement_id}"
        agreement_props = [
            'hs_object_id', 'agreement_plan_name', 'agreement_plan_effectivedate',
            'agreement_plan_supervision', 'agreement_plan_communication_path_primary',
            'agreement_plan_communication_path_secondary', 'agreement_plan_communication_15746',
            'agreement_plan_communication_15747', 'agreement_plan_gateway_14380',
            'agreement_partitions_included', 'agreement_partitions_additional', 'agreement_partitions_total',
            'agreement_plan_intrusion_14385', 'agreement_plan_intrusion_14386', 'agreement_plan_intrusion_14387',
            'agreement_plan_intrusion_14388', 'agreement_plan_intrusion_14399', 'agreement_plan_intrusion_15082',
            'agreement_plan_fire_14389', 'agreement_plan_fire_14390', 'agreement_plan_fire_14391',
            'agreement_plan_fire_14401', 'agreement_plan_environmental_14393', 'agreement_plan_environmental_14394',
            'agreement_plan_environmental_14395', 'agreement_plan_environmental_14396', 'agreement_plan_environmental_14397',
            'agreement_plan_integratedsystems_14398', 'agreement_plan_integratedsystems_14726',
            'agreement_plan_integratedsystems_11571', 'agreement_plan_integratedsystems_15787',
            'agreement_plan_integratedsystems_15272', 'agreement_plan_integratedsystems_14920',
            'agreement_plan_integratedsystems_14631', 'agreement_plan_response_15252',
            'agreement_plan_verify_14372', 'agreement_plan_trespass_15756', 'agreement_plan_trespass_15753',
            'agreement_plan_trespass_15754', 'agreement_plan_trespass_15753_quantity',
            'agreement_plan_trespass_15754_quantity', 'agreement_plan_trespass_15756_quantity',
            'agreement_plan_response_14366', 'agreement_plan_response_14367',
            'agreement_response_guarantee', 'agreement_plan_response_guarantee'
        ]
        
        response = requests.get(agreement_url, headers=headers, 
                               params={'properties': ','.join(agreement_props)})
        data['AGREEMENT'] = response.json().get('properties', {}) if response.status_code == 200 else {}
        
        # Fetch System
        system_url = f"https://api.hubapi.com/crm/v3/objects/{self.SYSTEM_TYPE_ID}/{system_id}"
        system_props = ['hs_object_id', 'name', 'communication_path_1', 'communication_path_2', 'integrations_gate_fire']
        
        response = requests.get(system_url, headers=headers,
                               params={'properties': ','.join(system_props)})
        data['SYSTEM'] = response.json().get('properties', {}) if response.status_code == 200 else {}
        
        # Fetch Site
        site_url = f"https://api.hubapi.com/crm/v3/objects/companies/{site_id}"
        site_props = ['name', 'mas_site_name', 'address', 'address2', 'city', 'state', 'zip', 'site_type']
        
        response = requests.get(site_url, headers=headers,
                               params={'properties': ','.join(site_props)})
        data['SITE'] = response.json().get('properties', {}) if response.status_code == 200 else {}
        
        # Fetch Customer (contact associated with agreement)
        customer_assoc_url = f"https://api.hubapi.com/crm/v4/objects/{self.AGREEMENT_TYPE_ID}/{agreement_id}/associations/contacts"
        response = requests.get(customer_assoc_url, headers=headers)
        
        if response.status_code == 200:
            customer_data = response.json()
            if customer_data.get('results'):
                customer_id = customer_data['results'][0]['toObjectId']
                customer_url = f"https://api.hubapi.com/crm/v3/objects/contacts/{customer_id}"
                response = requests.get(customer_url, headers=headers,
                                      params={'properties': 'firstname,lastname,address,city,state,zip'})
                data['CUSTOMER'] = response.json().get('properties', {}) if response.status_code == 200 else {}
            else:
                data['CUSTOMER'] = {}
        else:
            data['CUSTOMER'] = {}
        
        # Fetch Devices
        device_props = [
            'alarm_com_description', 'zone__', 'alarm_com_equipment_type',
            'alarm_com_central_station_reporting_type', 'alarm_com_power_source',
            'adc_deviceid', 'alarm_com_partition', 'installation_date',
            'alarm_com_sensor_group', 'equipment_subtype'
        ]
        
        search_payload = {
            'filterGroups': [{
                'filters': [{
                    'propertyName': f'associations.{self.SYSTEM_TYPE_ID}',
                    'operator': 'EQ',
                    'value': system_id
                }]
            }],
            'properties': device_props,
            'limit': 100
        }
        
        search_url = f"https://api.hubapi.com/crm/v3/objects/{self.DEVICE_TYPE}/search"
        response = requests.post(search_url, headers=headers, json=search_payload)
        devices_response = response.json() if response.status_code == 200 else {}
        
        devices = devices_response.get('results', [])
        data['DEVICES'] = [d.get('properties', {}) for d in devices]
        
        print(f"✓ Fetched data: {len(data['DEVICES'])} devices")
        return data
    
    def validate_data(self, data: Dict) -> Tuple[bool, List[str]]:
        """Validate certificate data completeness"""
        errors = []
        
        # Check site address
        site = data.get('SITE', {})
        if not site.get('address'):
            errors.append("Site address is missing")
        if not site.get('city'):
            errors.append("Site city is missing")
        if not site.get('state'):
            errors.append("Site state is missing")
        if not site.get('zip'):
            errors.append("Site postal code is missing")
        
        # Check customer name
        customer = data.get('CUSTOMER', {})
        if not customer.get('firstname') and not customer.get('lastname'):
            errors.append("Customer name is missing")
        
        # Check devices
        if not data.get('DEVICES'):
            errors.append("No devices found on system")
        
        return (len(errors) == 0, errors)
    
    def group_devices(self, devices: List[Dict]) -> Dict[str, List[Dict]]:
        """Group devices by category based on equipment_type and subtype"""
        
        # Initialize categories
        categories = [
            "Perimeter", "Motion", "Glassbreak", "ShockVibration", "PanicAlert", "Tamper",
            "NonRelaySmoke", "Sprinkler", "Heat", "CO", "Flood", "Sump",
            "120vSmoke", "Gas", "Humidity", "IntegratedEnvironmental", "Temperature", "Waterflow"
        ]
        grouped = {cat: [] for cat in categories}
        
        # Equipment type rules
        type_rules = {
            "Perimeter": ["1"],
            "Motion": ["2"],
            "Glassbreak": ["19"],
            "ShockVibration": ["54"],
            "PanicAlert": ["9", "34", "104"],
            "Tamper": ["124"],
            "NonRelaySmoke": ["5", "53"],
            "Heat": ["8"],
            "CO": ["6"],
            "Flood": ["16"]
        }
        
        # Subtype override mappings
        subtype_map = {
            "318": "Sump", "438": "Sump", "439": "Sump",
            "197": "Sprinkler", "198": "Sprinkler", "322": "Sprinkler",
            "119": "Flood", "120": "Flood",
            "187": "Heat", "188": "Heat",
            "108": "Glassbreak", "109": "Glassbreak",
            "110": "Perimeter", "111": "Perimeter", "194": "Perimeter",
            "195": "Perimeter", "222": "Perimeter", "223": "Perimeter",
            "221": "Perimeter", "225": "Perimeter", "224": "Perimeter",
            "167": "CO", "159": "CO"
        }
        
        for device in devices:
            # Check subtype first (priority override)
            equipment_subtype = str(device.get('equipment_subtype', '')).strip()
            if equipment_subtype in subtype_map:
                category = subtype_map[equipment_subtype]
                if category in grouped:
                    grouped[category].append(device)
                    continue
            
            # Fall back to equipment_type
            equipment_type = str(device.get('alarm_com_equipment_type', '')).strip()
            matched = False
            
            for category, types in type_rules.items():
                if equipment_type in types:
                    # Check exclusions for NonRelaySmoke
                    if category == "NonRelaySmoke":
                        desc = device.get('alarm_com_description', '').lower()
                        sensor_group = str(device.get('alarm_com_sensor_group', ''))
                        if 'relay' in desc or '120v' in desc or sensor_group == '48':
                            continue
                    
                    # Check exclusions for Heat
                    if category == "Heat":
                        sensor_group = str(device.get('alarm_com_sensor_group', ''))
                        if sensor_group == '48':
                            continue
                    
                    grouped[category].append(device)
                    matched = True
                    break
            
            # Check Sprinkler by sensor_group
            if not matched:
                sensor_group = str(device.get('alarm_com_sensor_group', ''))
                if sensor_group == '48':
                    grouped['Sprinkler'].append(device)
        
        # Sort devices in each category
        for category in grouped:
            grouped[category] = sorted(grouped[category], 
                                      key=lambda d: re.sub(r'^\d+\s+', '', 
                                                          d.get('alarm_com_description', '')).lower())
        
        return grouped
    
    def assemble_certificate_fields(self, hubspot_data: Dict, grouped_devices: Dict, 
                                   cert_number: str, broker_email: str = None, 
                                   requestor_name: str = None) -> Dict[str, Any]:
        """Assemble all fields for WebMerge"""
        
        site = hubspot_data.get('SITE', {})
        customer = hubspot_data.get('CUSTOMER', {})
        agreement = hubspot_data.get('AGREEMENT', {})
        system = hubspot_data.get('SYSTEM', {})
        
        # Calculate timestamp (PST)
        pst = timezone(timedelta(hours=-8))
        timestamp_dt = datetime.now(pst)
        timestamp_day = timestamp_dt.day
        timestamp_suffix = 'th' if 11 <= timestamp_day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(timestamp_day % 10, 'th')
        timestamp_str = f"{timestamp_dt.strftime('%B')} {timestamp_day}{timestamp_suffix}, {timestamp_dt.year} at {timestamp_dt.strftime('%I:%M%p').lower().lstrip('0')}"
        
        # Build certificate data
        cert_data = {
            # Provider info (corrected address)
            'provider_name': 'Provident Security Corp',
            'provider_address': '9123 Bentley Street, Unit 118',
            'provider_city': 'Vancouver',
            'provider_state': 'B.C.',
            'provider_zip': 'V6E 2E9',
            'provider_phone': '(604) 254-9734',
            
            # Certificate metadata
            'CERTIFICATE_Number': cert_number,
            'CERTIFICATE_TimeStamp': timestamp_str,
            
            # IDs
            'HSID': agreement.get('hs_object_id', ''),
            'HSID_Site': site.get('hs_object_id', ''),
            'HSID_System': system.get('hs_object_id', ''),
            
            # Site info
            'SITE_Name': site.get('mas_site_name') or site.get('name', ''),
            'SITE_Address1': site.get('address', ''),
            'SITE_Address2': site.get('address2', ''),
            'SITE_City': site.get('city', ''),
            'SITE_Province': site.get('state', ''),
            'SITE_PostalCode': self.transform_value(site.get('zip'), 'UPPER'),
            'SITE_Type': site.get('site_type', ''),
            
            # Customer info
            'AGREEMENT_Customer_Name': self.transform_value(
                [customer.get('firstname'), customer.get('lastname')], 'CONCAT_SPACE'),
            'AGREEMENT_Customer_Address1': customer.get('address', ''),
            'AGREEMENT_Customer_City': customer.get('city', ''),
            'AGREEMENT_Customer_Province': customer.get('state', ''),
            'AGREEMENT_Customer_PostalCode': self.transform_value(customer.get('zip'), 'UPPER'),
            
            # Agreement info
            'AGREEMENT_EffectiveDate': self.transform_value(
                agreement.get('agreement_plan_effectivedate'), 'DATETIME_FORMAT'),
            'AGREEMENT_Plan_ItemNumber': self.transform_value(
                agreement.get('agreement_plan_name'), 'EXTRACT_ITEM_NUMBER'),
            'AGREEMENT_Plan_Supervision': self.transform_value(
                agreement.get('agreement_plan_supervision'), 'EXTRACT_ITEM_NUMBER'),
            'AGREEMENT_Response_Guarantee': agreement.get('agreement_plan_response_guarantee', ''),
            
            # Communication paths
            'Path_Primary': self.transform_value(
                system.get('communication_path_1'), 'COMM_PATH_LABEL'),
            'Path_Secondary': self.transform_value(
                system.get('communication_path_2'), 'COMM_PATH_LABEL'),
            'Path_Phrase': self.transform_value(
                [system.get('communication_path_1'), system.get('communication_path_2')], 
                'COMM_PATH_PHRASE'),
            
            # Partitions
            'AGREEMENT_Partitions_Included': agreement.get('agreement_partitions_included', ''),
            'AGREEMENT_Partitions_Additional': agreement.get('agreement_partitions_additional', ''),
            'AGREEMENT_Partitions_Total': agreement.get('agreement_partitions_total', ''),
            
            # System integrations
            'Integrations_Gate_Fire': system.get('integrations_gate_fire', ''),
            
            # Recipient info (from user input)
            'CERTIFICATE_Recipient_Name': requestor_name or '',
            'CERTIFICATE_Recipient_Company': broker_email or '',
        }
        
        # Add all agreement status fields
        status_fields = [
            '15746', '15747', '14380', '14385', '14386', '14387', '14388', '14399', '15082',
            '14389', '14390', '14391', '14401', '14393', '14394', '14395', '14396', '14397',
            '14398', '14726', '15711', '15787', '15272', '14920', '14631', '15252', '14372',
            '15756', '15753', '15754', '14366', '14367'
        ]
        
        for field_num in status_fields:
            # Try different property name patterns
            for prefix in ['agreement_plan_communication_', 'agreement_plan_gateway_', 
                          'agreement_plan_intrusion_', 'agreement_plan_fire_', 
                          'agreement_plan_environmental_', 'agreement_plan_integratedsystems_',
                          'agreement_plan_response_', 'agreement_plan_verify_', 
                          'agreement_plan_trespass_']:
                prop_name = f'{prefix}{field_num}'
                if prop_name in agreement:
                    cert_data[f'AGREEMENT_Status_{field_num}'] = agreement.get(prop_name, '')
                    break
        
        # Add device zones and counts
        for category in grouped_devices.keys():
            devices = grouped_devices[category]
            device_list = []
            
            for device in devices:
                desc = device.get('alarm_com_description', 'Unknown Device')
                desc_clean = re.sub(r'^\d+\s+', '', desc)
                device_list.append({
                    'name': desc_clean,
                    'zone': device.get('adc_deviceid') or device.get('zone__') or ''
                })
            
            cert_data[f'Zones_{category}'] = device_list
            cert_data[f'Count_{category}'] = len(device_list)
        
        return cert_data
    
    def generate_certificate_data(self, agreement_id: str, system_id: str, site_id: str,
                                  broker_email: str = None, requestor_name: str = None) -> Dict[str, Any]:
        """
        Main orchestration method - generates complete certificate data
        
        Returns dict ready to send to WebMerge
        """
        print(f"\n{'='*60}")
        print(f"GENERATING CERTIFICATE DATA")
        print(f"Agreement: {agreement_id}")
        print(f"System: {system_id}")
        print(f"Site: {site_id}")
        print(f"{'='*60}\n")
        
        # Step 1: Fetch data from HubSpot
        print("Step 1: Fetching HubSpot data...")
        hubspot_data = self.fetch_hubspot_data(agreement_id, system_id, site_id)
        
        # Step 2: Validate data
        print("Step 2: Validating data...")
        is_valid, errors = self.validate_data(hubspot_data)
        
        if not is_valid:
            raise ValueError(f"Validation failed: {', '.join(errors)}")
        
        print("✓ Validation passed")
        
        # Step 3: Generate certificate number
        print("Step 3: Generating certificate number...")
        cert_number = self.generate_certificate_number(site_id, system_id)
        
        # Step 4: Group devices
        print("Step 4: Grouping devices...")
        grouped_devices = self.group_devices(hubspot_data['DEVICES'])
        
        device_summary = {cat: len(devices) for cat, devices in grouped_devices.items() if devices}
        print(f"✓ Device groups: {device_summary}")
        
        # Step 5: Assemble all fields
        print("Step 5: Assembling certificate fields...")
        cert_data = self.assemble_certificate_fields(
            hubspot_data, 
            grouped_devices, 
            cert_number,
            broker_email,
            requestor_name
        )
        print(f"DEBUG cert_data keys: {list(cert_data.keys())[:10]}", flush=True)
        print(f"DEBUG CERTIFICATE_TimeStamp in cert_data: {'CERTIFICATE_TimeStamp' in cert_data}", flush=True)
        print(f"DEBUG CERTIFICATE_TimeStamp value: {cert_data.get('CERTIFICATE_TimeStamp')}", flush=True)
        print(f"✓ Assembled {len(cert_data)} fields")
        print(f"\n{'='*60}")
        print("CERTIFICATE DATA READY")
        print(f"{'='*60}\n")
        
        return cert_data
