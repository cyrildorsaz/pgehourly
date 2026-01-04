import requests
import pandas as pd
from datetime import datetime
import json
import re

def fetch_energy_data(start_date, end_date):
    """
    Fetch energy pricing data from the API.

    Args:
        start_date (str): Start date in YYYYMMDD format
        end_date (str): End date in YYYYMMDD format

    Returns:
        dict: JSON response from the API
    """
    base_url = "https://pge-pe-api.gridx.com/v1/getPricing"
    params = {
        "utility": "PGE",
        "market": "DAM",
        "startdate": start_date,
        "enddate": end_date,
        "ratename": "EV2A",
        "representativeCircuitId": "024040403",
        "program": "CalFUSE"
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch data: {str(e)}")

def process_pricing_data(data):
    """
    Process the raw JSON data into a pandas DataFrame.

    Args:
        data (dict): Raw JSON data from the API

    Returns:
        pandas.DataFrame: Processed data
    """
    try:
        # Extract pricing data
        pricing_data = []
        skipped_count = 0

        # Check if data structure is as expected
        if not isinstance(data, dict):
            raise ValueError(f"Expected dict but got {type(data).__name__}")
        
        data_list = data.get('data', [])
        if not data_list:
            # Log the actual structure for debugging
            print(f"Warning: No 'data' key found or empty data list. Keys in response: {list(data.keys())}")
            raise ValueError("No data found in API response. The API may not have data for the selected date range.")

        # Navigate through the nested structure
        for data_item in data_list:
            if not isinstance(data_item, dict):
                continue
                
            price_details = data_item.get('priceDetails', [])
            if not price_details:
                continue

            for detail in price_details:
                if not isinstance(detail, dict):
                    continue
                    
                datetime_str = detail.get('startIntervalTimeStamp')
                price = detail.get('intervalPrice')

                if datetime_str and price is not None:
                    try:
                        # Parse datetime - handle various timezone formats
                        # Remove timezone offset (e.g., -0800, -0700, +0000, etc.)
                        # Use regex to remove timezone offset
                        datetime_clean = re.sub(r'[+-]\d{4}$', '', datetime_str)
                        
                        # Try parsing with common formats
                        try:
                            dt = datetime.strptime(datetime_clean, '%Y-%m-%dT%H:%M:%S')
                        except ValueError:
                            # Try with milliseconds
                            datetime_clean_ms = re.sub(r'\.\d+', '', datetime_clean)
                            dt = datetime.strptime(datetime_clean_ms, '%Y-%m-%dT%H:%M:%S')
                        
                        # Convert price to float
                        price_float = float(price)
                        
                        pricing_data.append({
                            'datetime': dt,
                            'price': price_float
                        })
                    except (ValueError, TypeError) as e:
                        skipped_count += 1
                        print(f"Warning: Skipping record due to parsing error: {str(e)}")
                        print(f"  datetime_str: {datetime_str}, price: {price}")
                        continue

        if not pricing_data:
            # Provide more detailed error message
            error_msg = "No valid pricing data records found after processing."
            if skipped_count > 0:
                error_msg += f" Skipped {skipped_count} invalid records."
            error_msg += " This may indicate: (1) No data available for the selected date range, (2) API response structure changed, or (3) All records had parsing errors."
            raise ValueError(error_msg)

        # Create DataFrame
        df = pd.DataFrame(pricing_data)

        # Sort by datetime
        df = df.sort_values('datetime')

        return df

    except Exception as e:
        print(f"Error processing data: {str(e)}")
        print("Data received:", json.dumps(data, indent=2)[:1000])  # Limit output to first 1000 chars
        raise Exception(f"Failed to process data: {str(e)}")
