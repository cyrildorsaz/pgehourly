import requests
import pandas as pd
from datetime import datetime
import json

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
        "representativeCircuitId": "012640401",
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

        # Navigate through the nested structure
        for data_item in data.get('data', []):
            price_details = data_item.get('priceDetails', [])

            for detail in price_details:
                datetime_str = detail.get('startIntervalTimeStamp')
                price = detail.get('intervalPrice')

                if datetime_str and price is not None:
                    try:
                        # Parse datetime (format: '2025-02-08T00:00:00-0800')
                        dt = datetime.strptime(datetime_str.split('-0700')[0], '%Y-%m-%dT%H:%M:%S')
                        pricing_data.append({
                            'datetime': dt,
                            'price': float(price)
                        })
                    except ValueError as e:
                        print(f"Warning: Skipping record due to datetime parsing error: {str(e)}")
                        continue

        if not pricing_data:
            raise ValueError("No valid pricing data records found after processing")

        # Create DataFrame
        df = pd.DataFrame(pricing_data)

        # Sort by datetime
        df = df.sort_values('datetime')

        return df

    except Exception as e:
        print(f"Error processing data: {str(e)}")
        print("Data received:", json.dumps(data, indent=2))
        raise Exception(f"Failed to process data: {str(e)}")
