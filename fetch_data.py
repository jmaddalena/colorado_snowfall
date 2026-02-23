import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from io import StringIO
import time

def get_colorado_sites():
    """Scrape the Colorado snow course sites page to get site IDs and names."""
    url = "https://wcc.sc.egov.usda.gov/nwcc/snow-course-sites.jsp?state=CO"
    
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    sites = []
    # Find the table containing site information
    table = soup.find('table')
    
    if table:
        # Process each row in the table
        for row in table.find_all('tr')[1:]:  # Skip header row
           # print(row)
            cells = row.find_all('td')
            # Find the link in the row
            for link in row.find_all('a', href=True):
                href = link['href']
                if 'station' in href and 'snowmonth_hist' in href:  
                    print(href)
                    # Extract site number from URL
                    match = re.search(r'station=([^&]+)', href)

                    if match:
                        station = match.group(1)

                        site_name = cells[1].text.strip()
                        latitude = cells[5].text.strip()
                        longitude = cells[6].text.strip()
                        county = cells[9].text.strip()
                                                
                        sites.append({
                            'station': station,
                            'site_name': site_name,
                            'latitude': latitude,
                            'longitude': longitude,
                            'county': county
                        })
    
    return pd.DataFrame(sites)

def download_site_data(site_num):
    """Download historical snow data for a specific site."""
    # The report generator URL for historical data
    #url = f"https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultipleStationReport/monthly/start_of_period/{site_num}:CO:SNTL%7Cid=%22%22%7Cname/-39,0/WTEQ::value,SNWD::value,WTEQ::value"
    url = f"https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customGroupByMonthReport/monthly/{site_num}:CO:SNOW%7Cid=%22%22%7Cname/POR_BEGIN,POR_END:1,2,3,4,5,6/WTEQ::collectionDate,SNWD::value,WTEQ::value"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error downloading ({site_num}): {e}")
        return None

def parse_snow_data(raw_data, station):
    """Parse the snow data CSV into a clean DataFrame."""
    if not raw_data:
        return None
    
    lines = raw_data.split('\n')
    
    # Find the data section (starts after the header rows)
    data_start = None
    for i, line in enumerate(lines):
        if line.startswith('Water Year,'):
            data_start = i
            break
    
    if data_start is None:
        return None

    # Get the actual data rows (skip the measurement description row)
    data_lines = [lines[data_start]] + lines[data_start + 2:]
    data_text = '\n'.join(data_lines)
    
    # Parse with pandas
    df = pd.read_csv(StringIO(data_text), header=0)
    
    # Create cleaner column names
    # Pattern: Jan.1 = date, Jan.2 = snow depth, Jan.3 = SWE
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']
    new_columns = ['water_year']
    
    for month in month_names:
        new_columns.extend([
            f'{month}_date',
            f'{month}_snow_depth_in',
            f'{month}_swe_in'
        ])
    
    df.columns = new_columns
    
    # Add site information
    df['station'] = station
    
    # Remove empty rows
    df = df[df['water_year'].notna()]
    
    return df

def pivot_to_long_format(df):
    """
    Pivot the wide-format snow data to long format.
    
    Input: Wide format with columns like Jan_date, Jan_snow_depth_in, Jan_swe_in
    Output: Long format with one row per year-month-site combination
    
    Returns DataFrame with columns:
    - water_year, site_num, site_name, state, county, month, 
      collection_date, snow_depth_in, swe_in
    """
    
    # ID columns that don't vary by month
    id_cols = ['water_year', 'station', 'site_name', 'county', 'latitude', 'longitude']
    
    # Month names in the data
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']
    
    # Build list of dataframes for each month
    monthly_dfs = []
    
    for month in months:
        print(month)

        cols = id_cols + [
            f'{month}_date',
            f'{month}_snow_depth_in', 
            f'{month}_swe_in'
        ]

        print(cols)

        # Extract columns for this month
        month_df = df[cols].copy()
        
        # Rename columns to standard names
        month_df.columns = id_cols + ['collection_date', 'snow_depth_in', 'swe_in']
        
        # Add month column
        month_df['month'] = month
        
        # Remove rows where all snow measurements are missing
        month_df = month_df.dropna(subset=['collection_date', 'snow_depth_in', 'swe_in'], how='all')
        
        monthly_dfs.append(month_df)
    
    # Concatenate all months
    long_df = pd.concat(monthly_dfs, ignore_index=True)
    
    # Reorder columns for better readability
    long_df = long_df[id_cols + ['month', 'collection_date', 'snow_depth_in', 'swe_in']
    ]
    
    # Sort by site, year, and month
    month_order = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6}
    long_df['month_num'] = long_df['month'].map(month_order)
    long_df = long_df.sort_values(['county', 'station', 'water_year', 'month_num'])
    long_df = long_df.drop('month_num', axis=1).reset_index(drop=True)
    
    return long_df

def download_all_colorado_sites(save_path='colorado_snow_data.csv', delay=1):
    """Download and combine data from all Colorado snow course sites."""
    print("Fetching list of Colorado sites...")
    sites_df = get_colorado_sites()
    print(f"Found {len(sites_df)} sites")
    
    all_data = []
    
    for idx, row in sites_df.iterrows():
        site_num = row['site_num']
        site_name = row['site_name']
        
        print(f"Downloading {idx+1}/{len(sites_df)}: {site_name} ({site_num})...")
        
        raw_data = download_site_data(site_num, site_name)
        if raw_data:
            df = parse_snow_data(raw_data, site_num, site_name)
            if df is not None and len(df) > 0:
                all_data.append(df)
                print(f"  ✓ Got {len(df)} years of data")
            else:
                print(f"  ✗ No data available")
        
        # Be nice to the server
        time.sleep(delay)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_csv(save_path, index=False)
        print(f"\n✓ Downloaded data for {len(all_data)} sites")
        print(f"✓ Saved to {save_path}")
        print(f"✓ Total rows: {len(combined_df)}")
        return combined_df
    else:
        print("No data was downloaded")
        return None

# Example usage
if __name__ == "__main__":
    # Download all sites
    df = download_all_colorado_sites()
    
    # Display sample
    if df is not None:
        print("\nSample of downloaded data:")
        print(df.head())
        print("\nColumns:", df.columns.tolist())
        print("\nData shape:", df.shape)