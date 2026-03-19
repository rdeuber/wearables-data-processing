import pandas as pd
from typing import Union
import os


def read_polar_ppg_csv(filepath: Union[str, bytes]) -> pd.DataFrame:
    """
    Reads a Polar smartwatch PPG CSV file and returns a pandas DataFrame with columns:
    - DevTime_us: Device time in microseconds
    - unix_dev_ms: Unix timestamp in milliseconds  
    - PPG0, PPG1, PPG2: Three PPG channels
    - ambient: Ambient light sensor
    - PPGsum: Sum of PPG values
    - acc_x: Accelerometer X-axis
    - acc_v_reduced: Reduced accelerometer value
    
    Args:
        filepath: Path to the Polar CSV file
    
    Returns:
        DataFrame with datetime as index (converted from unix_dev_ms, in US/Pacific timezone)
        and all original columns as data columns, sorted chronologically
    """
    try:
        # Read the CSV file
        df = pd.read_csv(filepath)
        
        # Convert unix_dev_ms to datetime if present
        if 'unix_dev_ms' in df.columns:
            df['datetime'] = pd.to_datetime(pd.to_numeric(df['unix_dev_ms'], errors='coerce'), unit='ms', errors='coerce', utc=True)
            df['datetime'] = df['datetime'].dt.tz_convert('US/Pacific')
            
            # Set datetime as index and sort by it
            df = df.set_index('datetime').sort_index()
        
        return df
        
    except Exception as e:
        print(f"Error reading Polar CSV file {filepath}: {e}")
        return pd.DataFrame()


def get_polar_file_info(filepath: str) -> dict:
    """
    Get information about a Polar CSV file.
    
    Args:
        filepath: Path to the Polar CSV file
    
    Returns:
        Dictionary with file information
    """
    try:
        df = read_polar_ppg_csv(filepath)
        
        if df.empty:
            return {
                'error': 'Could not read file or file is empty',
                'filepath': filepath
            }
        
        info = {
            'filepath': filepath,
            'filename': os.path.basename(filepath),
            'total_rows': len(df),
            'columns': list(df.columns),
            'file_size_mb': os.path.getsize(filepath) / (1024 * 1024)
        }
        
        # Add time range if datetime column exists
        if 'datetime' in df.columns:
            info['time_range'] = {
                'start': df['datetime'].min().isoformat(),
                'end': df['datetime'].max().isoformat(),
                'duration_seconds': (df['datetime'].max() - df['datetime'].min()).total_seconds()
            }
        
        # Add data ranges for numerical columns
        numerical_cols = ['PPG0', 'PPG1', 'PPG2', 'ambient', 'PPGsum', 'acc_x', 'acc_v_reduced']
        for col in numerical_cols:
            if col in df.columns:
                info[f'{col}_range'] = {
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'mean': df[col].mean()
                }
        
        return info
        
    except Exception as e:
        return {
            'error': str(e),
            'filepath': filepath
        } 