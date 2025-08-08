import json
import os
import glob
from typing import List, Tuple, Dict
from datetime import datetime
import pandas as pd
from data_readers.green_ppg_reader import read_green_ppg_json


def get_green_ppg_files_for_day(data_dir: str, date_str: str) -> List[str]:
    """
    Get all green PPG data files for a specific day.
    
    Args:
        data_dir: Directory containing the data files
        date_str: Date string in format 'DD.MM.YY' (e.g., '06.08.25')
    
    Returns:
        List of file paths sorted by actual timestamp (not filename)
    """
    pattern = os.path.join(data_dir, f"green_ppg_data_{date_str}_*.json")
    files = glob.glob(pattern)
    
    # Sort files by actual first timestamp in the file
    def get_first_timestamp(filepath):
        try:
            first_ts, _ = get_file_timestamps(filepath)
            return first_ts if first_ts is not None else 0
        except:
            return 0
    
    files.sort(key=get_first_timestamp)
    return files


def get_file_timestamps(filepath: str) -> Tuple[int, int]:
    """
    Get the first and last timestamps from a green PPG data file.
    
    Args:
        filepath: Path to the JSON file
    
    Returns:
        Tuple of (first_timestamp_ms, last_timestamp_ms)
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        samples = data.get('green_ppg_samples', [])
        if not samples:
            return None, None
        
        first_timestamp = int(samples[0]['unix_timestamp_in_ms'])
        last_timestamp = int(samples[-1]['unix_timestamp_in_ms'])
        
        return first_timestamp, last_timestamp
    
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return None, None


def read_green_ppg_file(filepath: str) -> pd.DataFrame:
    """
    Read a green PPG JSON file and return a pandas DataFrame.
    Uses the existing read_green_ppg_json function from data_readers.
    
    Args:
        filepath: Path to the JSON file
    
    Returns:
        DataFrame with columns: ppg_green_value, datetime
    """
    try:
        return read_green_ppg_json(filepath)
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return pd.DataFrame()


def concatenate_green_ppg_data(data_dir: str, date_str: str) -> pd.DataFrame:
    """
    Concatenate all green PPG data files for a specific day into a single DataFrame.
    
    Args:
        data_dir: Directory containing the data files
        date_str: Date string in format 'DD.MM.YY' (e.g., '06.08.25')
    
    Returns:
        Concatenated DataFrame with all data for the day
    """
    files = get_green_ppg_files_for_day(data_dir, date_str)
    
    if not files:
        print(f"No green PPG files found for date {date_str}")
        return pd.DataFrame()
    
    # Sort files in chronological order
    files.sort(key=lambda x: os.path.basename(x).split('_')[3].replace('.json', ''))
    
    print(f"Concatenating {len(files)} green PPG files for date {date_str}...")
    
    all_dataframes = []
    total_samples = 0
    
    for i, filepath in enumerate(files):
        print(f"Reading file {i+1}/{len(files)}: {os.path.basename(filepath)}")
        df = read_green_ppg_file(filepath)
        
        if not df.empty:
            all_dataframes.append(df)
            total_samples += len(df)
            print(f"  Added {len(df)} samples (total: {total_samples})")
        else:
            print(f"  ⚠️  No data found in {os.path.basename(filepath)}")
    
    if all_dataframes:
        concatenated_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Sort by datetime to ensure chronological order
        if 'datetime' in concatenated_df.columns:
            concatenated_df = concatenated_df.sort_values('datetime').reset_index(drop=True)
        
        print(f"\n✅ Successfully concatenated {len(concatenated_df)} total samples from {len(all_dataframes)} files")
        print(f"Time range: {concatenated_df['datetime'].min()} to {concatenated_df['datetime'].max()}")
        
        return concatenated_df
    else:
        print("❌ No valid data found in any files")
        return pd.DataFrame()


def export_concatenated_data(df: pd.DataFrame, date_str: str, output_dir: str = "data"):
    """
    Export the concatenated DataFrame as both PKL and CSV files.
    
    Args:
        df: DataFrame to export
        date_str: Date string for filename
        output_dir: Directory to save the exported files
    """
    if df.empty:
        print("❌ No data to export")
        return
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filenames
    base_filename = f"concatenated_green_ppg_{date_str}"
    pkl_filename = os.path.join(output_dir, f"{base_filename}.pkl")
    csv_filename = os.path.join(output_dir, f"{base_filename}.csv")
    
    try:
        # Export as PKL
        df.to_pickle(pkl_filename)
        print(f"✅ Exported PKL file: {pkl_filename}")
        
        # Export as CSV
        df.to_csv(csv_filename, index=False)
        print(f"✅ Exported CSV file: {csv_filename}")
        
        # Print summary statistics
        print(f"\n📊 Data Summary:")
        print(f"  Total samples: {len(df):,}")
        print(f"  Time range: {df['datetime'].min()} to {df['datetime'].max()}")
        print(f"  Duration: {df['datetime'].max() - df['datetime'].min()}")
        print(f"  PPG value range: {df['ppg_green_value'].min():,} to {df['ppg_green_value'].max():,}")
        print(f"  File sizes: PKL={os.path.getsize(pkl_filename)/1024/1024:.2f}MB, CSV={os.path.getsize(csv_filename)/1024/1024:.2f}MB")
        
    except Exception as e:
        print(f"❌ Error exporting data: {e}")


def check_continuity_for_day(data_dir: str, date_str: str, max_gap_ms: int = 40, error_threshold_ms: int = 80) -> Dict:
    """
    Check the continuity of green PPG data files for a specific day.
    
    Args:
        data_dir: Directory containing the data files
        date_str: Date string in format 'DD.MM.YY' (e.g., '06.08.25')
        max_gap_ms: Maximum allowed gap between consecutive files in milliseconds
    
    Returns:
        Dictionary with continuity analysis results
    """
    files = get_green_ppg_files_for_day(data_dir, date_str)
    
    if not files:
        return {
            'date': date_str,
            'total_files': 0,
            'gaps_found': [],
            'continuity_issues': [],
            'summary': f"No green PPG files found for date {date_str}"
        }
    
    print(f"Found {len(files)} green PPG files for date {date_str}")
    
    gaps = []
    continuity_issues = []
    
    for i in range(len(files) - 1):
        current_file = files[i]
        next_file = files[i + 1]
        
        current_first, current_last = get_file_timestamps(current_file)
        next_first, next_last = get_file_timestamps(next_file)
        
        if current_last is None or next_first is None:
            continuity_issues.append({
                'current_file': os.path.basename(current_file),
                'next_file': os.path.basename(next_file),
                'issue': 'Could not read timestamps from one or both files'
            })
            continue
        
        # Calculate gap between current file's last timestamp and next file's first timestamp
        gap_ms = next_first - current_last
        
        if gap_ms > max_gap_ms:
            gaps.append({
                'current_file': os.path.basename(current_file),
                'next_file': os.path.basename(next_file),
                'current_last_timestamp': current_last,
                'next_first_timestamp': next_first,
                'gap_ms': gap_ms,
                'gap_seconds': gap_ms / 1000.0
            })
        
        if gap_ms > max_gap_ms:
            print(f"File {i+1}/{len(files)-1}: {os.path.basename(current_file)} -> {os.path.basename(next_file)}")
            print(f"  Gap: {gap_ms} ms ({gap_ms/1000.0:.3f} seconds)")
            if gap_ms > error_threshold_ms:
                print(f"  ❌  GAP EXCEEDS {error_threshold_ms}ms THRESHOLD!")
            else:
                print(f"  ⚠️  GAP EXCEEDS {max_gap_ms}ms THRESHOLD!")
            print()
    
    return {
        'date': date_str,
        'total_files': len(files),
        'files_checked': len(files) - 1,
        'gaps_found': gaps,
        'continuity_issues': continuity_issues,
        'max_gap_threshold_ms': max_gap_ms,
        'error_threshold_ms': error_threshold_ms,
        'summary': f"Found {len(gaps)} gaps exceeding {max_gap_ms}ms threshold out of {len(files)-1} file transitions"
    }





def main():
    """
    Main function to run the continuity checker and concatenate data.
    """
    # Configuration
    data_dir = "data/Smartwatch"
    date_str = "06.08.25"  # Change this to the date you want to check
    max_gap_ms = 40
    
    print(f"🔍 GREEN PPG DATA PROCESSING")
    print(f"Date: {date_str}")
    print(f"Data directory: {data_dir}")
    print(f"Max gap threshold: {max_gap_ms}ms")
    print("=" * 80)
    
    # Step 1: Check continuity
    print("\n📋 STEP 1: Checking data continuity...")
    results = check_continuity_for_day(data_dir, date_str, max_gap_ms=max_gap_ms, error_threshold_ms=80)
    
    # Step 2: Concatenate data
    print("\n📦 STEP 2: Concatenating all data files...")
    concatenated_df = concatenate_green_ppg_data(data_dir, date_str)
    
    # Step 3: Export data
    if not concatenated_df.empty:
        print("\n💾 STEP 3: Exporting concatenated data...")
        export_concatenated_data(concatenated_df, date_str, output_dir="data")
        
        print(f"\n🎉 PROCESSING COMPLETE!")
        print(f"✅ Data concatenation: {len(concatenated_df):,} samples processed")
        print(f"✅ Data export: PKL and CSV files created in data/ directory")
    else:
        print("\n❌ PROCESSING FAILED: No data to process")
    
    print("=" * 80)


if __name__ == "__main__":
    main() 