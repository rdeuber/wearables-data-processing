import json
import os
import glob
from typing import List, Tuple, Dict
from datetime import datetime
import pandas as pd
from data_readers.galaxy_heart_rate_reader import read_galaxy_heart_rate_json


def get_heart_rate_files_for_day(data_dir: str, date_str: str) -> List[str]:
    """
    Get all heart rate data files for a specific day.
    
    Args:
        data_dir: Directory containing the data files
        date_str: Date string in format 'DD.MM.YY' (e.g., '06.08.25')
    
    Returns:
        List of file paths sorted by actual timestamp (not filename)
    """
    pattern = os.path.join(data_dir, f"heart_rate_data_{date_str}_*.json")
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
    Get the first and last timestamps from a heart rate data file.
    
    Args:
        filepath: Path to the JSON file
    
    Returns:
        Tuple of (first_timestamp_ms, last_timestamp_ms)
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        samples = data.get('samples', [])
        if not samples:
            return None, None
        
        first_timestamp = int(samples[0]['unix_timestamp_in_ms'])
        last_timestamp = int(samples[-1]['unix_timestamp_in_ms'])
        
        return first_timestamp, last_timestamp
    
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return None, None


def read_heart_rate_file(filepath: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read a Galaxy heart rate JSON file and return pandas DataFrames.
    Uses the existing read_galaxy_heart_rate_json function from data_readers.
    
    Args:
        filepath: Path to the JSON file
    
    Returns:
        Tuple of (main_df, ibi_df) DataFrames
    """
    try:
        return read_galaxy_heart_rate_json(filepath, ibi_timestamp_method='forward')
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return pd.DataFrame(), pd.DataFrame()


def concatenate_heart_rate_data(data_dir: str, date_str: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Concatenate all heart rate data files for a specific day into DataFrames.
    
    Args:
        data_dir: Directory containing the data files
        date_str: Date string in format 'DD.MM.YY' (e.g., '06.08.25')
    
    Returns:
        Tuple of (main_df, ibi_df) concatenated DataFrames
    """
    files = get_heart_rate_files_for_day(data_dir, date_str)
    
    if not files:
        print(f"No heart rate files found for date {date_str}")
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"Concatenating {len(files)} heart rate files for date {date_str}...")
    
    all_main_dataframes = []
    all_ibi_dataframes = []
    total_main_samples = 0
    total_ibi_samples = 0
    
    for i, filepath in enumerate(files):
        print(f"Reading file {i+1}/{len(files)}: {os.path.basename(filepath)}")
        main_df, ibi_df = read_heart_rate_file(filepath)
        
        if not main_df.empty:
            all_main_dataframes.append(main_df)
            total_main_samples += len(main_df)
            print(f"  Added {len(main_df)} main samples (total: {total_main_samples})")
        else:
            print(f"  ⚠️  No main data found in {os.path.basename(filepath)}")
        
        if not ibi_df.empty:
            all_ibi_dataframes.append(ibi_df)
            total_ibi_samples += len(ibi_df)
            print(f"  Added {len(ibi_df)} IBI samples (total: {total_ibi_samples})")
        else:
            print(f"  ⚠️  No IBI data found in {os.path.basename(filepath)}")
    
    # Concatenate main data
    if all_main_dataframes:
        concatenated_main_df = pd.concat(all_main_dataframes, ignore_index=True)
        
        # Sort by datetime to ensure chronological order
        if 'datetime' in concatenated_main_df.columns:
            concatenated_main_df = concatenated_main_df.sort_values('datetime').reset_index(drop=True)
        
        print(f"\n✅ Successfully concatenated {len(concatenated_main_df)} main samples from {len(all_main_dataframes)} files")
        if 'datetime' in concatenated_main_df.columns:
            print(f"Main data time range: {concatenated_main_df['datetime'].min()} to {concatenated_main_df['datetime'].max()}")
    else:
        concatenated_main_df = pd.DataFrame()
        print("❌ No valid main data found in any files")
    
    # Concatenate IBI data
    if all_ibi_dataframes:
        concatenated_ibi_df = pd.concat(all_ibi_dataframes, ignore_index=True)
        
        # Sort by datetime to ensure chronological order
        if 'datetime' in concatenated_ibi_df.columns:
            concatenated_ibi_df = concatenated_ibi_df.sort_values('datetime').reset_index(drop=True)
        
        print(f"✅ Successfully concatenated {len(concatenated_ibi_df)} IBI samples from {len(all_ibi_dataframes)} files")
        if 'datetime' in concatenated_ibi_df.columns:
            print(f"IBI data time range: {concatenated_ibi_df['datetime'].min()} to {concatenated_ibi_df['datetime'].max()}")
    else:
        concatenated_ibi_df = pd.DataFrame()
        print("❌ No valid IBI data found in any files")
    
    return concatenated_main_df, concatenated_ibi_df


def export_concatenated_data(main_df: pd.DataFrame, ibi_df: pd.DataFrame, date_str: str, output_dir: str = "data"):
    """
    Export the concatenated DataFrames as both PKL and CSV files.
    
    Args:
        main_df: Main heart rate DataFrame to export
        ibi_df: IBI DataFrame to export
        date_str: Date string for filename
        output_dir: Directory to save the exported files
    """
    if main_df.empty and ibi_df.empty:
        print("❌ No data to export")
        return
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filenames
    base_filename = f"concatenated_heart_rate_{date_str}"
    main_pkl_filename = os.path.join(output_dir, f"{base_filename}_main.pkl")
    main_csv_filename = os.path.join(output_dir, f"{base_filename}_main.csv")
    ibi_pkl_filename = os.path.join(output_dir, f"{base_filename}_ibi.pkl")
    ibi_csv_filename = os.path.join(output_dir, f"{base_filename}_ibi.csv")
    
    try:
        # Export main data
        if not main_df.empty:
            main_df.to_pickle(main_pkl_filename)
            print(f"✅ Exported main PKL file: {main_pkl_filename}")
            
            main_df.to_csv(main_csv_filename, index=False)
            print(f"✅ Exported main CSV file: {main_csv_filename}")
        
        # Export IBI data
        if not ibi_df.empty:
            ibi_df.to_pickle(ibi_pkl_filename)
            print(f"✅ Exported IBI PKL file: {ibi_pkl_filename}")
            
            ibi_df.to_csv(ibi_csv_filename, index=False)
            print(f"✅ Exported IBI CSV file: {ibi_csv_filename}")
        
        # Print summary statistics
        print(f"\n📊 Data Summary:")
        if not main_df.empty:
            print(f"  Main data samples: {len(main_df):,}")
            if 'datetime' in main_df.columns:
                print(f"  Main data time range: {main_df['datetime'].min()} to {main_df['datetime'].max()}")
            if 'hr' in main_df.columns:
                print(f"  Heart rate range: {main_df['hr'].min()} to {main_df['hr'].max()} bpm")
            print(f"  Main file sizes: PKL={os.path.getsize(main_pkl_filename)/1024/1024:.2f}MB, CSV={os.path.getsize(main_csv_filename)/1024/1024:.2f}MB")
        
        if not ibi_df.empty:
            print(f"  IBI samples: {len(ibi_df):,}")
            if 'datetime' in ibi_df.columns:
                print(f"  IBI time range: {ibi_df['datetime'].min()} to {ibi_df['datetime'].max()}")
            if 'ibi' in ibi_df.columns:
                print(f"  IBI range: {ibi_df['ibi'].min()} to {ibi_df['ibi'].max()} ms")
            print(f"  IBI file sizes: PKL={os.path.getsize(ibi_pkl_filename)/1024/1024:.2f}MB, CSV={os.path.getsize(ibi_csv_filename)/1024/1024:.2f}MB")
        
    except Exception as e:
        print(f"❌ Error exporting data: {e}")


def check_continuity_for_day(data_dir: str, date_str: str, max_gap_ms: int = 40, error_threshold_ms: int = 80) -> Dict:
    """
    Check the continuity of heart rate data files for a specific day.
    
    Args:
        data_dir: Directory containing the data files
        date_str: Date string in format 'DD.MM.YY' (e.g., '06.08.25')
        max_gap_ms: Maximum allowed gap between consecutive files in milliseconds
        error_threshold_ms: Threshold for error-level gaps in milliseconds
    
    Returns:
        Dictionary with continuity analysis results
    """
    files = get_heart_rate_files_for_day(data_dir, date_str)
    
    if not files:
        return {
            'date': date_str,
            'total_files': 0,
            'gaps_found': [],
            'continuity_issues': [],
            'summary': f"No heart rate files found for date {date_str}"
        }
    
    print(f"Found {len(files)} heart rate files for date {date_str}")
    
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
    Main function to run the heart rate continuity checker and concatenate data.
    """
    # Configuration
    data_dir = "data/smartwatch_data"
    date_str = "06.08.25"  # Change this to the date you want to check
    max_gap_ms = 40
    error_threshold_ms = 80
    
    print(f"🔍 GALAXY HEART RATE DATA PROCESSING")
    print(f"Date: {date_str}")
    print(f"Data directory: {data_dir}")
    print(f"Max gap threshold: {max_gap_ms}ms")
    print(f"Error threshold: {error_threshold_ms}ms")
    print("=" * 80)
    
    # Step 1: Check continuity
    print("\n📋 STEP 1: Checking data continuity...")
    results = check_continuity_for_day(data_dir, date_str, max_gap_ms=max_gap_ms, error_threshold_ms=error_threshold_ms)
    
    # Step 2: Concatenate data
    print("\n📦 STEP 2: Concatenating all data files...")
    main_df, ibi_df = concatenate_heart_rate_data(data_dir, date_str)
    
    # Step 3: Export data
    if not main_df.empty or not ibi_df.empty:
        print("\n💾 STEP 3: Exporting concatenated data...")
        export_concatenated_data(main_df, ibi_df, date_str, output_dir="data")
        
        print(f"\n🎉 PROCESSING COMPLETE!")
        if not main_df.empty:
            print(f"✅ Main data concatenation: {len(main_df):,} samples processed")
        if not ibi_df.empty:
            print(f"✅ IBI data concatenation: {len(ibi_df):,} samples processed")
        print(f"✅ Data export: PKL and CSV files created in data/ directory")
    else:
        print("\n❌ PROCESSING FAILED: No data to process")
    
    print("=" * 80)


if __name__ == "__main__":
    main() 