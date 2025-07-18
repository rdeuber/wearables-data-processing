import json
import pandas as pd
from typing import Union, Tuple, Literal


def read_heart_rate_json(filepath: Union[str, bytes], ibi_timestamp_method: Literal['forward', 'backward', 'average'] = 'forward') -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Reads a heart rate JSON file and returns two pandas DataFrames:
    1. Main DataFrame with columns:
        - hr
        - hrInterBeatInterval
        - status
        - effective_time_frame
        - datetime (converted from unix_timestamp_in_ms, in US/Pacific timezone)
    2. IBI DataFrame with columns:
        - datetime (calculated based on selected method)
        - ibi (value from ibi_list)
        - ibi_status (value from ibi_status_list)
    
    IBI timestamp calculation methods:
    forward: First IBI gets sample timestamp, subsequent IBIs get cumulative timestamps
    backward: Last IBI gets next sample timestamp minus its duration, working backwards
    average: Average of forward and backward
    
    Ignores samples with empty ibi_list.
    """
    with open(filepath, 'r') as f:
        data = json.load(f)
    samples = data.get('samples', [])
    
    # Extract main fields
    records = [
        {
            'hr': sample.get('hr'),
            'hrInterBeatInterval': sample.get('hrInterBeatInterval'),
            'status': sample.get('status'),
            'unix_timestamp_in_ms': sample.get('unix_timestamp_in_ms'),
            'effective_time_frame': sample.get('effective_time_frame')
        }
        for sample in samples
    ]
    df = pd.DataFrame(records)
    
    # Convert unix_timestamp_in_ms to datetime if present
    if 'unix_timestamp_in_ms' in df.columns:
        df['datetime'] = pd.to_datetime(pd.to_numeric(df['unix_timestamp_in_ms'], errors='coerce'), unit='ms', errors='coerce', utc=True)
        df['datetime'] = df['datetime'].dt.tz_convert('US/Pacific')
        df = df.drop(columns=['unix_timestamp_in_ms'])

    # Extract IBI values and statuses with timestamp calculation
    ibi_records = []
    
    for i, sample in enumerate(samples):
        ibi_list = sample.get('ibi_list', [])
        ibi_status_list = sample.get('ibi_status_list', [])
        
        # Only process if both lists are non-empty and of the same length
        if ibi_list and ibi_status_list and len(ibi_list) == len(ibi_status_list):
            # Get the timestamp for this sample
            unix_ts = sample.get('unix_timestamp_in_ms')
            if unix_ts is not None:
                sample_dt = pd.to_datetime(pd.to_numeric(unix_ts, errors='coerce'), unit='ms', errors='coerce', utc=True)
                sample_dt = sample_dt.tz_convert('US/Pacific')
                
                # Get next sample timestamp for backward method
                next_sample_dt = None
                if i + 1 < len(samples):
                    next_unix_ts = samples[i + 1].get('unix_timestamp_in_ms')
                    if next_unix_ts is not None:
                        next_sample_dt = pd.to_datetime(pd.to_numeric(next_unix_ts, errors='coerce'), unit='ms', errors='coerce', utc=True)
                        next_sample_dt = next_sample_dt.tz_convert('US/Pacific')
                
                # Calculate timestamps based on selected method
                if ibi_timestamp_method == 'forward':
                    # Forward calculation
                    current_time = sample_dt
                    for ibi, ibi_status in zip(ibi_list, ibi_status_list):
                        ibi_records.append({
                            'datetime': current_time,
                            'ibi': ibi,
                            'ibi_status': ibi_status
                        })
                        # Add IBI duration to current time (convert ms to timedelta)
                        current_time = current_time + pd.Timedelta(milliseconds=ibi)
                
                elif ibi_timestamp_method == 'backward':
                    # Backward calculation
                    if next_sample_dt is not None:
                        # Start from the end and work backwards
                        current_time = next_sample_dt
                        for ibi, ibi_status in zip(reversed(ibi_list), reversed(ibi_status_list)):
                            # Subtract IBI duration from current time
                            current_time = current_time - pd.Timedelta(milliseconds=ibi)
                            ibi_records.append({
                                'datetime': current_time,
                                'ibi': ibi,
                                'ibi_status': ibi_status
                            })
                        # Reverse back to original order
                        ibi_records[-len(ibi_list):] = reversed(ibi_records[-len(ibi_list):])
                    else:
                        # Fallback to forward if no next sample
                        current_time = sample_dt
                        for ibi, ibi_status in zip(ibi_list, ibi_status_list):
                            ibi_records.append({
                                'datetime': current_time,
                                'ibi': ibi,
                                'ibi_status': ibi_status
                            })
                            current_time = current_time + pd.Timedelta(milliseconds=ibi)
                
                elif ibi_timestamp_method == 'average':
                    # Average of forward and backward
                    # Calculate forward timestamps
                    timestamps_forward = []
                    current_time_fwd = sample_dt
                    for ibi in ibi_list:
                        timestamps_forward.append(current_time_fwd)
                        current_time_fwd = current_time_fwd + pd.Timedelta(milliseconds=ibi)
                    
                    # Calculate backward timestamps
                    timestamps_backward = []
                    if next_sample_dt is not None:
                        current_time_bwd = next_sample_dt
                        for ibi in reversed(ibi_list):
                            current_time_bwd = current_time_bwd - pd.Timedelta(milliseconds=ibi)
                            timestamps_backward.append(current_time_bwd)
                        timestamps_backward = list(reversed(timestamps_backward))
                    else:
                        # Fallback to forward timestamps
                        timestamps_backward = timestamps_forward
                    
                    # Average the timestamps
                    for (ibi, ibi_status), ts_fwd, ts_bwd in zip(zip(ibi_list, ibi_status_list), timestamps_forward, timestamps_backward):
                        avg_timestamp = ts_fwd + (ts_bwd - ts_fwd) / 2
                        ibi_records.append({
                            'datetime': avg_timestamp,
                            'ibi': ibi,
                            'ibi_status': ibi_status
                        })
    
    ibi_df = pd.DataFrame(ibi_records)
    return df, ibi_df 