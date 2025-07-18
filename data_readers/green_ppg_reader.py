import json
import pandas as pd
from typing import Union

def read_green_ppg_json(filepath: Union[str, bytes]) -> pd.DataFrame:
    """
    Reads a green PPG JSON file and returns a pandas DataFrame with columns:
    - ppg_green_value
    - datetime (converted from unix_timestamp_in_ms, in US/Pacific timezone)
    """
    with open(filepath, 'r') as f:
        data = json.load(f)
    samples = data.get('green_ppg_samples', [])
    records = [
        {
            'ppg_green_value': sample.get('ppg_green_value'),
            'unix_timestamp_in_ms': sample.get('unix_timestamp_in_ms')
        }
        for sample in samples
    ]
    df = pd.DataFrame(records)
    if 'unix_timestamp_in_ms' in df.columns:
        df['datetime'] = pd.to_datetime(pd.to_numeric(df['unix_timestamp_in_ms'], errors='coerce'), unit='ms', errors='coerce', utc=True)
        df['datetime'] = df['datetime'].dt.tz_convert('US/Pacific')
        df = df.drop(columns=['unix_timestamp_in_ms'])
    return df 