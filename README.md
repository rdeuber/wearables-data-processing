# Wearables Data Processing

This repository provides tools and scripts for processing and analyzing data from wearable devices, such as heart rate and green PPG (photoplethysmography) signals.

## Features
- **Data Readers:**
  - Read and process heart rate JSON files, extracting heart rate, IBI (inter-beat interval) values, and timestamps.
  - Read and process green PPG JSON files, extracting raw PPG values and timestamps.
- **Flexible IBI Timestamp Calculation:**
  - Choose between forward, backward, or average methods for assigning timestamps to IBI values.
- **Jupyter Notebook Support:**
  - Example notebooks for data analysis and visualization.
- **CSV Export:**
  - Convert processed data to CSV for further analysis or sharing.

## Directory Structure
- `data_readers/` — Python modules for reading and processing raw data files.
- `.ipynb_checkpoints/` and `__pycache__/` — Ignored by git.

## Getting Started
1. Install dependencies (e.g., pandas, matplotlib, pytz).
2. Use the provided reader functions to load and process your data files.
3. Analyze or visualize the data in Jupyter notebooks or Python scripts.

## Example Usage
```python
from data_readers.heart_rate_reader import read_heart_rate_json
from data_readers.green_ppg_reader import read_green_ppg_json

df_hr, df_ibi = read_heart_rate_json('heart_rate_data.json', ibi_timestamp_method='forward')
df_ppg = read_green_ppg_json('green_ppg_data.json')
```

## License
MIT License 