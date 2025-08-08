# Wearables Data Processing

This project processes wearable device data, specifically focusing on green PPG (Photoplethysmography) data from smartwatches.

## Environment Setup

### Creating the Environment

```bash
# Create a new conda environment
conda create -n mindprocessing python=3.9 -y

# Activate the environment
conda activate mindprocessing

# Install required packages
pip install -r requirements.txt
```

### Required Packages

The project uses the following main packages:
- `pandas` - Data manipulation and analysis
- `numpy` - Numerical computing
- `matplotlib` - Plotting and visualization
- `seaborn` - Statistical data visualization
- `plotly` - Interactive plotting
- `jupyter` - Jupyter notebooks for analysis

## Data Processing

### Green PPG Data Concatenation

The main script `green_ppg_concat.py` performs the following operations:

1. **Continuity Check**: Analyzes time gaps between consecutive green PPG data files
2. **Data Concatenation**: Combines all files for a specific day into a single dataset
3. **Data Export**: Saves the concatenated data in both PKL and CSV formats

#### Usage

```bash
# Activate the environment
conda activate mindprocessing

# Run the concatenation script
python green_ppg_concat.py
```

#### Output

The script generates:
- `data/concatenated_green_ppg_{date}.pkl` - Pickle format for efficient loading
- `data/concatenated_green_ppg_{date}.csv` - CSV format for compatibility

#### Features

- **Continuity Analysis**: Identifies gaps exceeding 40ms threshold between files
- **Chronological Ordering**: Ensures data is properly sorted by timestamp
- **Comprehensive Reporting**: Provides detailed statistics and progress information
- **Error Handling**: Gracefully handles missing or corrupted files

## Data Structure

### Green PPG Data Files

Files are named with the pattern: `green_ppg_data_06.08.25_*.json`

Each file contains:
- `ppg_green_value`: Raw PPG sensor readings
- `unix_timestamp_in_ms`: Timestamp in milliseconds
- `datetime`: Converted datetime in US/Pacific timezone

### Concatenated Output

The final dataset includes:
- All samples from all files for the specified date
- Chronologically ordered data with proper timestamps
- Both PKL and CSV export formats for flexibility

## Project Structure

```
wearables-data-processing/
├── data_readers/
│   ├── green_ppg_reader.py      # Green PPG data reading utilities
│   └── heart_rate_reader.py     # Heart rate data reading utilities
├── data/                        # Raw data files
├── green_ppg_concat.py          # Main concatenation script
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## Analysis Results

The continuity analysis provides:
- Total number of files processed
- Number of gaps exceeding the 40ms threshold
- File transition analysis
- Detailed gap reporting with timestamps

This helps identify potential data collection issues or device interruptions during recording sessions. 