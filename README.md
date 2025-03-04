# Attribution Pipeline

This project implements a data pipeline for marketing attribution using the IHC (Impression, Holder, Closer) attribution model. The pipeline extracts data from a SQLite database, transforms it into customer journeys, sends these journeys to the IHC Attribution API, stores the attribution results back in the database, and generates a reporting table with marketing performance metrics.

## Project Structure
```
attribution-pipeline/
├── config.py                  # Configuration parameters
├── main.py                    # Pipeline orchestration
├── db_utils.py                # Database utility functions
├── api_utils.py               # API interaction functions
├── journey_builder.py         # Customer journey construction
├── reporting.py               # Reporting and metrics calculation
├── create_db.py               # Script to initialize the database
├── requirements.txt           # Python dependencies
training data for IHC API
├── extracted_training_data.py # Script to extract training data for
├── ihc_training_data.json     # Sample training data for IHC API
tables
├── challenge_db_create.sql    # SQL for creating required tables
└── challenge.db               # SQLite database
```


## Prerequisites

1. Python 3.7 or higher
2. An IHC API account (free test account available at [ihc-attribution.com](https://ihc-attribution.com/))

## Setup

1. Clone this repository:

```bash
git clone <repository-url>
cd attribution-pipeline
```

2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Create a free IHC test account at [ihc-attribution.com](https://ihc-attribution.com/)

4. Update the `config.py` file with your IHC API credentials:
   - `IHC_API_KEY`: Your API key from IHC
   - `IHC_CONV_TYPE_ID`: Your conversion type ID (e.g., "challenge_conversion")

5. Initialize the database (if not already present):

```bash
python create_db.py
```

6. (Optional) Extract and upload training data to improve attribution accuracy:

```bash
python extracted_training_data.py
```

## Usage

Run the pipeline with optional date range parameters:

```bash
python main.py --start_date 2023-09-01 --end_date 2023-09-30
```

Command-line arguments:
- `--db_path`: Path to the SQLite database file (default: from config.py)
- `--sql_file`: Path to the SQL file for creating tables (default: challenge_db_create.sql)
- `--start_date`: Start date for processing conversions (YYYY-MM-DD)
- `--end_date`: End date for processing conversions (YYYY-MM-DD)
- `--output_path`: Path for the output CSV report (default: from config.py)
- `--chunk_size`: Number of conversions to process in each batch (default: 10)

## Pipeline Steps

1. **Database Initialization**: Creates required tables if they don't exist
2. **Data Extraction**: Retrieves conversion and session data from the database
3. **Journey Building**: Constructs customer journeys by connecting sessions to conversions
4. **API Integration**: Sends journey data to the IHC API in chunks
5. **Result Storage**: Stores attribution results in the database
6. **Reporting**: Creates a channel reporting table with marketing metrics
7. **Export**: Exports the final report to a CSV file

## Output

The pipeline generates a CSV file (`channel_reporting.csv` by default) containing the following columns:
- `channel_name`: Marketing channel name
- `date`: Date of the sessions
- `cost`: Total marketing cost for the channel and date
- `ihc`: Total attribution value for the channel and date
- `ihc_revenue`: Attributed revenue for the channel and date
- `CPO`: Cost Per Order (cost / ihc)
- `ROAS`: Return On Ad Spend (ihc_revenue / cost)

## IHC Attribution Model

The IHC (Impression, Holder, Closer) attribution model is a data-driven approach that assigns credit to marketing touchpoints based on their role in the customer journey:
- **Impression**: A touchpoint that creates awareness
- **Holder**: A touchpoint that maintains engagement
- **Closer**: A touchpoint that leads to conversion

For more information, visit [ihc-attribution.com/ihc-data-driven-attribution-model](https://ihc-attribution.com/ihc-data-driven-attribution-model/).

## API Considerations

- The IHC API has limits on request size and frequency
- The pipeline implements chunking and rate limiting to handle these constraints
- For test accounts, there are stricter limits on the number of conversions that can be processed

## Troubleshooting

- **Database Connection Issues**: Ensure the database file exists and is accessible
- **API Authentication Errors**: Verify your API key and conversion type ID in `config.py`
- **Missing Sessions Warning**: Some conversions may not have associated sessions, which is logged but doesn't halt the pipeline
- **Duplicate Records Error**: The pipeline handles duplicate records by using INSERT OR IGNORE when writing to the database

## Attribution Pipeline Design Report

### Design Overview
The attribution pipeline processes marketing data through a modular, step-by-step approach that:
- Extracts conversion and session data from SQLite
- Constructs customer journeys
- Sends data to the IHC Attribution API
- Stores results back in the database
- Generates marketing performance metrics

The architecture consists of five core components:
- **Main Orchestration** (main.py): Coordinates pipeline execution
- **Database Utilities** (db_utils.py): Handles data retrieval and storage
- **Journey Builder** (journey_builder.py): Constructs and formats customer journeys
- **API Utilities** (api_utils.py): Manages IHC API interactions
- **Reporting** (reporting.py): Creates performance reports with CPO and ROAS metrics

### Key Design Decisions
- **Modular Structure**: Separate components with clear responsibilities for maintainability
- **Efficient Chunking**: Optimizes API usage by respecting request limits
- **Session Assignment Logic**: Ensures each session contributes to only one conversion
- **Comprehensive Error Handling**: Implements robust error management with detailed logging
- **Configurability**: Centralizes parameters in config.py with command-line overrides
- **Incremental Processing**: Supports date-range filtering for efficient updates

### Assumptions
- Sessions precede conversions chronologically for each user
- IHC attribution model is appropriate for the marketing data
- Each session should only contribute to one conversion
- Database structure follows the predefined schema
- All timestamps are in UTC as specified

### Potential Improvements
- **Parallel Processing**: Implement concurrent API requests for better performance
- **Caching**: Add response caching to reduce redundant API calls
- **Advanced Error Recovery**: Allow resuming from failure points
- **Data Quality Checks**: Implement pre-processing validation
- **Visualization**: Add graphical reporting capabilities
- **Testing**: Develop comprehensive test suite for reliability
- **Monitoring**: Implement pipeline performance tracking and alerting

### Conclusion
The pipeline effectively implements the IHC attribution model while prioritizing maintainability and error handling. It successfully processes marketing data and generates valuable performance metrics that can inform marketing strategy. While the current implementation meets the challenge requirements, the suggested improvements would enhance its efficiency and reliability in a production environment.