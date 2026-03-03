import dlt
import requests
from typing import Any


@dlt.resource(name="taxi_data", write_disposition="append")
def taxi_data() -> Any:
    """
    REST API source for NYC taxi data.
    
    API Details:
    - Base URL: https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
    - Data format: Paginated JSON (1,000 records per page)
    - Pagination: Stops when empty page is returned
    """
    base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    page = 1  # API uses 1-based pagination
    
    while True:
        # Fetch data for current page
        url = f"{base_url}?page={page}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            break
        
        # Check if page is empty - stop pagination
        if not data or len(data) == 0:
            print(f"Empty page received at page {page}. Stopping pagination.")
            break
        
        print(f"Fetched page {page} with {len(data)} records")
        
        # Yield each record
        for record in data:
            yield record
        
        page += 1


def main():
    """Run the taxi data pipeline."""
    # Create and run the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data"
    )
    
    # Load taxi data
    load_info = pipeline.run(
        taxi_data(),
        table_name="taxi_records"
    )
    
    print(f"\nPipeline completed successfully!")
    print(load_info)


if __name__ == "__main__":
    main()
