import sys
import os
from pathlib import Path
import json
import pandas as pd
import csv
import logging
import time
from typing import List, Dict, Optional, Tuple

# Get absolute path to project root
project_root = Path(__file__).parent.parent.resolve()

# Add project root to Python path
sys.path.append(str(project_root))

# Configure paths
config_path = project_root / 'config' / 'config.yaml'
data_path = project_root / 'data' / 'sicil_records.csv'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Import after setting up paths
from utils.openai_client import client
from scripts.prompt import prompt
from scripts.batch_processing_jan import process_batch_results

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def check_batch_status_and_process(
    batch_job_id: str, 
    df,
    case_id_column_name: str,
    max_retries: int = 3, 
    retry_delay: int = 60
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Check batch job status and process results if completed.
    
    Args:
        batch_job_id: The ID of the batch job to check
        df: DataFrame containing the original data
        case_id_column_name: Name of the case ID column in df
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
    
    Returns:
        Tuple[bool, Optional[str], Optional[str]]: Success status, output file path if successful, error message if failed
    """
    output_file = f"processed_results_{batch_job_id}.csv"
    
    try:
        for attempt in range(max_retries):
            try:
                batch_job = client.batches.retrieve(batch_job_id)
                logger.info(f"Batch job status: {batch_job.status} (attempt {attempt + 1}/{max_retries})")
                
                if batch_job.status == 'completed':
                    if not batch_job.output_file_id:
                        return False, None, "Batch job completed but no output_file_id found"

                    # Get results directly from API
                    result = client.files.content(batch_job.output_file_id).content
                    
                    try:
                        # Decode and parse results
                        results = [
                            json.loads(line) 
                            for line in result.decode().strip().split('\n')
                        ]
                        
                        # Process results and save to CSV
                        result_df = process_batch_results(
                            results=results,
                            df=df,
                            case_id_column_name=case_id_column_name,
                            output_file=output_file
                        )
                        
                        logger.info(f"Results processed and saved to {output_file}")
                        return True, output_file, None
                        
                    except json.JSONDecodeError as e:
                        error_msg = f"Error decoding JSON results: {str(e)}"
                        logger.error(error_msg)
                        return False, None, error_msg
                        
                    except Exception as e:
                        error_msg = f"Error processing results: {str(e)}"
                        logger.error(error_msg)
                        return False, None, error_msg
                
                elif batch_job.status == 'failed':
                    error_msg = f"Batch job failed: {batch_job_id}"
                    logger.error(error_msg)
                    return False, None, error_msg
                
                elif batch_job.status in ['pending', 'running']:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    continue
                
                else:
                    return False, None, f"Unknown batch status: {batch_job.status}"
                
            except Exception as e:
                logger.error(f"Error on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                continue
        
        return False, None, f"Failed to complete batch job after {max_retries} attempts"
        
    except Exception as e:
        error_msg = f"Unexpected error checking batch status: {str(e)}"
        logger.error(error_msg)
        return False, None, error_msg

def main():
    if len(sys.argv) != 2:
        print("Usage: python batch_checker.py <batch_job_id>")
        sys.exit(1)

    batch_job_id = sys.argv[1]
    case_id_column = 'case_id'
    
    logger.info(f"Loading data from {data_path}")
    df = pd.read_csv(data_path)
        
    # Create a unique ID by concatenating 'court_title', 'sicil_number', and 'case_number' with a separator
    df['case_id'] = (df['court_title'] + '_' + 
                    df['sicil_number'].astype(str) + '_' + 
                    df['case_number']).str.lower()
    
    success, output_file, error = check_batch_status_and_process(
        batch_job_id=batch_job_id,
        df=df,
        case_id_column_name=case_id_column
    )
    
    if success:
        print(f"Successfully processed batch job. Results saved to: {output_file}")
        sys.exit(0)
    else:
        print(f"Failed to process batch job: {error}")
        sys.exit(1)
        
if __name__ == "__main__":
    main()