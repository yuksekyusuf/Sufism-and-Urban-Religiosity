import sys
import os
from pathlib import Path
import json
import pandas as pd
import csv
import logging
import time
from typing import List, Dict, Optional
from utils.openai_client import client
from scripts.prompt import prompt


# Configure logging
logging.basicConfig(
   level=logging.INFO,
   filename='batch_ner_processing.log',
   filemode='a',
   format='%(asctime)s - %(levelname)s - %(message)s', 
   datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def write_to_csv(file_path: str, batch_data: List[Dict], headers: Optional[List[str]] = None, mode: str = 'a') -> None:
    with open(file_path, mode, newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        if mode == 'w' and headers:
            writer.writeheader()
        writer.writerows(batch_data)

# Add log messages in key places
def process_ner_batch(df, summary_column_name, court_title_column_name, case_id_column_name, output_file):
    print("Starting batch processing...")

    logger.info(f"Starting batch processing for {len(df)} records")
    try:
       # Step 1: Create tasks
        tasks = []
        for index, row in df.iterrows():
            task = {
                "custom_id": f"{row[court_title_column_name]}_{row[case_id_column_name]}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4o",
                    "temperature": 0.05,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": f"Extract information in JSON format from this text: {row[summary_column_name]}"}
                    ]
                }
            }
            tasks.append(task)
        logger.info(f"Created {len(tasks)} batch tasks")

       # Step 2: Write batch file
        batch_file_name = "batch_tasks_ner.jsonl"
        with open(batch_file_name, 'w', encoding='utf-8') as file:
            for task in tasks:
                file.write(json.dumps(task, ensure_ascii=False) + '\n')
        logger.info("Wrote batch tasks to JSONL file")

       # Step 3: Submit batch
        batch_file = client.files.create(
            file=open(batch_file_name, "rb"),
            purpose="batch"
        )
        logger.info(f"Created batch file with ID: {batch_file.id}")

        batch_job = client.batches.create(
            input_file_id=batch_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        logger.info(f"Created batch job with ID: {batch_job.id}")

       # Step 4: Check status
        max_retries = 3
        retry_delay = 60
        for attempt in range(max_retries):
            try:
                batch_job = client.batches.retrieve(batch_job.id)
                logger.info(f"Batch job status: {batch_job.status} (attempt {attempt + 1}/{max_retries})")
               
                if batch_job.status == 'completed':
                    if not batch_job.output_file_id:
                        logger.error(f"Batch job completed but no output_file_id returned. Batch job: {batch_job}")
                        raise ValueError("Batch job completed but no output_file_id found.")
                    break
                elif batch_job.status == 'failed':
                    logger.error(f"Batch job failed: {batch_job.id}")
                    raise Exception(f"Batch job failed: {batch_job.id}")
                time.sleep(retry_delay)
            except Exception as e:
                logger.error(f"Error checking batch status: {e}")
                time.sleep(retry_delay)
                continue
        
        result_file_name = "batch_results.jsonl"
        result = client.files.content(batch_job.output_file_id).content

        with open(result_file_name, 'wb') as file:
            file.write(result)
        logger.info(f"Batch results saved to {result_file_name}")

        with open(result_file_name, 'r') as file:
            results = [json.loads(line.strip()) for line in file]


       # Step 5: Process results
        logger.info("Processing batch results")
        result = client.files.content(batch_job.output_file_id).content
        result_df = process_batch_results(
           [json.loads(line) for line in result.decode().strip().split('\n')],
           df,
           case_id_column_name,
           output_file
        )
        logger.info("Batch processing completed successfully")
        return result_df

    except Exception as e:
        logger.error(f"Error in batch processing: {e}", exc_info=True)
        raise
    finally:
        if os.path.exists(batch_file_name):
           os.remove(batch_file_name)
           logger.info("Cleaned up temporary files")

def process_batch_results(results, df, case_id_column_name, output_file):
    """Process batch results and write to CSV."""
    headers = ['person_id', 'name', 'gender', 'religion_ethnicity', 
              'social_status_job', 'role_in_case', 'titles',
              'date', 'calendar', 'place_name', 'place_type', 
              'legal_case_type', 'case_result', 'case_result_type',
              'row_index', 'case_unique_id']
    
    all_rows = []
    
    for result in results:
        try:
            custom_id = result['custom_id']
            response = result.get('response', {}).get('body', {})
            content = response.get('choices', [{}])[0].get('message', {}).get('content')

            if not content:
                logger.error(f"No content found for {custom_id}")
                continue

            
            parts = custom_id.split('_')
            court_title = parts[0]
            case_id = '_'.join(parts[1:])
            matching_rows = df[df[case_id_column_name] == case_id]

            if matching_rows.empty:
                logger.error(f"No matching case_id found: {case_id}")
                continue
                
            row_index = matching_rows.index[0]
            ner_result = json.loads(content)
            
            # Process entities and create rows
            rows_to_write = []
            base_row = {
                'person_id': 'N/A', 'name': 'N/A', 'gender': 'N/A', 
                'religion_ethnicity': 'N/A', 'social_status_job': 'N/A', 
                'role_in_case': 'N/A', 'titles': 'N/A',
                'date': 'N/A', 'calendar': 'N/A',
                'place_name': 'N/A', 'place_type': 'N/A',
                'legal_case_type': 'N/A', 'case_result': 'N/A', 
                'case_result_type': 'N/A',
                'row_index': row_index,
                'case_unique_id': case_id
            }
            
            # Process each entity type
            # Process persons
            for idx, person in enumerate(ner_result.get('persons', []), start=1):
                person_row = base_row.copy()
                person_row.update({
                    'person_id': f"{court_title}_{case_id}_{idx}",
                    'name': person.get('name', 'N/A'),
                    'gender': person.get('gender', 'N/A'),
                    'religion_ethnicity': person.get('religion_ethnicity', 'N/A'),
                    'social_status_job': person.get('social_status_job', 'N/A'),
                    'role_in_case': person.get('role_in_case', 'N/A'),
                    'titles': person.get('titles', 'N/A')
                })
                rows_to_write.append(person_row)
            
            # Process hijri dates
            for date in ner_result.get('hijri_dates', []):
                date_row = base_row.copy()
                date_row.update({
                    'date': date,
                    'calendar': 'Hijri',
                    'row_index': row_index,
                    'case_unique_id': case_id
                })
                rows_to_write.append(date_row)
            
            # Process miladi dates
            for date in ner_result.get('miladi_dates', []):
                date_row = base_row.copy()
                date_row.update({
                    'date': date,
                    'calendar': 'Miladi',
                    'row_index': row_index,
                    'case_unique_id': case_id
                })
                rows_to_write.append(date_row)

            # Process places
            for place in ner_result.get('places', []):
                place_row = base_row.copy()
                place_row.update({
                    'place_name': place.get('place_name', 'N/A'),
                    'place_type': place.get('place_type', 'N/A'),
                    'row_index': row_index,
                    'case_unique_id': case_id
                })
                rows_to_write.append(place_row)

            # Process legal case type    
            legal_type = ner_result.get('legal_case_type', 'N/A')
            legal_type_row = base_row.copy()
            legal_type_row.update({
                'legal_case_type': legal_type,
                'row_index': row_index,
                'case_unique_id': case_id
            })
            rows_to_write.append(legal_type_row)

            # Process case result
            case_result = ner_result.get('case_result', 'N/A')
            case_result_row = base_row.copy()
            case_result_row.update({
                'case_result': case_result,
                'row_index': row_index,
                'case_unique_id': case_id
            })
            rows_to_write.append(case_result_row)

            # Process case result type
            case_result_type = ner_result.get('case_result_type', 'N/A')
            case_result_type_row = base_row.copy()
            case_result_type_row.update({
                'case_result_type': case_result_type,
                'row_index': row_index,
                'case_unique_id': case_id
            })
            rows_to_write.append(case_result_type_row)
            
            # Add other entities processing here (dates, places, etc.)
            all_rows.extend(rows_to_write)
            
        except Exception as e:
            logger.error(f"Error processing result for {custom_id}: {e}")
    
    # Write all processed results to CSV
    if all_rows:
        write_to_csv(output_file, all_rows, headers=headers, mode='w')
    
    return pd.DataFrame(all_rows)