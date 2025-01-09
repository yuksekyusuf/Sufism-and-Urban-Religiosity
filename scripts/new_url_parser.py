from pathlib import Path
import logging
from typing import Dict, Optional, Tuple
from bs4 import BeautifulSoup
import re
from dataclasses import dataclass
from datetime import datetime
import pandas as pd


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sicil_parser.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def parse_court_title(titles):
    try:
        logger.info(f"Processing titles: {titles}")
        court_title = ""
        sicil_number = None

        # Case 1: Handle special institutions without "Mahkemesi"
        if "Müfettişliği" in titles:
            mahkeme_index = titles.index("Müfettişliği")
            court_title = " ".join(titles[:mahkeme_index + 1])
            
            # Find sicil number
            for i, word in enumerate(titles[mahkeme_index + 1:]):
                if word.isdigit():
                    sicil_number = word
                    break
            return court_title, sicil_number
        
        # Case 2: Handle parenthetical additions and regular Mahkemesi
        mahkeme_index = -1
        for i, word in enumerate(titles):
            if word == "Mahkemesi":
                mahkeme_index = i
                break
        
        if mahkeme_index != -1:
            # Get base court title
            court_title = " ".join(titles[:mahkeme_index + 1])
            
            # Check for parenthetical addition
            remaining_words = titles[mahkeme_index + 1:]
            if remaining_words and remaining_words[0].startswith("("):
                parenthetical = []
                for word in remaining_words:
                    if word.endswith(")"):
                        parenthetical.append(word)
                        break
                    parenthetical.append(word)
                if parenthetical:
                    court_title = f"{court_title} {' '.join(parenthetical)}"
            
            # Find sicil number
            for word in remaining_words:
                if word.isdigit():
                    sicil_number = word
                    break

        # Case 3: Handle variations of Numaralı
        if sicil_number is None:
            for i, word in enumerate(titles):
                if word.isdigit():
                    # Check if next word is a variation of numaralı/numaları
                    if i + 1 < len(titles) and titles[i + 1].lower().startswith("numar"):
                        sicil_number = word
                        break

        return court_title, sicil_number

    except Exception as e:
        logger.error(f"Error parsing title '{titles}': {str(e)}")
        return None, None
    

def sicil_parser(number):
    try:
        record = {}
        data_folder = Path("data/urls")
        file_to_open = data_folder / f"{number}.html"
        
        logger.info(f"Processing record {number}")
        
        with open(file_to_open) as f:
            soup = BeautifulSoup(f, "lxml")
        
        header, text = soup.select(".bas8")
        header = header.get_text(separator="\f").split("\f")
        text = text.get_text(separator="\f").split("\f")
        
        title_register, volume_page, case_number, original_page_n, acknow = header
        summary, *original_text = text

        # Split by date part first
        title_parts = title_register.split('(H.')
        main_title = title_parts[0].strip()
        sicil_date = f"H.{title_parts[1]}" if len(title_parts) > 1 else ""
        
        # Split the main title by "Numaralı" or "numaları"
        title_elements = main_title.split('Numaralı') if 'Numaralı' in main_title else main_title.split('numaları')
        
        if len(title_elements) > 1:
            # Get everything before the number
            full_court_title = title_elements[0].strip()
            # Get the last word before "Numaralı" as the number
            words = full_court_title.split()
            sicil_number = words[-1] if words[-1].isdigit() else None
            court_title = ' '.join(words[:-1]) if sicil_number else full_court_title
        else:
            court_title = main_title
            sicil_number = None
        
        # Get Hijri and Miladi dates
        date_parts = sicil_date.split('/')
        date_hijri = date_parts[0].strip() if len(date_parts) > 0 else ""
        date_miladi = date_parts[-1].strip() if len(date_parts) > 1 else ""
        
        record["court_title"] = court_title
        record["sicil_number"] = sicil_number
        record["sicil_date"] = sicil_date
        record["date_hijri"] = date_hijri
        record["date_miladi"] = date_miladi
        record["case_text"] = original_text
        record["case_summary"] = summary
        record["case_number"] = case_number
        record["id"] = number
        
        return record
        
    except Exception as e:
        logger.error(f"Error processing record {number}: {str(e)}")
        raise


def find_unique_court_titles(start_num: int, end_num: int) -> pd.DataFrame:
    """
    Process multiple records and collect unique court titles with their frequencies.
    
    Args:
        start_num: Starting record number
        end_num: Ending record number
        
    Returns:
        DataFrame with court titles and their counts
    """
    court_titles = []
    failed_records = []
    
    for num in range(start_num, end_num + 1):
        try:
            record = sicil_parser(str(num))
            if record["court_title"] is not None:
                court_titles.append(record["court_title"])
            else:
                failed_records.append(num)
        except Exception as e:
            logger.error(f"Error processing record {num}: {e}")
            failed_records.append(num)
    
    # Create DataFrame with court titles and their counts
    title_counts = pd.DataFrame(court_titles, columns=['court_title'])
    title_counts = title_counts['court_title'].value_counts().reset_index()
    title_counts.columns = ['court_title', 'count']
    
    # Log statistics
    logger.info(f"Total records processed: {end_num - start_num + 1}")
    logger.info(f"Unique court titles found: {len(title_counts)}")
    logger.info(f"Failed records: {len(failed_records)}")
    
    if failed_records:
        logger.warning(f"Failed record numbers: {failed_records}")
    
    return title_counts

def find_unique_court_titles(start_num: int, end_num: int) -> pd.DataFrame:
    """
    Process multiple records and collect unique court titles with their frequencies.
    
    Args:
        start_num: Starting record number
        end_num: Ending record number
        
    Returns:
        DataFrame with court titles and their counts
    """
    court_titles = []
    failed_records = []
    
    for num in range(start_num, end_num + 1):
        try:
            record = sicil_parser(str(num))
            if record["court_title"] is not None and record["sicil_number"] is not None:
                full_title = f"{record['court_title']} {record['sicil_number']} Numaralı Sicil"
                court_titles.append(full_title)
            else:
                failed_records.append(num)
        except Exception as e:
            logger.error(f"Error processing record {num}: {e}")
            failed_records.append(num)
    
    # Create DataFrame with court titles and their counts
    title_counts = pd.DataFrame(court_titles, columns=['court_title'])
    title_counts = title_counts['court_title'].value_counts().reset_index()
    title_counts.columns = ['court_title', 'count']
    
    # Sort by court title
    title_counts = title_counts.sort_values('court_title')
    
    # Log statistics
    logger.info(f"Total records processed: {end_num - start_num + 1}")
    logger.info(f"Unique court titles found: {len(title_counts)}")
    logger.info(f"Failed records: {len(failed_records)}")
    
    if failed_records:
        logger.warning(f"Failed record numbers: {failed_records}")
    
    return title_counts

def process_all_cases(start_num=1, end_num=54671, batch_size=1000):
    """
    Process all cases in batches and save to CSV, with periodic saves
    """
    all_cases = []
    failed_cases = []
    current_batch = []
    
    logger.info(f"Starting to process cases from {start_num} to {end_num}")
    
    for i in range(start_num, end_num + 1):
        try:
            case = sicil_parser(str(i))
            current_batch.append(case)
            
            # Save batch when it reaches batch_size
            if len(current_batch) >= batch_size:
                df_batch = pd.DataFrame(current_batch)
                
                # If this is the first batch, write with headers
                if not all_cases:
                    df_batch.to_csv("sicil_records.csv", index=False)
                else:
                    # Append without headers
                    df_batch.to_csv("sicil_records.csv", mode='a', header=False, index=False)
                
                # Add to master list and clear batch
                all_cases.extend(current_batch)
                current_batch = []
                
                logger.info(f"Processed and saved batch up to record {i}")
            
        except Exception as e:
            logger.error(f"Failed to process record {i}: {str(e)}")
            failed_cases.append(i)
            continue
        
        # Print progress every 1000 records
        if i % 1000 == 0:
            logger.info(f"Processed {i} records. Failed records so far: {len(failed_cases)}")
    
    # Save any remaining records in the last batch
    if current_batch:
        df_batch = pd.DataFrame(current_batch)
        if not all_cases:
            df_batch.to_csv("sicil_records.csv", index=False)
        else:
            df_batch.to_csv("sicil_records.csv", mode='a', header=False, index=False)
        all_cases.extend(current_batch)
    
    # Save failed cases to a separate file
    if failed_cases:
        with open("failed_records.txt", "w") as f:
            for case_id in failed_cases:
                f.write(f"{case_id}\n")
        logger.info(f"Failed records saved to failed_records.txt")
    
    logger.info(f"Processing complete. Total records: {len(all_cases)}, Failed: {len(failed_cases)}")
    return len(all_cases), len(failed_cases)

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('processing.log'),
            logging.StreamHandler()
        ]
    )

    # Process all cases
    total_processed, total_failed = process_all_cases()
    
    print(f"\nProcessing complete:")
    print(f"Total records processed: {total_processed}")
    print(f"Failed records: {total_failed}")
