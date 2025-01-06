import sys
import random
import os
# Get the absolute path to the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
from scripts.batch_processing_jan import process_ner_batch
import pandas as pd
from utils.openai_client import client
import logging


# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Function to create sample DataFrame
def create_sample_df(size=2):
    """
    Create a sample DataFrame for testing batch processing.
    """
    data = []
    for i in range(size):
        court_title = "adalar"
        sicil_number = random.randint(1, 5)
        case_number = i + 1
        data.append({
            'summary': (
                "es-Seyyid Mehmed Efendi b. Abdullah filed a complaint against "
                "Kirkor v. Yanos regarding a property dispute in Istanbul Mahmutpaşa "
                "district on 15 Ramadan 1255. The dispute was resolved through mediation "
                "(muslihûn) with a settlement."
            ),
            'court_title': court_title,
            'sicil_number': sicil_number,
            'case_id': f"{court_title}_{sicil_number}_hüküm no: {case_number}"
        })
    return pd.DataFrame(data)

# Test function for batch processing
def test_batch_processing():
    """
    Test the `process_ner_batch` function to verify end-to-end functionality.
    """
    # Step 1: Create a sample DataFrame
    df = create_sample_df(size=2)

    # Step 2: Set output file path
    output_file = "batch_ner_test.csv"

    try:
        # Step 3: Call the batch processing function
        result_df = process_ner_batch(
            df=df,
            summary_column_name='summary',
            court_title_column_name='court_title',  # Correct column name
            case_id_column_name='case_id',
            output_file='batch_ner_test.csv'
        )

        # Step 4: Verify results
        assert os.path.exists(output_file), "Output file not created"
        assert len(result_df) > 0, "Result DataFrame is empty"

        logger.info("Test passed: Batch processing completed successfully.")
        print("Test passed: Batch processing completed successfully.")

    except AssertionError as ae:
        logger.error(f"Assertion failed: {ae}")
        raise

    except Exception as e:
        logger.error(f"Test failed with an exception: {e}", exc_info=True)
        raise

    # finally:
    #     # Clean up test output file
    #     if os.path.exists(output_file):
    #         os.remove(output_file)
    #         logger.info("Cleaned up test output file.")

if __name__ == "__main__":
    test_batch_processing()