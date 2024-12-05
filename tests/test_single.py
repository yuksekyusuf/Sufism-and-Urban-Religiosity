import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.ner_automation import automate_ner
import logging
import json

def test_single_case():
    """Test NER processing with a single case to debug JSON parsing"""
    # Configure logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    
    test_input = {
        'summary': (
            "es-Seyyid Mehmed Efendi b. Abdullah filed a complaint against "
            "Kirkor v. Yanos regarding a property dispute in Istanbul Mahmutpaşa "
            "district on 15 Ramadan 1255. The dispute was resolved through mediation "
            "(muslihûn) with a settlement."
        ),
        'court': "Test_Court",
        'case_id': "TEST_001"
    }
    
    result = automate_ner(
        test_input['summary'],
        test_input['court'],
        test_input['case_id']
    )
    
    print("Test case result:", json.dumps(result, indent=2))
    return result

if __name__ == "__main__":
    test_result = test_single_case()
    if 'error' not in test_result:
        print("Test successful!")
    else:
        print("Test failed:", test_result['error'])