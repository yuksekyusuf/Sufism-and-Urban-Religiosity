from pathlib import Path
import yaml
from openai import OpenAI

def load_config():
    """Load configuration from YAML file."""
    # Get the project root directory (two levels up from this file)
    project_root = Path(__file__).parent.parent.resolve()
    config_path = project_root / 'config' / 'config.yaml'
    
    try:
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found at {config_path}. Please ensure config.yaml exists in the config directory.")

def setup_openai_client():
    """Setup OpenAI client with configuration."""
    config = load_config()
    return OpenAI(api_key=config['openai']['api_key'])

client = setup_openai_client()
# print("List:", client.models.list())