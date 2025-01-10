from pathlib import Path
import yaml
from openai import OpenAI

def load_config(config_path="config/config.yaml"):
    project_root = Path(__file__).parent.parent.resolve()
    config_path = project_root / 'config' / 'config.yaml'
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

def setup_openai_client():
    config = load_config()
    client = OpenAI(api_key=config["openai"]["api_key"])
    return client

client = setup_openai_client()



