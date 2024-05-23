import openai
import yaml

def load_config(config_path="config/config.yaml"):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

def setup_openai_client():
    config = load_config()
    openai.api_key = config["openai"]["api_key"]
    return openai

client = setup_openai_client()
# print("List:", client.models.list())