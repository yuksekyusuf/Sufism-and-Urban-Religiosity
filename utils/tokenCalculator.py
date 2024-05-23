import json
from transformers import GPT2Tokenizer
def calculate_openai_tokens(text):
    # Initialize the tokenizer
    tokenizer = GPT2Tokenizer.from_pretrained('gpt2')
    
    # Tokenize the text
    tokens = tokenizer.encode(text, add_special_tokens=False)
    
    # Calculate the total number of tokens
    total_tokens = len(tokens)

    print(f"Total tokens in this text: {total_tokens}")

