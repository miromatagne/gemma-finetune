from transformers import AutoTokenizer

from dataset_loader import load_jsonl_dataset

model_name = "google/gemma-2-9b"
tokenizer = AutoTokenizer.from_pretrained(model_name)
tokenizer.padding_side = "right"


def tokenize_function(examples):
    return tokenizer(examples['text'], truncation=True, padding=True, max_length=512)


def get_tokenized_dataset():
    dataset = load_jsonl_dataset("data/data_2024-06.jsonl")
    tokenized_dataset = dataset.map(tokenize_function, batched=True)
    tokenized_dataset = tokenized_dataset.remove_columns(dataset.column_names)
    return tokenized_dataset
