from transformers import Trainer, TrainingArguments, AutoModelForCausalLM, BitsAndBytesConfig, AutoTokenizer
from peft import LoraConfig
from tokenizer import get_tokenized_dataset
import torch
from trl import SFTTrainer
import transformers


def train_model():
    model_name = "google/gemma-2-9b"
    quantization_config = BitsAndBytesConfig(load_in_4bit=True, bnb_4bit_quant_type="nf4", bnb_4bit_compute_dtype=torch.bfloat16)
    model = AutoModelForCausalLM.from_pretrained(model_name, quantization_config=quantization_config, attn_implementation='eager')
    tokenized_dataset = get_tokenized_dataset()
    print(tokenized_dataset[0])

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    text = "Quote: Imagination is more"
    device = "cuda:0"
    inputs = tokenizer(text, return_tensors="pt").to(device)

    outputs = model.generate(**inputs, max_new_tokens=20)
    print(tokenizer.decode(outputs[0], skip_special_tokens=True))

    lora_config = LoraConfig(
        r=8,
        #lora_alpha=32,
        #lora_dropout=0.05,
        target_modules=["gate_proj", "down_proj", "up_proj", "q_proj", "v_proj", "k_proj", "o_proj"],
        task_type="CAUSAL_LM"
    )

    # model.add_adapter(lora_config, adapter_name="adapter_1")

    training_args = TrainingArguments(
        per_device_train_batch_size=1,
        gradient_accumulation_steps=4,
        warmup_steps=2,
        max_steps=10,
        learning_rate=2e-4,
        fp16=True,
        logging_steps=1,
        output_dir="outputs",
        optim="paged_adamw_8bit",
        label_names=["text"],
    )

    trainer = SFTTrainer(
        model=model,
        train_dataset=tokenized_dataset,
        args=training_args,
        peft_config=lora_config,
    )
    trainer.train()
    trainer.save_model("outputs")


if __name__ == "__main__":
    train_model()
    model = AutoModelForCausalLM.from_pretrained("outputs")
    tokenizer = AutoTokenizer.from_pretrained("outputs")
    text = "Quote: Imagination is more"
    device = "cuda:0"
    inputs = tokenizer(text, return_tensors="pt")
    print(tokenizer.decode(model.generate(**inputs, max_new_tokens=20)[0], skip_special_tokens=True))