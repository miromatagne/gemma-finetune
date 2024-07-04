import json
from datasets import Dataset, load_dataset


def load_jsonl_dataset(path):
    """
    Load a dataset from a JSONL file
    :param path: path to the JSONL file
    :return: dataset
    """
    def generate_dataset():
        f = open(path)
        for line in f.readlines():
            if len(line) > 10:
                #print(line)
                json_line = json.loads(line)
                if "text" in json_line.keys():
                    temp_dict = {"text": json_line["title"] + json_line["text"], "eurovoc_concepts": json_line["eurovoc_concepts"]}
                    yield temp_dict

    print("[Dataset] - Creating dataset from JSONL file")
    dataset = Dataset.from_generator(generate_dataset)
    return dataset
