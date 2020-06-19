import json

def init_config() -> dict:
    with open('config.json') as f:
        config = json.load(f)
    return config

def get_attachment_links(links):
    pass

def upload_link_to_facebook():
    pass

def run():
    config = init_config()
    client = MongoClient("mongodb://host:port/")
    database = client["gelm2uat"]
    collection = database["flow"]
