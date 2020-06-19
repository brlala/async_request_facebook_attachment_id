import asyncio
import json
import os
import re
from pprint import PrettyPrinter
from urllib.parse import unquote

import aiofiles
import aiohttp
from bson import Regex
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from cloud_manager import upload_file
from facebook_manager import upload_facebook_attachment

pp = PrettyPrinter(indent=4)


def init_config() -> dict:
    with open('config.json') as f:
        config = json.load(f)
    return config


class MongoDatabase:
    def init_database(self, config: dict) -> Database:
        client = MongoClient(config['mongo_url'])
        database = client[config["abbreviation"]]
        return database


def get_attachment_urls(collection: Collection):
    """
    Return a list of urls from collections
    """
    pipeline = [{"$match": {"is_active": True,
                            "flow.data.url": {"$exists": True},
                            "flow.data.attachment_id": {"$exists": True}}},
                {"$unwind": {"path": "$flow"}},
                {"$match": {"flow.data.url": {"$exists": True, "$ne": ""}}},
                {"$group": {"_id": None,
                            "urls": {"$addToSet": u"$flow.data.url"}}}]

    cursor = collection.aggregate(pipeline)

    try:
        urls = cursor.next()['urls']
    except:
        urls = []
    return urls


async def check_url_valid(url: str, *, session) -> None:
    """
    Check if URL is valid and exist
    """
    async with session.head(url) as resp:
        return {"status": resp.status,
                "url": url}


def insert_attachment_into_database(collection: Collection, entry: dict) -> None:
    doc = collection.find_one({"url": entry['url']})
    if doc:
        doc['facebook']['attachment_id'] = entry['attachment_id']
    else:
        doc = {"url": entry['url'],
               "facebook": {"attachment_id": entry['attachment_id']}}
    res = collection.replace_one({"url": entry['url']}, doc, upsert=True)
    if res.modified_count > 1:
        print(f"Completed: {entry['url']}")


def remove_attachment_id_from_flow(flow_collection: Collection, url: str) -> None:
    """
    Remove attachment_id from old collection and update url with new bucket
    """
    filename = os.path.split(url)[-1]
    query = {"flow.data.url": Regex(f".*{re.escape(filename)}$", "i")}
    docs = flow_collection.find(query)
    for doc in docs:
        flows = doc['flow']
        for flow in flows:
            if flow['type'] in ['image', 'video'] and flow['data']['url'].endswith(filename):
                flow['data']['url'] = url
                flow['data'].pop('attachment_id', None)
        flow_collection.replace_one({"_id": doc['_id']}, doc)


async def bound_download(url, *, semaphore, session, config, save_collection, flow_collection):
    """
    Using semaphore to limit the amount of connections, main method
    """
    async with semaphore:
        result = await check_url_valid(url, session=session)
        if result['status'] != 200:
            print(f"FAIL: {result}")
        else:
            save_folder = f'portal'
            file_name = unquote(url.split('/')[-1])
            save_location = f"{save_folder}/{file_name}"
            await download_to_local(result['url'], save_location, session=session)
            url = reupload_to_new_bucket(save_location, config=config)
            attachment_id = await upload_file_to_facebook(url, session=session, config=config)
            insert_attachment_into_database(save_collection, {"url": url, "attachment_id": attachment_id})
            remove_attachment_id_from_flow(flow_collection, url)
            return url, attachment_id


async def download_to_local(url: str, save_location: str, *, session) -> None:
    """
    This downloads the file to a local directory in chunks
    """
    print(f"downloading: {url}")
    async with session.get(url, timeout=0) as resp:
        try:
            # writing files directly to computer
            async for data in resp.content.iter_chunked(1024):
                async with aiofiles.open(save_location, "ba") as f:
                    await f.write(data)
        except Exception as e:
            print(e)


def reupload_to_new_bucket(save_location: str, config: dict) -> str:
    """
    Uploading the files to new bucket
    :return: url
    """
    print(f"uploading: {save_location}")
    url = upload_file(save_location, config=config)
    return url


async def upload_file_to_facebook(save_location: str, *, session, config) -> str:
    """
    Upload files to facebook
    :return: attachment_id
    """
    print(f"getting attachment id: {save_location}")
    extension = os.path.splitext(save_location)[-1]
    if extension in ['.jpg', '.png', '.gif', '.jpeg']:
        attachment_type = 'image'
    elif extension in ['.mp4']:
        attachment_type = 'video'
    else:
        print(f"error for file: {save_location}")
        return
    attachment_id = await upload_facebook_attachment(attachment_type, save_location, config=config, session=session)
    print(f"attachment id: {save_location} - {attachment_id}")
    return attachment_id


async def main():
    """
    Main method to glue up the methods
    """
    config = init_config()
    mongo = MongoDatabase()
    database = mongo.init_database(config)
    flow_collection = database['flow']
    attachment_collection = database['attachment']
    urls = get_attachment_urls(flow_collection)
    semaphore = asyncio.Semaphore(8)
    async with aiohttp.ClientSession() as session:
        futures = (
            bound_download(url, semaphore=semaphore, session=session, config=config,
                           save_collection=attachment_collection, flow_collection=flow_collection)
            for url in urls)
        results = await asyncio.gather(*futures)
    pp.pprint(results)


if __name__ == "__main__":
    import time

    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
