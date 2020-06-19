import asyncio
import json

import aiofiles
import aiohttp
import logging
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pprint import PrettyPrinter
from urllib.parse import urlparse
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
    pipeline = [{"$match": {"is_active": True,
                            "flow.data.url": {"$exists": True}}},
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


async def check_url_valid(url, *, session):
    async with session.head(url) as resp:
        return {"status": resp.status,
                "url": url}


async def bound_download(url, *, semaphore, session):
    async with semaphore:
        result = await check_url_valid(url, session=session)
        if result['status'] != 200:
            print(f"FAIL: {result}")
        else:
            await download_to_local(result['url'], session=session)
            await reupload_to_new_bucket(result['url'], session=session)
            await upload_url_to_facebook(result['url'], session=session)


async def download_to_local(url: str, *, session):
    print(f"downloading: {url} - started")
    save_folder = f'temp'
    file_name = url.split('/')[-1]
    async with session.get(url, timeout=0) as resp:
        try:
            # writing files directly to computer
            async for data in resp.content.iter_chunked(1024):
                async with aiofiles.open(f"{save_folder}/{file_name}", "ba") as f:
                    await f.write(data)
        except Exception as e:
            print(e)
    print(f"downloading: {url} - ended")


async def reupload_to_new_bucket(url: str, *, session):
    print(f"downloading: {url} - started")

    print(f"downloading: {url} - ended")


async def upload_url_to_facebook(url: str, *, session):
    print(f"downloading: {url} - started")

    print(f"downloading: {url} - ended")


async def main():
    config = init_config()
    mongo = MongoDatabase()
    database = mongo.init_database(config)
    collection = database['flow']

    urls = get_attachment_urls(collection)
    semaphore = asyncio.Semaphore(10)
    async with aiohttp.ClientSession() as session:
        futures = (bound_download(url, semaphore=semaphore, session=session) for url in urls)
        results = await asyncio.gather(*futures)


if __name__ == "__main__":
    import time
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")


