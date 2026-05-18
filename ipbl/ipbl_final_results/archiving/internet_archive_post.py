import os
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import json

from dotenv import load_dotenv

load_dotenv()

file_lock = threading.Lock()
OUTPUT_FILE = "results.jsonl"


ARCHIVE_URL = "https://web.archive.org/save"
STATUS_URL = "https://web.archive.org/save/status/{}"


thread_local = threading.local()


def get_session():

    if not hasattr(thread_local, "session"):
        session = requests.Session()

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100,
            max_retries=3,
        )

        session.mount("https://", adapter)
        thread_local.session = session

    return thread_local.session

def append_result(result: dict):
    line = json.dumps(result, ensure_ascii=False)

    with file_lock:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()


class InternetArchiveClient:

    def __init__(self, cred_idx=1):
        access_key = os.getenv(f"ACCESS_KEY_{cred_idx}")
        secret_key = os.getenv(f"SECRET_KEY_{cred_idx}")

        self.headers = {
            "Accept": "application/json",
            "Authorization": f"LOW {access_key}:{secret_key}",
        }

    def start_job(self, target_url):
        session = get_session()

        response = session.post(
            ARCHIVE_URL,
            headers=self.headers,
            data={
                "url": target_url,
                "capture_outlinks": 0,
                "skip_first_archive": 1,
            },
            timeout=(10, 60),
        )

        response.raise_for_status()

        return response.json()["job_id"]

    def check_status(self, job_id):
        session = get_session()

        response = session.get(
            STATUS_URL.format(job_id),
            headers=self.headers,
            timeout=(10, 60),
        )

        response.raise_for_status()

        return response.json()

    def archive_target(
        self,
        target_url,
        max_checks=20,
        min_sleep=20,
        max_sleep=40,
    ):

        try:
            time.sleep(random.randint(min_sleep, max_sleep))
            job_id = self.start_job(target_url)

        except Exception as e:
            return {
                "url": target_url,
                "status": "error",
                "message": f"start_job failed: {e}",
            }

        for _ in range(max_checks):

            time.sleep(random.randint(min_sleep, max_sleep))

            try:
                status_data = self.check_status(job_id)

            except requests.RequestException as e:
                continue

            status = status_data.get("status")

            if status == "success":

                archive_url = (
                    f'https://web.archive.org/web/'
                    f'{status_data["timestamp"]}/'
                    f'{status_data["original_url"]}'
                )
                append_result({
                    "url": target_url,
                    "status": "success",
                    "archive_url": archive_url,
                })
                return {
                    "url": target_url,
                    "status": "success",
                    "archive_url": archive_url,
                }

            if status == "error":
                append_result({
                    "url": target_url,
                    "status": "error",
                    "message": status_data.get("message"),
                })
                return {
                    "url": target_url,
                    "status": "error",
                    "message": status_data.get("message"),
                }
        
        append_result({
            "url": target_url,
            "status": "timeout",
            "message": "Max status checks exceeded",
        })
        return {
            "url": target_url,
            "status": "timeout",
            "message": "Max status checks exceeded",
        }


def archive_many(urls, workers=10):

    futures = []

    with ThreadPoolExecutor(max_workers=workers) as executor:

        for url in urls:

            client = InternetArchiveClient()

            futures.append(
                executor.submit(client.archive_target, url)
            )

        for future in as_completed(futures):
            yield future.result()


if __name__ == "__main__":

    with open('urls_to_archive.json', 'r', encoding='utf-8') as jfile:
        urls = json.load(jfile)

    for result in archive_many(urls, workers=5):
        print(result)