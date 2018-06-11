import time
import signal

from urllib.parse import parse_qs, urlencode, urlparse
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from threading import Lock
from functools import partial
from random import random

import requests

STOP = (None, None)

ENTITIES = {
    "annotation",
    "area",
    "artist",
    "cdstub",
    "editor",
    "event",
    "instrument",
    "label",
    "place",
    "recording",
    "release",
    "release-group",
    "series",
    "tag",
    "url",
    "work",
}

class LoadTester:

    def __init__(self, log_path, prefix="http://195.201.149.141:8983/solr", max_workers=9, endpoint="advanced", percent=33):
        self.log_path = log_path
        self.max_workers = max_workers
        self.prefix = prefix
        self.endpoint = endpoint

        self.queue = Queue(maxsize=256)
        self.responses = defaultdict(lambda: [0, 0, 0])
        self.count = 0
        self.lock = Lock()
        self.stop = False
        self.ratio = percent/100
        self.executor = None

        print("Stating benchmark against:")
        print("URL: {}".format(self.prefix))
        print("File: {}".format(self.log_path))

    def parse_line(self, line):
        request = line.split(" ")[7]
        if "?query" in request:
            parsed_req = urlparse(request)
            entity = list(filter(bool, parsed_req.path.split("/")))[-1]
            if entity not in ENTITIES:
                return None
            query = parse_qs(parsed_req.query)
            query['q'] = query.pop('query', '*:*')
            query['rows'] = query.pop('limit', ['25'])
            fmt = query.pop('fmt', ['json']).pop()
            if fmt == 'json':
                query['wt'] = ['mbjson']
            else:
                query['wt'] = ['mbxml']
            for key in list(query.keys()):
                if key not in ('wt', 'rows', 'q'):
                    query.pop(key, None)
            return (entity, "{}/{}/{}?{}".format(self.prefix, entity, self.endpoint, urlencode(query, doseq=True)))
        return None

    def read(self):
        file = open(self.log_path)
        file.seek(0, 2)
        while True:
            if self.stop:
                break
            line = file.readline()
            if not line:
                time.sleep(0.1)
                continue
            yield line

    def test(self):
        self.executor = executor = ThreadPoolExecutor(max_workers=self.max_workers)
        executor.submit(self.producer)
        for _ in range(self.max_workers-1):
            executor.submit(self.consumer)

    def producer(self):
        for line in self.read():
            req = self.parse_line(line)
            to_queue = random() < self.ratio
            if req and to_queue:
                self.queue.put(req)
        self.queue.put(STOP)

    def consumer(self):
        while True:
            entity, req = self.queue.get()
            if not entity:
                self.queue.put(STOP)
                break
            res = requests.get(req)
            with self.lock:
                self.responses[entity][res.ok] += 1
                self.count += 1
                if self.count % 100 == 0:
                    print("Completed {} requests".format(self.count))

    def print_stats(self):
        if self.count:
            print('\n')
            print("="*50)
            print("Entity\t\t\tOk\t\tNot Ok")
            print("="*50)
            for entity, values in self.responses.items():
                print("{}\t\t{}\t\t{}".format(entity, values[0], values[1]))
                print("-"*50)
            print("="*50)
            print("Total requests: {}".format(t.count))
            print('\n')
        else:
            print("No requests made.")

def signal_handler(tester, *args):
    tester.stop = True
    print("Stopping script. Please wait...")
    tester.executor.shutdown()
    tester.print_stats()
    print("Done")

if __name__ == "__main__":
    t = LoadTester("access_logs")
    signal.signal(signal.SIGINT, partial(signal_handler, t))
    t.test()