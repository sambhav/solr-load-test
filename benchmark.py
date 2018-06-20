import os
import sys
import time
import signal

from urllib.parse import parse_qs, urlencode, urlparse
from queue import Queue
from threading import Thread
from collections import defaultdict
from threading import Lock
from functools import partial
from random import random

import requests

STOP = (None, None)
HEADER_FMT_STRING = "|{:^20}|{:^10}|{:^10}|{:^10}|"
FMT_STRING = "|{:^20}|{:^10}|{:^10}|{:^10.2f}|"
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

    def __init__(self, log_path, prefix="http://195.201.47.148/solr", max_workers=30, endpoint="advanced", ratio=1):
        self.log_path = log_path
        self.max_workers = max_workers
        self.prefix = prefix
        self.endpoint = endpoint

        self.queue = Queue()
        self.responses = defaultdict(lambda: [0, 0, 0])
        self.count = 0
        self.total_count = 0
        self.start_time = 0
        self.lock = Lock()
        self.stop = False
        self.ratio = ratio
        self.executor = None
        self.threads = []

        print("Stating benchmark against:")
        print("URL: {}".format(self.prefix))
        print("File: {}".format(self.log_path))

    def parse_line(self, line):
        ls = line.split('"')
        request = ls[1][4:-9]
        status = int(ls[2].split()[0])
        if "?query" in request and status < 400:
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
        # Tail -f code with log rotation adapted from
        # https://stackoverflow.com/a/25632664/5458985
        current = open(self.log_path)
        current.seek(0, 2)
        curino = os.fstat(current.fileno()).st_ino
        self.start_time = time.time()
        while True:
            if self.stop:
                break
            while True:
                if self.stop:
                    break
                line = current.readline()
                if not line:
                    break
                yield line
            try:
                if os.stat(self.log_path).st_ino != curino:
                    print("Log rotation detected. Changing current inode: {}".format(curino))
                    new = open(self.log_path)
                    current.close()
                    current = new
                    curino = os.fstat(current.fileno()).st_ino
                    print("New inode: {}".format(curino))
                    continue
            except IOError:
                pass
            time.sleep(0.1)

    def test(self):
        for _ in range(self.max_workers-1):
            consumer = Thread(target=self.consumer)
            self.threads.append(consumer)
            consumer.start()
        producer = Thread(target=self.producer)
        self.threads.append(producer)
        producer.start()

    def producer(self):
        print("Watching logs")
        for line in self.read():
            try:
                req = self.parse_line(line)
            except:
                req = None
            if req:
                self.total_count += 1
                to_queue = (self.total_count % self.ratio) == 0
                if to_queue:
                    self.queue.put(req)
        self.queue.put(STOP)

    def consumer(self):
        while True:
            entity, req = self.queue.get()
            if self.stop or not entity:
                self.queue.put(STOP)
                break
            try:
                res = requests.get(req)
            except:
                status = False
            else:
                status = res.ok
            with self.lock:
                self.responses[entity][status] += 1
                self.count += 1
                ccount = self.count
            if ccount % 5000 == 0:
                self.print_stats()
            elif ccount % 1000 == 0:
                print("{} requests made.".format(ccount))

    def print_stats(self):
        if self.count:
            total_time = time.time() - self.start_time
            print('\n')
            print("="*55)
            print(HEADER_FMT_STRING.format("ENTITY", "OK", "NOT OK", "PERCENT"))
            print("="*55)
            total_hits = 0
            for entity, values in self.responses.items():
                total_hits += values[1]
                print(FMT_STRING.format(entity, values[1], values[0], values[1]/(values[0]+values[1])*100))
                print("-"*55)
            print("="*55)
            print("Total requests: {}".format(t.count))
            print("Original total requests: {}".format(t.total_count))
            print("Keepup percent: {:.2f}".format((t.count/t.total_count)*100))
            print("Total time: {:.2f}".format(total_time))
            print("Hits/sec: {:.2f}".format(total_hits/total_time))
            print('\n')
        else:
            print("No requests made.")

def signal_handler(tester, *args):
    tester.stop = True
    print("Stopping script. Please wait...")
    for t in tester.threads:
        t.join()
    tester.print_stats()
    print("Done")

if __name__ == "__main__":
    t = LoadTester("access_logs")
    signal.signal(signal.SIGINT, partial(signal_handler, t))
    t.test()
