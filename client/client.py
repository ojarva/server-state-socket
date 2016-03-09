#!/usr/bin/env python

from local_settings import PORT, SERVER_IP
import json
import multiprocessing
import redis
import socket
import sys
import time
import uuid


class StateClient(object):
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.redis = redis.StrictRedis()

    def connect_and_ping(self):
        s = socket.create_connection((self.ip, self.port), 5)
        while True:
            ping_data = str(uuid.uuid4())
            s.send("ping %s" % ping_data)
            data = s.recv(1024)
            if data.startswith("pong"):
                if data[5:] == ping_data:
                    self.send_status("running")
            time.sleep(1)
        s.close()

    def send_status(self, status):
        self.redis.publish("home:broadcast:generic", json.dumps({"key": "server_power", "content": {"status": status}}))

    def run(self):
        while True:
            try:
                self.connect_and_ping()
            except socket.error:
                self.send_status("down")
            time.sleep(1)


def main(args):
    if len(args) == 2:
        ip, port = args[0], args[1]
    else:
        ip, port = SERVER_IP, PORT
    state_client = StateClient(SERVER_IP, PORT)
    state_client.run()

if __name__ == '__main__':
    main(sys.argv[1:])
