#!/usr/bin/env python

import datetime
import json
import logging
import multiprocessing
import redis
import select
import socket
import sys
import time
import uuid


class StateClient(object):
    STATES = {
        "connected": 2,
        "connected_before_reply": 1,
        "disconnected": 0,
        "unknown": -1,
    }

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

        self.logger = logging.getLogger("socket-state-client")
        self.logger.setLevel(logging.INFO)
        format_string = "%(asctime)s - {server_ip}:{server_port} - %(levelname)s - %(message)s".format(server_ip=self.ip, server_port=self.port)
        formatter = logging.Formatter(format_string)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        self.redis = redis.StrictRedis()
        self.has_connected = None
        self.last_sent_at = 0
        self.current_reconnect_time = 0.5

    def connect_and_ping(self):
        s = socket.create_connection((self.ip, self.port), 5)
        self.send_influx_state("connected_before_reply")
        while True:
            ping_data = str(uuid.uuid4())
            sent_at = time.time()
            s.send("ping {random_data} {timestamp:.5f}".format(random_data=ping_data, timestamp=sent_at))
            self.logger.debug("Sent ping {random_data} {timestamp:.5f}".format(random_data=ping_data, timestamp=sent_at))
            ready = select.select([s], [], [], 5)
            if ready[0]:
                data = s.recv(1024)
                self.logger.debug("Received {data}".format(data=data))
            else:
                self.logger.info("Connected, but no reply from server.")
                s.close()
                raise socket.error
            if data.lower().startswith("pong"):
                reply_received_at = time.time()
                received_data = data.split(" ")
                if len(received_data) == 3:
                    if received_data[1] == ping_data:
                        if self.has_connected is False or self.has_connected is None:
                            self.current_reconnect_time = 0.5
                            self.send_influx_state("connected")
                            self.has_connected = True
                        self.send_status("running")
                        server_received_at = float(received_data[2])
                        client_to_server = server_received_at - sent_at
                        server_to_client = reply_received_at - server_received_at
                        if client_to_server < 0:
                            self.logger.warning("client->server timestamp is <0: {client_to_server}".format(client_to_server=client_to_server))
                        if server_to_client < 0:
                            self.logger.warning("server->client timestamp is <0: {server_to_client}".format(server_to_client=server_to_client))

                        if time.time() - self.last_sent_at > 10 and client_to_server > 0 and server_to_client > 0:
                            self.send_influx_values(client_to_server, server_to_client)
                            self.logger.debug("Sent at {sent_at:.5f}, server received at {server_received_at:.5f}, finished at {reply_received_at:.5f}. Client->server: {client_to_server:.5f}. Server->client: {server_to_client:.5f}".format(sent_at=sent_at, server_received_at=server_received_at, reply_received_at=reply_received_at, client_to_server=client_to_server, server_to_client=server_to_client))
                else:
                    self.logger.info("Received malformed reply: {reply}".format(reply=data))
            else:
                self.logger.info("Received invalid reply: {reply}".format(reply=data))
            time.sleep(1)
        s.close()

    def send_influx_values(self, client_to_server, server_to_client):
        client_to_server = round(client_to_server, 6)
        server_to_client = round(server_to_client, 6)
        self.logger.debug("Sending values to influx: client->server: {client_to_server}, server_to_client: {server_to_client}".format(client_to_server=client_to_server, server_to_client=server_to_client))
        self.last_sent_at = time.time()
        influx_data = [{
            "measurement": "socket-ping",
            "tags": {
                "host": self.ip,
                "port": self.port,
            },
            "time": datetime.datetime.utcnow().isoformat() + "Z",
            "fields": {
                "client_to_server": client_to_server,
                "server_to_client": server_to_client,
            },
        }]
        self.redis.publish("influx-update-pubsub", json.dumps(influx_data))

    def send_status(self, status):
        self.logger.debug("Broadcasting server status: {status}".format(status=status))
        self.redis.publish("home:broadcast:generic", json.dumps({"key": "server_power", "content": {"status": status}}))

    def send_influx_state(self, state):
        timestamp = datetime.datetime.utcnow().isoformat() + "Z"
        state_number = self.STATES.get(state)
        self.logger.info("State changed to {state} at {timestamp}".format(state=state, timestamp=timestamp))
        influx_data = [{
            "measurement": "internet-connection-stability",
            "tags": {
                "host": self.ip,
                "port": self.port,
            },
            "time": timestamp,
            "fields": {
                "state": state,
                "state_number": state_number,
            },
        }]
        self.redis.publish("influx-update-pubsub", json.dumps(influx_data))

    def run(self):
        while True:
            try:
                self.connect_and_ping()
            except socket.error:
                self.send_status("down")
                if self.has_connected is True or self.has_connected is None:
                    self.send_influx_state("disconnected")
                    self.has_connected = False
            self.logger.debug("Sleeping for {reconnect_time}".format(reconnect_time=self.current_reconnect_time))
            time.sleep(self.current_reconnect_time)
            self.current_reconnect_time = min(self.current_reconnect_time * 1.25, 10)


def main(args):
    if len(args) == 2:
        ip, port = args[0], args[1]
    else:
        from local_settings import PORT, SERVER_IP
        ip, port = SERVER_IP, PORT
    state_client = StateClient(ip, port)
    state_client.run()

if __name__ == '__main__':
    main(sys.argv[1:])
