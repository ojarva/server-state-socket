#!/usr/bin/env python

"""Internet connectivity.

Usage:
    client.py ping <ip> <port> --password=<password> [--update] [--debug]
    client.py speed <ip> <port> --password=<password> [--debug]

Options:
    -h --help   Show this screen.
    --version   Show version.
    --debug     Include debug logging.
    --update    Send connection status updates.
"""

import datetime
import docopt
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

    def __init__(self, ip, port, **kwargs):
        self.ip = ip
        self.port = port
        self.password = kwargs.get("password")

        self.logger = logging.getLogger("socket-state-client")
        if kwargs.get("debug"):
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)
        format_string = "%(asctime)s - {server_ip}:{server_port} - %(levelname)s - %(message)s".format(server_ip=self.ip, server_port=self.port)
        formatter = logging.Formatter(format_string)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        self.method = kwargs.get("method", "ping")
        self.update_state = kwargs.get("update", False)
        self.logger.debug("Setting method={method}, update_state={update_state}, ip={ip}, port={port}".format(method=self.method, update_state=self.update_state, ip=self.ip, port=self.port))

        self.redis = redis.StrictRedis()
        self.has_connected = None
        self.last_sent_at = 0
        self.current_reconnect_time = 0.5

    def consume_and_discard(self, soc):
        while True:
            ready = select.select([soc], [], [], .1)
            if ready[0]:
                soc.recv(1024)
            else:
                break

    def read_random_data(self, size):
        f = open("/dev/urandom")
        data = ""
        while size > 0:
            input_data = f.read(size)
            size -= len(input_data)
            data += input_data
        return data

    def measure_upload(self, soc, upload_size):
        data = self.read_random_data(upload_size)
        request_id = str(uuid.uuid4())
        self.consume_and_discard(soc)
        sent_at = time.time()
        soc.send("upload {request_id} {timestamp:.5f} {upload_size} ".format(request_id=request_id, timestamp=sent_at, upload_size=upload_size))
        self.logger.debug("Started upload request {request_id} at {timestamp:.5f} for {upload_size} bytes. Data is {data_bytes} bytes.".format(request_id=request_id, timestamp=sent_at, upload_size=upload_size, data_bytes=len(data)))
        i = 0
        chunk = 10240
        while i < len(data):
            soc.send(data[i:i + chunk])
            i += chunk
        soc.send("--eof--")
        while True:
            ready = select.select([soc], [], [], 30)
            if ready[0]:
                data = soc.recv(300)
                if data.lower().startswith("upload-failed "):
                    self.logger.info("Upload failed.")
                    return
                if data.lower().startswith("upload-completed "):
                    self.logger.info("Upload completed.")
                    received_data = data.split(" ")
                    if len(received_data) != 5:
                        self.logger.info("Received malformed upload-completed from server: {data}".format(data=data))
                        return
                    received_request_id = received_data[1]
                    if received_request_id != request_id:
                        self.logger.warn("Received out-of-sync request ID from server.")
                        return
                    try:
                        receive_started_at = float(received_data[2])
                        receive_finished_at = float(received_data[3])
                        received_bytes = int(received_data[4])
                    except ValueError:
                        self.logger.info("Received malformed upload-completed from server: {data}".format(data=data))
                        return
                    diff = receive_finished_at - sent_at
                    if diff == 0:
                        self.logger.warn("Upload processed too quickly - no time information available.")
                        return
                    speed = received_bytes / diff / 1024 / 1024 * 8
                    self.logger.info("Received upload reply: {diff}s for {bytes} bytes. {speed}Mb/s".format(diff=diff, bytes=received_bytes, speed=speed))
                    return

    def measure_download(self, soc, download_size):
        request_id = str(uuid.uuid4())
        sent_at = time.time()
        self.consume_and_discard(soc)
        soc.send("download {request_id} {timestamp:.5f} {download_size}".format(request_id=request_id, timestamp=sent_at, download_size=download_size))
        self.logger.debug("Sent download request {request_id} at {timestamp:.5f} for {download_size} bytes".format(request_id=request_id, timestamp=sent_at, download_size=download_size))
        received_bytes = 0
        receive_started_at = None
        while True:
            ready = select.select([soc], [], [], 5)
            if ready[0]:
                data = soc.recv(10240)
                if receive_started_at is None:
                    receive_started_at = time.time()
                    self.logger.debug("Receiving download data started at {started_at}".format(started_at=receive_started_at))
                    received_data = data.split(" ", 3)
                    if len(received_data) != 4:
                        self.logger.info("Received malformed reply from server: {server_reply}".format(server_reply=received_data[:3]))
                        return
                    try:
                        send_started_at = float(received_data[2])
                    except ValueError:
                        self.logger.info("Received malformed timestamp from server.")
                        return
                data_len = len(data)
                received_bytes += data_len
                if "--eof--" in data:
                    self.logger.debug("Received eof.")
                    break
            else:
                self.logger.info("No EOF received. Received {bytes} bytes of data, when requesting {download_size} bytes.".format(bytes=received_bytes, download_size=download_size))
                return
        receive_finished_at = time.time()
        consumed_time = receive_finished_at - send_started_at
        speed = received_bytes / consumed_time * 8 / 1024 / 1024
        self.logger.info("Received {bytes} bytes of data, when requesting {download_size} bytes. Spent {consumed_time}s -> {speed}Mb/s".format(bytes=received_bytes, download_size=download_size, started_at=send_started_at, finished_at=receive_finished_at, consumed_time=consumed_time, speed=speed))
        self.consume_and_discard(soc)

    def connect_and_transfer(self):
        soc = socket.create_connection((self.ip, self.port), 5)
        if self.password:
            soc.send("auth %s" % self.password)
        while True:
            self.measure_download(soc, 1024 * 1024 * 5)
            self.measure_upload(soc, 1024 * 1024 * 5)
        soc.close()

    def connect_and_ping(self):
        s = socket.create_connection((self.ip, self.port), 5)
        self.send_influx_state("connected_before_reply")
        if self.password:
            soc.send("auth %s" % self.password)
        clock_diff = []
        server_diff = None
        while True:
            request_id = str(uuid.uuid4())
            sent_at = time.time()
            s.send("ping {request_id} {timestamp:.5f}".format(request_id=request_id, timestamp=sent_at))
            self.logger.debug("Sent ping {request_id} at {timestamp:.5f}".format(request_id=request_id, timestamp=sent_at))
            ready = select.select([s], [], [], 5)
            if ready[0]:
                data = s.recv(200)
                self.logger.debug("Received {data}".format(data=data))
            else:
                self.logger.info("Connected, but no reply from server.")
                s.close()
                raise socket.error
            if data.lower().startswith("pong"):
                reply_received_at = time.time()
                received_data = data.split(" ")
                if len(received_data) == 3:
                    if received_data[1] == request_id:
                        if self.has_connected is False or self.has_connected is None:
                            self.current_reconnect_time = 0.5
                            self.send_influx_state("connected")
                            self.has_connected = True
                        self.send_status("running")
                        server_received_at = float(received_data[2])
                        client_to_server = server_received_at - sent_at
                        server_to_client = reply_received_at - server_received_at
                        if client_to_server < 0 or server_to_client < 0:
                            self.logger.warning("client->server timestamp is {client_to_server} and server->client is {server_to_client}. Resetting to roundtrip times.".format(client_to_server=client_to_server, server_to_client=server_to_client))
                            trip_time = client_to_server = server_to_client = (reply_received_at - sent_at) / 2
                        if len(clock_diff) < 5:
                            clock_diff.append((sent_at, server_received_at, reply_received_at))
                        elif server_diff is None:
                            values = []
                            for a1, a2, a3 in clock_diff:
                                values.append((a1 - a2) + (a3 - a1) / 2)
                            server_diff = float(sum(values)) / len(values)

                        if client_to_server < 0 or server_to_client < 0:
                            if server_diff is not None:
                                client_to_server += server_diff
                                server_to_client += server_diff
                        if time.time() - self.last_sent_at > 10 and client_to_server > 0 and server_to_client > 0:

                            self.send_influx_values(client_to_server, server_to_client)
                            self.logger.debug("Sent at {sent_at:.5f}, server received at {server_received_at:.5f}, finished at {reply_received_at:.5f}. Client->server: {client_to_server:.5f}. Server->client: {server_to_client:.5f}".format(sent_at=sent_at, server_received_at=server_received_at, reply_received_at=reply_received_at, client_to_server=client_to_server, server_to_client=server_to_client))
                    else:
                        self.logger.info("Received out-of-sync request id for ping.")
                else:
                    self.logger.info("Ping received malformed reply: {reply}".format(reply=data))
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
        if self.update_state:
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

    def run_speed(self):
        while True:
            try:
                self.connect_and_transfer()
            except socket.error:
                pass
            self.logger.debug("Sleeping for {reconnect_time}".format(reconnect_time=self.current_reconnect_time))
            time.sleep(self.current_reconnect_time)
            self.current_reconnect_time = min(self.current_reconnect_time * 1.25, 10)

    def run_ping(self):
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

    def run(self):
        if self.method == "ping":
            self.run_ping()
        if self.method == "speed":
            self.run_speed()


def main(args):
    arguments = docopt.docopt(__doc__, version='1.0')
    if arguments.get("ping"):
        method = "ping"
    elif arguments.get("speed"):
        method = "speed"
    else:
        method = None
    state_client = StateClient(arguments["<ip>"], arguments["<port>"], debug=arguments.get("--debug"), update=arguments.get("--update"), method=method, password=arguments["--password"])
    state_client.run()

if __name__ == '__main__':
    main(sys.argv[1:])
