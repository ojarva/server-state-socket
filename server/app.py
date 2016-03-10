#!/usr/bin/env python

from local_settings import PORT
from threading import Thread
import logging
import SocketServer
import time


logger = logging.getLogger("socket-state-server")
logger.setLevel(logging.INFO)
format_string = "%(asctime)s - %(client_ip)s:%(client_port)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(format_string)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


class service(SocketServer.BaseRequestHandler):
    def handle(self):
        data = 'dummy'
        client_ip, client_port = self.client_address
        logging_extra = {"client_ip": client_ip, "client_port": client_port}
        logger.info("Connected", extra=logging_extra)
        while len(data):
            data = self.request.recv(1024).strip()
            if len(data) > 0 and data.lower().startswith("ping"):
                request_data = data.split(" ")
                if len(request_data) == 3:
                    request_id = request_data[1]
                    request_timestamp = float(request_data[2])
                    now = time.time()
                    diff = now - request_timestamp
                    logger.debug("Received {sent_timestamp:.5f} at {receive_timestamp:.5f}: {diff:.5f}".format(sent_timestamp=request_timestamp, receive_timestamp=now, diff=diff), extra=logging_extra)
                    self.request.send("pong {request_id} {receive_timestamp:.5f}".format(request_id=request_id, receive_timestamp=now))
                else:
                    logger.info("Received malformed ping request: {data}".format(data=data), extra=logging_extra)
                    self.request.send("Invalid request\n")
            else:
                logger.info("Received invalid request: {data}".format(data=data), extra=logging_extra)
                self.request.send("Invalid request\n")

        logger.info("Disconnected", extra=logging_extra)
        self.request.close()


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

server = ThreadedTCPServer(('', PORT), service)
server.serve_forever()
