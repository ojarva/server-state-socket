#!/usr/bin/env python

from local_settings import PORT, PASSWORD
from threading import Thread
import logging
import select
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
    MAXIMUM_DOWNLOAD_CHUNK = 1024 * 1024 * 10

    def handle_upload(self, data):
        logger.info("Upload started", extra=self.logging_extra)
        request_data = data.split(" ", 3)
        if len(request_data) != 4:
            logger.info("Invalid upload request.", extra=self.logging_extra)
            return
        request_id = request_data[1]
        try:
            request_timestamp = float(request_data[2])
        except ValueError:
            logger.info("Received malformed timestamp: {timestamp}".format(timestamp=request_data[2]), extra=self.logging_extra)
            return
        data_len = 0
        upload_data = "dummy"
        receive_started_at = time.time()
        while len(upload_data):
            ready = select.select([self.request], [], [], .5)
            if ready[0]:
                upload_data = self.request.recv(10240)
                data_len += len(upload_data)
                if "--eof--" in upload_data:
                    logger.debug("Received eof after {data_len} bytes".format(data_len=data_len), extra=self.logging_extra)
                    receive_finished_at = time.time()
                    self.request.send("upload-completed {request_id} {receive_started:.5f} {receive_finished:.5f} {bytes}".format(request_id=request_id, receive_started=receive_started_at, receive_finished=receive_finished_at, bytes=data_len))
                    return
            else:
                self.request.send("upload-failed {request_id}".format(request_id=request_id), extra=self.logging_extra)
                return

    def handle_download(self, data):
        request_data = data.split(" ")
        if len(request_data) == 4:
            request_id = request_data[1]
            try:
                request_timestamp = float(request_data[2])
            except ValueError:
                logger.info("Received malformed timestamp: {timestamp}".format(timestamp=request_data[2]), extra=self.logging_extra)
                return
            try:
                download_size = int(request_data[3])
            except ValueError:
                logger.info("Received malformed download size.", extra=self.logging_extra)
                return
            if download_size < 0:
                logger.info("Download size must be >0", extra=self.logging_extra)
                return
            if download_size > 1024 * 1024 * 10:
                logger.info("Download size can't be >10MB", extra=self.logging_extra)
                return

            f = open("/dev/urandom")
            upload_data = f.read(min(download_size, self.MAXIMUM_DOWNLOAD_CHUNK))
            data_sent = 0
            self.request.send("downloading {request_id} {now} ".format(request_id=request_id, now=time.time()))
            while True:
                self.request.send(upload_data)
                data_sent += len(upload_data)
                remaining_data = download_size - data_sent
                if remaining_data <= 0:
                    break
                if remaining_data > self.MAXIMUM_DOWNLOAD_CHUNK:
                    upload_data = f.read(self.MAXIMUM_DOWNLOAD_CHUNK)
                else:
                    upload_data = f.read(remaining_data)
            self.request.send("--eof--")
            f.close()
        else:
            logger.info("Received malformed download request: {data}".format(data=data), extra=self.logging_extra)
            self.request.send("Invalid request\n")

    def handle_ping(self, data):
        request_data = data.split(" ")
        if len(request_data) == 3:
            request_id = request_data[1]
            try:
                request_timestamp = float(request_data[2])
            except ValueError:
                logger.info("Received malformed timestamp: {timestamp}".format(timestamp=request_data[2]), extra=self.logging_extra)
                return
            now = time.time()
            diff = now - request_timestamp
            logger.debug("Received {sent_timestamp:.5f} at {receive_timestamp:.5f}: {diff:.5f}".format(sent_timestamp=request_timestamp, receive_timestamp=now, diff=diff), extra=self.logging_extra)
            self.request.send("pong {request_id} {receive_timestamp:.5f}".format(request_id=request_id, receive_timestamp=now))
        else:
            logger.info("Received malformed ping request: {data}".format(data=data), extra=self.logging_extra)
            self.request.send("Invalid request\n")

    def handle(self):
        data = 'dummy'
        client_ip, client_port = self.client_address
        self.logging_extra = {"client_ip": client_ip, "client_port": client_port}
        logger.info("Connected", extra=self.logging_extra)
        while len(data):
            data = self.request.recv(100)
            if data != "auth %s" % PASSWORD:
                logger.debug("No authorization string sent. Disconnecting.", extra=self.logging_extra)
                self.request.close()
                return
            else:
                break
        while len(data):
            data = self.request.recv(100).strip()
            if len(data) > 0 and data.lower().startswith("ping"):
                self.handle_ping(data)
            elif len(data) > 0 and data.lower().startswith("upload"):
                self.handle_upload(data)
            elif len(data) > 0 and data.lower().startswith("download"):
                self.handle_download(data)
            else:
                logger.info("Received invalid request: {data}".format(data=data[:75]), extra=self.logging_extra)
                self.request.send("Invalid request\n")

        logger.info("Disconnected", extra=self.logging_extra)
        self.request.close()


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

server = ThreadedTCPServer(('', PORT), service)
server.serve_forever()
