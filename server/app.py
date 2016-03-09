#!/usr/bin/env python

from local_settings import PORT
import SocketServer
from threading import Thread

class service(SocketServer.BaseRequestHandler):
    def handle(self):
        data = 'dummy'
        print "Client connected with ", self.client_address
        while len(data):
            data = self.request.recv(1024)
            if len(data) > 0 and data.lower().startswith("ping"):
                self.request.send("pong" + data[4:])
            else:
                self.request.send("Invalid request\n")

        print "Client exited"
        self.request.close()


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

server = ThreadedTCPServer(('', PORT), service)
server.serve_forever()
