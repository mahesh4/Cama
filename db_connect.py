import pymongo
import json, os
from sshtunnel import SSHTunnelForwarder

MONGO_KEYFILE = "Cama.pem"
MONGO_IP = "129.114.26.230"


class DBConnect:

    def __init__(self):
        global MONGO_IP

        # SSH / Mongo Configuration #
        self.MONGO_SERVER = SSHTunnelForwarder(
            (MONGO_IP, 22),
            ssh_username="cc",
            ssh_pkey="~/.ssh/{0}".format(MONGO_KEYFILE),
            remote_bind_address=('localhost', 27017)
        )

        self.MONGO_CLIENT = None

    def connect_db(self):
        try:
            # open the SSH tunnel to the mongo server
            self.MONGO_SERVER.start()
            # open mongo connection
            self.MONGO_CLIENT = pymongo.MongoClient('localhost', self.MONGO_SERVER.local_bind_port)

        except Exception as e:
            print('cannot connect')
            raise e

    def get_connection(self):
        return self.MONGO_CLIENT

    def disconnect_db(self):
        try:

            # close mongo connection
            self.MONGO_CLIENT.close()
            # close the SSH tunnel to the mongo server
            self.MONGO_SERVER.close()
            self.MONGO_CLIENT = None

            print('closed all the connections')

        except Exception as e:
            print('cannot disconnect')
            raise e



