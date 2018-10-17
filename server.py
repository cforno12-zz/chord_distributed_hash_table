#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import glob
import sys
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

#hashlib to convert string to SHA256
import hashlib
import socket


class FileStoreHandler:
    def __init__(self, ip, port):
        self.files = {}
        self.fingertable = []
        self.nodeid_obj = NodeID()
        self.nodeid_obj.id = unicode(hashlib.sha256(str(ip) + ":" + str(port)).hexdigest())
        self.nodeid_obj.ip = ip
        self.nodeid_obj.port = port        

    def writeFile(self, rFile):
        filename = rFile.meta.filename
        file_id = hashlib.sha256(filename).hexdigest()

        if self.nodeid_obj != self.findSucc(file_id):
            raise SystemException("The node, {} does not own this file".format(self.nodeid_obj))

        if file_id in self.files:
            self.files[file_id].content = rFile.content
            self.files[file_id].meta.contentHash = rFile.meta.contentHash
            self.files[file_id].meta.version += 1
        else:
            #we want to add it to our files dictionary
            rFile.meta.version = 0
            self.files[file_id] = rFile
            

    def readFile(self, filename):
        file_id = hashlib.sha256(filename).hexdigest()
        if file_id in self.files:
            return self.files[file_id]
        else:
            raise SystemException("File {} not found.".format(filename))

        
    def setFingertable(self, node_list):
        self.fingertable = node_list

        
    def findSucc(self, key):
        if int(self.nodeid_obj.id, 16) < int(key, 16) and int(key,16) <= int(self.getNodeSucc().id, 16):
            return self.getNodeSucc()

        pred = self.findPred(key)
        client, transport = get_client_and_transport_objs(pred)
        transport.open()
        succ_node = client.getNodeSucc()
        transport.close()
        return succ_node
                

    def findPred(self, key):
        print ("we are in the findPred function")
        if int(self.nodeid_obj.id, 16) < int(key, 16) and int(key, 16) <= int(self.getNodeSucc().id, 16):
            print("we are returning a node obj")
            return self.nodeid_obj
        else:
            print("this isn't the fucking node so we are searching for another one")
            curr_node = self.node_finder(key)
            print("curr_node", curr_node.id)
            print("this node", self.nodeid_obj.id)
            if curr_node.id == self.nodeid_obj.id:
                print('We were the closest preceding node')
                return self.getNodeSucc()
            
            print("we returned from the node_finder function")
            client, transport = get_client_and_transport_objs(curr_node)
            print("we get the client and transport objs")
            transport.open()
            print("we opened the transport")
            curr_node_succ = client.getNodeSucc()
            print("we got he node succ")
            transport.close()
            print("closed transport")
            while not int(curr_node.id, 16) < int(key, 16) and int(key, 16) <= int(curr_node_succ.id, 16):
                curr_node = client.findPred(key)
                client, transport = get_client_and_transport_objs(curr_node)
                transport.open()
                curr_node_succ = client.getNodeSucc()
                transport.close()
        print("exiting findPred")


    def getNodeSucc(self):
        print ("we are in the getNodeSucc function of the client whatever that means")
        if len(self.fingertable) == 0:
            raise SystemException("Something is wrong with this node's fingertable.")
        print("we are returning the node's succ")
        return self.fingertable[0]
            

    def node_finder(self, key):
        key = int(key, 16)
        for i in range(255, 0, -1):
            if int(self.fingertable[i].id, 16) < key:
                return self.fingertable[i]
        return self.nodeid_obj


def get_client_and_transport_objs(node):
    transport = TSocket.TSocket(node.ip, node.port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    client = FileStore.Client(protocol)
    return client, transport

if __name__ == '__main__':
    ip_add = socket.gethostbyname(socket.gethostname())
    handler = FileStoreHandler(ip_add, int(sys.argv[1]))
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
