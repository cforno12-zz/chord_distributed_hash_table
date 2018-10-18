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
        self.nodeid_obj.ip = unicode(ip)
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
        if pred.id == self.nodeid_obj.id or self.nodeid_obj.id == unicode(int(key, 16)):
            return self.getNodeSucc()

        client, transport = get_client_and_transport_objs(pred)
        transport.open()
        succ_node = client.getNodeSucc()
        transport.close()
        return succ_node
                

    def findPred(self, key):
        # we have to check if the fingertable is properly set
        if len(self.fingertable) == 0:
            raise SystemException("Fingertable is not properly set for this node, {}".format(self.nodeid_obj))

        nodehash = int(self.nodeid_obj.id.decode('utf-8'),16)
        tempkey = key
        key = int(key,16)
        list_len = len(self.fingertable)
        
        for i in range(list_len-1, 0, -1):
            curr_node_key = int(self.fingertable[i].id.decode('utf-8'),16)

            cond1 = curr_node_key > nodehash and curr_node_key < key
            cond2 = curr_node_key < key and key < nodehash
            cond3 = curr_node_key > nodehash and key < nodehash

            if cond1 or cond2 or cond3:
                client, transport = get_client_and_transport_objs(self.fingertable[i])
                transport.open()
                true_pred = client.findPred(tempkey)
                transport.close()
                return true_pred

        return self.nodeid_obj
            

    def getNodeSucc(self):
        if len(self.fingertable) == 0:
            raise SystemException("Something is wrong with this node's fingertable.")
        return self.fingertable[0]

    def getnodeobj():
        return self.nodeid_obj
            



def get_client_and_transport_objs(node):
    transport = TSocket.TSocket(node.ip.decode('utf-8'), node.port)
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
