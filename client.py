import grpc
import sys
import raft_pb2
import raft_pb2_grpc
import time

class raft_client():
 def __init__(self):    
  print("opening channel")
  self.table = {}
  self.stubs = []
  self.port_addr = []
  with open(sys.argv[1] + ".txt", "r") as self.f:
     self.line = self.f.readline()
     while (self.line):
         self.lineList = self.line.split()
         print(self.lineList)
         self.table[self.lineList[0]] = self.lineList[1]
         self.d = self.lineList[0], self.lineList[1]
         self.port_addr.append(self.d)
         self.line = self.f.readline()
  for self.address in self.table.values():
            self.channel = grpc.insecure_channel(self.address)
            self.stub = raft_pb2_grpc.RaftServerStub(self.channel)
            self.stubs.append(self.stub)

 def client_append_request(self,string):
  i = 0
  k = string
  for stub in self.stubs:
    print("Contacting", self.port_addr[i])
    try:
      self.req = stub.ClientAppend(raft_pb2.ClientAppendRequest(decree=k))
      print("got client append response: {}".format(self.req))
    except:
      print("cannot connect to " + str(self.port_addr[i]))
    i += 1
 def client_req_index(self, index_num):
  i = 0
  k = index_num
  for stub in self.stubs:
    try:
      self.req = stub.ClientRequestIndex(raft_pb2.ClientRequestIndexRequest(index=int(k)))
      print("got client request index response: {}".format(self.req))
    except:
      print("cannot connect to " + str(self.port_addr[i]))
    i += 1
client = raft_client()
while(True):
 d = input("Enter 1. Client Append request ; 2. Client request Index\n")
 if (int(d) == 1):
    f = input("Enter decree:\n")
    client.client_append_request(f)
 else:
    f = input("Enter Index value to be requested:\n")
    client.client_req_index(f)
   

