#############################################################################################
#Distributed Computing Assignment Project on RAFT Implementation.
#Author: Rakesh Nagaraju.     #Student ID: 014279304.
#############################################################################################

#############################################################################################
#Running Instruction: $python (rakesh_raft_server.py) (config-file) (port to start server on)
#############################################################################################
from atexit import register
from concurrent import futures
from os.path import isfile
from math import ceil
from random import random
from time import time, sleep
import grpc
import sys
import raft_pb2
import raft_pb2_grpc

#Main class for RAFT server.
class Raft_server(raft_pb2_grpc.RaftServerServicer):
    #Initialization.   
    def __init__(self, my_dict_address):
        self.my_state = 'follower'
        print("I am a Follower!!!")
        self.last_log_idx = 0
        self.last_log_term = 0
        self.commit_idx = 0
        self.id = int(sys.argv[3])
        self.leader_id = None
        self.log = {}
        self.my_dict_address = my_dict_address
        self.my_dict_address.pop(str(self.id))
        self.port_addr = []
        self.term = 0
        self.timeout = time() + 11 + 4 * random() 
        self.vote_count = 0
        self.voted_for = -1
        self.stub_list = []
        #Storing address of all channels to be contacted to.
        for i in self.my_dict_address:
            tmp = i,self.my_dict_address[i]
            self.port_addr.append(tmp)
        if (isfile('log.txt')):
            with open('log.txt', 'r') as fp:
                line = fp.readline()
                while (line):
                    temp_list = line.strip('\n').split(' ')
                    entry = raft_pb2.Entry(term=int(temp_list[0]), index=int(temp_list[1]), decree=temp_list[2])
                    self.log[entry.index] = entry
                    self.last_log_idx = entry.index
                    self.last_log_term = entry.term
                    line = fp.readline()
        print("Opening Channel...") 
        for self.address in self.my_dict_address.values():
            channel = grpc.insecure_channel(self.address)
            stub = raft_pb2_grpc.RaftServerStub(channel)
            self.stub_list.append(stub)

#Function to Update my state once in a while. 
    def update(self):
        if (self.my_state == 'follower'):
            if (time() > self.timeout):
                self.my_state = 'candidate'
                print('Now I am a Candidate!!!')
        elif (self.my_state == 'candidate'):
            if (time() > self.timeout):
                self.term += 1
                self.vote_count = 1
                #Requesting Vote.
                print('Requesting vote....')
                req = raft_pb2.RequestVoteRequest(term=self.term, cadidateId=self.id, lastLogIndex=self.last_log_idx,
                                                  lastLogTerm=self.last_log_term)
                i = 0
                for stub in self.stub_list:
                    try:
                        #Store Response.
                        response = stub.RequestVote(req)
                        print('Got request vote response: {}'.format(response))
                        if (response.voteGranted):
                            self.vote_count += 1
                    except:
                        print('cannot connect to ' + str(self.port_addr[i]))
                    i += 1
                self.timeout = time() + 11 + 4 * random()
            elif (self.vote_count >= (len(self.my_dict_address) + 1) // 2 + 1):
                self.my_state = 'leader'
                self.vote_count = 1
                self.voted_for = self.id
                self.timeout = time()
                print('Now I am the Leader!!!')
        elif (self.my_state == 'leader'):
            if (time() > self.timeout):
                prevLogIndex = self.last_log_idx
                if (prevLogIndex in self.log):
                    entry = self.log[prevLogIndex]
                else:
                    entry = None
                #Append Entries Request.
                req = raft_pb2.AppendEntriesRequest(term=self.term, leaderId=self.id, prevLogIndex=prevLogIndex,
                                                    prevLogTerm=self.last_log_term, entry=entry,
                                                    leaderCommit=self.commit_idx)
                print('Sending Append entries request...')
                i = 0
                for stub in self.stub_list:
                    try:
                        response = stub.AppendEntries(req)
                        print('I am the Leader!!!')
                        print('Got append entries response: {}'.format(response))
                        while (response.success == False):
                            prevLogIndex -= 1
                            entry = self.log[prevLogIndex]
                            #Append Entries Request.
                            print('Sending Append entries request...')
                            req = raft_pb2.AppendEntriesRequest(term=self.term,leaderId=self.id,
                                                                prevLogIndex=prevLogIndex, prevLogTerm=entry.term,
                                                                entry=entry, leaderCommit=self.commit_idx)
                            response = stub.AppendEntries(req)
                            print('I am the Leader!!!')
                            print('Got append entries response: {}'.format(response))
                        while (prevLogIndex < self.last_log_idx):
                            prevLogIndex += 1
                            entry = self.log[prevLogIndex]
                            print('Sending Append entries request...')
                            #Append Entries Request.
                            req = raft_pb2.AppendEntriesRequest(term=self.term, leaderId=self.id,
                                                                prevLogIndex=prevLogIndex, prevLogTerm=entry.term,
                                                                entry=entry, leaderCommit=self.commit_idx)
                            response = stub.AppendEntries(req)
                            print('I am the Leader!!!')
                            print('Got append entries response: {}'.format(response))
                    except:
                        print('cannot connect to ' + str(self.port_addr[i]))
                    i += 1
                self.timeout = time() + 10

#Function to Request Vote.
    def RequestVote(self, req, context):
        print('Got request vote: {}'.format(req))
        if (req.term > self.term):
            self.term = req.term
            self.voted_for = -1
        if (req.term < self.term):
            print('Returning Vote Response as false since requested term is less')
            return raft_pb2.RequestVoteResponse(term=self.term, voteGranted=False)
        if ((self.voted_for == -1 or self.voted_for == req.cadidateId) and (req.lastLogTerm > self.last_log_term or (
                req.lastLogTerm == self.last_log_term and req.lastLogIndex >= self.last_log_idx))):
            self.my_state = 'follower'
            print('I am a Follower!!!')
            self.voted_for = req.cadidateId
            self.timeout = time() + 11 + 4 * random()
            print('Returning Vote Response : granted vote')
            return raft_pb2.RequestVoteResponse(term=self.term, voteGranted=True)
        print('Returning Vote Response as False')
        return raft_pb2.RequestVoteResponse(term=self.term, voteGranted=False)

#Function to handle Append Entries.
    def AppendEntries(self, req, context):
        print('Got append entries: {}'.format(req))
        if (req.term < self.term or (
                req.prevLogIndex in self.log and self.log[req.prevLogIndex].term != req.prevLogTerm)):
            print('Returning Append entries Response as false since requested term is less')  
            return raft_pb2.AppendEntriesResponse(term=self.term, success=False)
        self.my_state = 'follower'
        print('I am a follower!!!')
        self.term = req.term
        self.leader_id = req.leaderId
        self.timeout = time() + 11 + 4 * random()
        if (req.prevLogIndex == self.last_log_idx + 1 and req.prevLogTerm >= self.last_log_term):
            self.log[req.entry.index] = req.entry
            self.last_log_idx += 1
            self.last_log_term = req.prevLogTerm
            print(self.log)
            print('Returning Append entries Response as True')
            return raft_pb2.AppendEntriesResponse(term=self.term, success=True)
        if (req.prevLogIndex == self.last_log_idx and req.prevLogTerm == self.last_log_term):
            print('Returning Append entries Response as True')
            return raft_pb2.AppendEntriesResponse(term=self.term, success=True)
        print('Returning Append entries Response as false')
        return raft_pb2.AppendEntriesResponse(term=self.term, success=False)

#Function to handle Client Append Request.
    def ClientAppend(self, req, context):
        print('Got client append: {}'.format(req))
        if (self.my_state != 'leader'):
            print('Returning Client append Response as rc = 1 since I am not a leader')
            return raft_pb2.ClientAppendResponse(rc=1, leader=self.leader_id, index=self.last_log_idx)
        self.last_log_term = self.term
        self.last_log_idx += 1
        entry = raft_pb2.Entry(term=self.term, index=self.last_log_idx, decree=req.decree)
        self.log[self.last_log_idx] = entry
        #Append Entries Request.
        print('Sending Append entries Request...')        
        req = raft_pb2.AppendEntriesRequest(term=self.term, leaderId=self.id, prevLogIndex=self.last_log_idx,
                                            prevLogTerm=self.last_log_term, entry=entry, leaderCommit=self.commit_idx)
        i = 0
        for stub in self.stub_list:
            try:
                response = stub.AppendEntries(req)
                print('I am the Leader!!!')
                print('Got append entries response: {}'.format(response))
            except:
                print('cannot connect to ' + str(self.port_addr[i]))
            i += 1
        self.commit_idx += 1
        self.timeout = time() + 10
        print('Returning Client append Response as rc = 0')
        return raft_pb2.ClientAppendResponse(rc=0, leader=self.id, index=self.last_log_idx)

#Function to handle client request index.
    def ClientRequestIndex(self, req, context):
        print('Got client request index: {}'.format(req))
        if (req.index in self.log):
            print('Returning Client request index Response as rc = 0')
            return raft_pb2.ClientRequestIndexResponse(rc=0, leader=self.id, index=req.index,
                                                       decree=self.log[req.index].decree)
        if (self.my_state != 'leader'):
            print('Returning Client request index Response as rc = 1 since I am not a leader')
            return raft_pb2.ClientRequestIndexResponse(rc=1, leader=self.leader_id, index=req.index, decree=None)
        print('Returning Client request index Response as rc = 0')
        return raft_pb2.ClientRequestIndexResponse(rc=0, leader=self.id, index=req.index, decree=None)  
#Storing Log Information.
    def get_log(self):
        return self.log

#Function to write to disk.
def writeToDisk():
    print("Writing...")
    with open('log.txt', 'w') as f:
        log = raftserver.get_log()
        for entry in log.values():
            f.write(str(entry.term) + ' ' + str(entry.index) + ' ' + entry.decree + '\n')
#Calling the write to disk function.
register(writeToDisk)

#Read the text file and store replica_number and address:Port as (key,value) pair in a Dictionary.
my_dict_address = {}
with open(sys.argv[1] + '.txt', 'r') as f:
    line = f.readline()
    while (line):
        temp_list = line.split()
        print(temp_list)
        my_dict_address[temp_list[0]] = temp_list[1]
        line = f.readline()

#Calling the Raft_server class.
raftserver = Raft_server(my_dict_address)
server = grpc.server(futures.ThreadPoolExecutor())
raft_pb2_grpc.add_RaftServerServicer_to_server(raftserver, server)
#Set default IP address and Port number is taken as argument.
server.add_insecure_port('[::]:' + sys.argv[2])
#Start the server.
server.start()
while True:
    sleep(0.1)
    raftserver.update()
#END,