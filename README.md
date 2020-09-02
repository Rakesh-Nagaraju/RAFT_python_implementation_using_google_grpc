# RAFT_python_implementation_using_google_grpc
Implementation of the well-known RAFT protocol for Distributed Computing using Google GRPC.

# Instructions:
1.) Gitclone this repository.

2.) Install Google GRPC, follow: https://grpc.io/docs/languages/python/quickstart/

3.) Edit the data.txt file to contain the IP address and port number as per your system.

4.) Run as : $python rakesh_raft_server filename IPaddress:port_number  
      Eg: $python rakesh_raft_server data 10.10.10.10:9900
      
5.) Once RAFT is server is running. Download and run client file as : $python client filename
      Eg: $python client data

6.) For any doubts and clarifications, kindly contact rakesh.nagaraju@sjsu.edu or rakenju@gmail.com .
