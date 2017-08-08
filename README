README
------

for server:
$ ./ofpcp -i 0 -c 4 -l -N [-a] [-w /data]
-l [IP_ADDRESS]: 
	mean listen on any port if have no ip_address following it.
-a: 
	ask server to auto create the non-exist dir path if client have the dir path.
-w WORK_DIR: 
	ask server change the working dir to specific location.
-S:
	pseudo mode. in pseudo mode, there is no data writing in local files.
-N:
	notify mode. 
-I STATISTICS_SECONDS:
	server start statistic in file ofpcp.stat
	

for client:
$ ./ofpcp -i 0 -c 10 -s 192.168.100.33 [-p 12345] [-P 4] [-b 8] -d ../*.data
-s IP_ADDRESS: 
	specify the server address to connect
-p PORT: 
	if ommited, use default port
-d FILES: 
	FILES can use wildcard and with path, be careful the path will be created in the server side if it has -a argument.
-b SPLIT_BLOCK_K: 
	specify the split block size in KB, default is 16, if specify 0, it mean send files without split.
-C 
	enter to interactive command mode
-B 
	send file with burst mode, means use multi thread concurrently.
-P POOL_COUNT: 
	set to pool pthread mode, each thread has a pre-connection to server.
-S:
	pseudo mode. in pseudo mode, there is no data read from local files.
-k PINGPONG_COUNT:
	set the pingpong count, test the latency between server.
-A AGENT_COUNT:
	set the dedicated agent thread count, other threads send packets through these threads.


