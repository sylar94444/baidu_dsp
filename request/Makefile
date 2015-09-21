PROTO_COMPILER=`which protoc`
SRC_DIR = .
PROTO_SRC_DIR = .
DEST_DIR = .

all: baidu_realtime_bidding_pb2.py 

baidu_realtime_bidding_pb2.py : baidu_realtime_bidding.proto
	@echo "[[1;32;40mTarget:'[1;32;40mmake'[0m]"
	$(PROTO_COMPILER) -I=$(SRC_DIR) --python_out=$(DEST_DIR) baidu_realtime_bidding.proto 
	@echo "make all done"

clean : 
	@echo "[[1;32;40mTarget:'[1;32;40mclean'[0m]"
	rm baidu_realtime_bidding_pb2.py -rf
