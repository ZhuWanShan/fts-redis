#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef SRC_DIR
	SRC_DIR=src
endif

all: module.so

module.so:
	$(MAKE) -C ./$(SRC_DIR)
	cp ./$(SRC_DIR)/fts-redis.so .

clean: FORCE
	rm -rf *.xo *.so *.o
	rm -rf ./$(SRC_DIR)/*.xo ./$(SRC_DIR)/*.so ./$(SRC_DIR)/*.o
	rm -rf ./$(RMUTIL_LIBDIR)/*.so ./$(RMUTIL_LIBDIR)/*.o ./$(RMUTIL_LIBDIR)/*.a

FORCE:
