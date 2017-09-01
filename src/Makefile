#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef STS_DEPS
	STS_DEPS_DIR=../deps
endif

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -shared -Bsymbolic
else
	SHOBJ_CFLAGS ?= -dynamic -fno-common -g -ggdb
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif
#CFLAGS = -I$(RM_INCLUDE_DIR) -Wall -g -fPIC -lc -lm -std=gnu99  
CFLAGS = -I$(STS_DEPS_DIR) -Wall -g -fPIC -std=gnu99  
CC=gcc

all: fts-redis.so

fts-redis.so: module.o series.o
	$(LD) -o $@ module.o series.o $(SHOBJ_LDFLAGS) $(LIBS)

smoke: clean
	rm -rf smoke
	gcc ./smoke.c -o smoke
	./smoke

clean:
	rm -rf *.xo *.so *.o main

FORCE:
