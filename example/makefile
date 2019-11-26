GCC = g++

INCLUDE_PATH = -I ../include
LIB_PATH = ../include/hiredis/libhiredis.a
BIN_PATH = ../bin

SRC_FILE = ../src/redis.cpp ./*.cpp

REDIS_OBJ = redis.o
STRING_OBJ = example_string.o
HASH_OBJ = example_hash.o
LIST_OBJ = example_list.o
SET_OBJ = example_set.o
ZSET_OBJ = example_zset.o

OBJS = $(REDIS_OBJ) $(STRING_OBJ) $(HASH_OBJ) $(LIST_OBJ)

STRING_EXE = $(BIN_PATH)/example_string
HASH_EXE = $(BIN_PATH)/example_hash
LIST_EXE = $(BIN_PATH)/example_list
SET_EXE = $(BIN_PATH)/example_set
ZSET_EXE = $(BIN_PATH)/example_zset

ifdef string
$(STRING_EXE):$(REDIS_OBJ) $(STRING_OBJ) 
	$(GCC) -o $(STRING_EXE) $(REDIS_OBJ) $(STRING_OBJ) $(LIB_PATH)
endif

ifdef hash
$(HASH_EXE):$(REDIS_OBJ) $(HASH_OBJ) 
	$(GCC) -o $(HASH_EXE) $(REDIS_OBJ) $(HASH_OBJ) $(LIB_PATH)
endif

ifdef list
$(LIST_EXE):$(REDIS_OBJ) $(LIST_OBJ) 
	$(GCC) -o $(LIST_EXE) $(REDIS_OBJ) $(LIST_OBJ) $(LIB_PATH)
endif

ifdef set
$(SET_EXE):$(REDIS_OBJ) $(SET_OBJ)
	$(GCC) -o $(SET_EXE) $(REDIS_OBJ) $(SET_OBJ) $(LIB_PATH)
endif

ifdef zset
$(ZSET_EXE):$(REDIS_OBJ) $(ZSET_OBJ)
	$(GCC) -o $(ZSET_EXE) $(REDIS_OBJ) $(ZSET_OBJ) $(LIB_PATH)
endif

$(OBJS):$(SRC_FILE)
	$(GCC) -c $(SRC_FILE) $(INCLUDE_PATH)

clean:
	rm *.o
