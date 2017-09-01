#include "redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <stdint.h>
#include "series.h"

static RedisModuleType *SeriesType;

int FTS_HelpCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);

    RedisModule_ReplyWithSimpleString(ctx,
            "\
Push status to xxx:\n\
fts.push xxx time_t value samples max|min|incr|refr\n\
~~~~");

    return REDISMODULE_OK;
}

int FTS_PushCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);

    if(argc != 5){
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx,
            argv[1],
            REDISMODULE_READ|REDISMODULE_WRITE);

    int keyType = RedisModule_KeyType(key);

    Series* ss;

    if(keyType == REDISMODULE_KEYTYPE_EMPTY){
        ss = createSeries(STS_AGG_INRC, 60, 1440);
        RedisModule_ModuleTypeSetValue(key, SeriesType, ss);
    }else{
        ss = RedisModule_ModuleTypeGetValue(key);
    }

    if(RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    double value;
    long long count;
    long long at;

    if(RedisModule_StringToDouble(argv[2], &value) != REDISMODULE_OK
            || RedisModule_StringToLongLong(argv[3], &count)
            || RedisModule_StringToLongLong(argv[4], &at)){
        return RedisModule_ReplyWithError(ctx, "Wrong value in commands");
    }

    Sample* sa = createSample((time_t)at, value, (size_t)count);

    long long r = pushSample(ss, sa);

    for(int i = 0; i< ss->size; i++){
        if(ss->data[i] != NULL){
            RedisModule_Log(ctx, "warnning", "value:%f", ss->data[i]->value);
        }
    }

    RedisModule_ReplyWithLongLong(ctx, r);
    RedisModule_ReplicateVerbatim(ctx);

    return REDISMODULE_OK;
}

int FTS_RangeCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if(argc != 4) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
            REDISMODULE_READ|REDISMODULE_WRITE);

    int type = RedisModule_KeyType(key);
    if(type != REDISMODULE_KEYTYPE_EMPTY 
            && RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    long long arraylen = 0;
    
    if(type != REDISMODULE_KEYTYPE_EMPTY){
        Series* ss = RedisModule_ModuleTypeGetValue(key);
        RedisModule_ReplyWithLongLong(ctx, ss->size);
        RedisModule_ReplyWithLongLong(ctx, ss->duration);
        RedisModule_ReplyWithLongLong(ctx, ss->aggType);
        RedisModule_ReplyWithLongLong(ctx, ss->nextMax);
        RedisModule_ReplyWithLongLong(ctx, ss->nextMin);
        RedisModule_ReplyWithLongLong(ctx, ss->nextInc);
        RedisModule_ReplyWithLongLong(ctx, ss->nextRef);
        arraylen = 7;
        for(int i = 0; i < ss->size; i++){
            if(ss->data[i] != NULL){
                RedisModule_ReplyWithLongLong(ctx, ss->data[i]->at);
                RedisModule_ReplyWithDouble(ctx, ss->data[i]->value);
                RedisModule_ReplyWithLongLong(ctx, ss->data[i]->samples);
                arraylen = arraylen + 3;
            }
        }
    }

    RedisModule_ReplySetArrayLength(ctx, arraylen);

    return REDISMODULE_OK;
}

void* SeriesRdbLoad(RedisModuleIO* io, int encver){
    printf("SeriesRdbLoad\n");
    if(encver != 0){
        return NULL;
    }
    uint64_t size = (size_t)RedisModule_LoadUnsigned(io);
    uint64_t lastIndex = (size_t)RedisModule_LoadUnsigned(io);
    uint64_t duration = (size_t)RedisModule_LoadUnsigned(io);
    uint64_t aggType = (short)RedisModule_LoadUnsigned(io);
    uint64_t nextMax = (size_t)RedisModule_LoadUnsigned(io);
    uint64_t nextMin = (size_t)RedisModule_LoadUnsigned(io);
    uint64_t nextInc = (size_t)RedisModule_LoadUnsigned(io);
    unsigned nextRef = (size_t)RedisModule_LoadUnsigned(io);

    Series* ss = createSeries(aggType, duration, size);
    ss->lastIndex = lastIndex;
    ss->nextMax = nextMax;
    ss->nextMin = nextMin;
    ss->nextInc = nextInc;
    ss->nextRef = nextRef;

    for(int i = 0; i < size; i ++){
        long index = (size_t)RedisModule_LoadSigned(io);

        if(index < 0) continue;

        time_t at = (time_t)RedisModule_LoadUnsigned(io);
        double  value = RedisModule_LoadDouble(io);
        size_t samples = (size_t)RedisModule_LoadUnsigned(io);
        Sample* sa = createSample(at, value, samples);
        ss->data[index] = sa;
    }

    return ss;
}

void SeriesRdbSave(RedisModuleIO* io, void* data){
    printf("SeriesRdbSave\n");
    Series* ss = (Series*)data;
    RedisModule_SaveUnsigned(io, ss->size);
    RedisModule_SaveUnsigned(io, ss->lastIndex);
    RedisModule_SaveUnsigned(io, ss->duration);
    RedisModule_SaveUnsigned(io, ss->aggType);
    RedisModule_SaveUnsigned(io, ss->nextMax);
    RedisModule_SaveUnsigned(io, ss->nextMin);
    RedisModule_SaveUnsigned(io, ss->nextInc);
    RedisModule_SaveUnsigned(io, ss->nextRef);

    for(int i = 0; i < ss->size; i ++){
        if(ss->data[i] != NULL){
            RedisModule_SaveSigned(io, i);
            RedisModule_SaveUnsigned(io, ss->data[i]->at);
            RedisModule_SaveDouble(io, ss->data[i]->value);
            RedisModule_SaveUnsigned(io, ss->data[i]->samples);
        }else{
            RedisModule_SaveSigned(io, -1);
        }
    }
}

void SeriesRdbAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value){
    printf("SeriesRdbAofRewrite\n");
}

size_t SeriesRdbMemUsage(const void *data){
    printf("SeriesRdbMemUsage\n");
    
    Series* ss = (Series*)data;

    size_t mem = sizeof(Series);
    
    mem = mem + ss->size * sizeof(Sample*);

    for(int i = 0; i < ss->size; i++){
        if(ss->data[i] != NULL){
            mem = mem + sizeof(Sample);
        }
    }
    return mem;
}

void FreeRdbSeries(void* data){
    printf("FreeRdbSeries\n");
    freeSeries((Series*)data);
}

void SeriesRdbDigest(RedisModuleDigest *md, void *value){
    printf("SeriesRdbDigest\n");
}


/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx,"fts",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = SeriesRdbLoad,
        .rdb_save = SeriesRdbSave,
        .aof_rewrite = SeriesRdbAofRewrite,
        .mem_usage = SeriesRdbMemUsage,
        .free = FreeRdbSeries,
        .digest = SeriesRdbDigest
    };

    SeriesType = RedisModule_CreateDataType(ctx, "ftsSeries", 0, &tm);
    if (SeriesType == NULL) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"fts.push",
        FTS_PushCmd, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"fts.range",
        FTS_RangeCmd, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"fts.help",
        FTS_HelpCmd, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
