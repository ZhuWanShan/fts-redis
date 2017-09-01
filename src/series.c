#include "redismodule.h"
#include <stdlib.h>
#include <sys/types.h>
#include <stdint.h>
#include <string.h>
#include "series.h"
#include <time.h>

Series* createSeries(short aggType, size_t duration, size_t size){
    printf("createSeries\n");
    Series* ss;
    ss = RedisModule_Alloc(sizeof(*ss));
    ss->duration        = duration;
    ss->lastIndex       = 0;
    ss->size            = size;
    ss->aggType         = aggType;
    ss->nextMax         = 0;
    ss->nextMin         = 0;
    ss->nextInc         = 0;
    ss->nextRef         = 0;
    
    Sample** sas;
    sas = RedisModule_Alloc(ss->size * sizeof(**sas));
    for(int i = 0; i < ss->size; i ++){
        sas[i] = NULL;
    }
    ss->data = sas;
    return ss;
}

void freeSeries(Series* ss){
    printf("freeSeries\n");
    for(int i = 0; i < ss->size; i++){
        RedisModule_Free(ss->data[i]);
    }
    RedisModule_Free(ss->data);
    RedisModule_Free(ss);
}

int pushSample(Series* ss, Sample* sa){
    printf("pushSample\n");

    if(NULL == ss || NULL == sa ){
        return STS_PUSH_ERR_NULL;
    }

    time_t current;
    time(&current);

    //未来时间
    long gap = sa->at - current;
    long maxGap = STS_MAX_FUTURE + ss->duration;
    if(gap > maxGap){
        return STS_PUSH_ERR_FUTRUE;
    }

    //过去很久，不在周期内
    if(current - sa->at > ss->duration * ss->size){
        return STS_PUSH_ERR_OUT_RANGE;
    }
    
    //第一个Sample
    if(0 == ss->lastIndex && NULL == ss->data[ss->lastIndex]){
        ss->data[0]         = sa;
        ss->lastIndex       = 0;
        return STS_PUSH_OK;
    }
    
    time_t latestTimestamp = ss->data[ss->lastIndex]->at;
    long offset = (sa->at - latestTimestamp) / ss->duration;
    size_t index  = ss->lastIndex + offset; 
    
    if(offset > 0){
        //间隔lastIndex一段时间后的数据，需要清除 lastindex + 1 到 index前的一个元素
        for(size_t i = ss->lastIndex + 1; i < index; i++){
            freeSample(ss->data[i]);
            ss->data[i] = NULL;
        }
        ss->lastIndex = index;
    }

    ss->data[index] = mergeSample(ss->data[index], sa, ss->aggType);

    return STS_PUSH_OK;
}

Sample* createSample(time_t at, double value, size_t samples){
    printf("createSample\n");
   struct Sample* sa;
   sa = RedisModule_Alloc(sizeof(*sa));
   sa->at       = at;
   sa->value    = value;
   sa->samples  = samples;
   return sa;
}

void freeSample(Sample* sa){

    printf("freeSample\n");

    if(NULL != sa){
        RedisModule_Free(sa);
    }
}

//合并两个Sample #注意，两个sample需要在同一个区间
Sample* mergeSample(Sample* thiz, Sample* that, short aggType){
    
    printf("mergeSample\n");

    if(thiz == that){
        return thiz;
    }
    
    if(NULL == thiz){
        return that;
    }
    if(NULL == that){
        return thiz;
    }

    if(aggType == STS_AGG_INRC){
        thiz->value   = thiz->value + that->value; 
    }else if(aggType == STS_AGG_MAX && thiz->value < that->value){
        thiz->value = that->value;
    }else if(aggType == STS_AGG_MIN && thiz->value > that->value){
        thiz->value = that->value;
    }else if(thiz->at < that->at){
        thiz->value = that->value;
    }

    thiz->samples = thiz->samples + that->samples;
    if(thiz->at < that->at){
        thiz->at = that->at;
    }
    RedisModule_Free(that);
    return thiz;
}
