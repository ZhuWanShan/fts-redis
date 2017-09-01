#ifndef STS_SERIES_H
#define STS_SERIES_H

#define STS_MAX_FUTURE 120

#define STS_AGG_MAX  1
#define STS_AGG_MIN  2
#define STS_AGG_INRC 3
#define STS_AGG_RFER 4

#define STS_PUSH_OK 1
#define STS_PUSH_ERR_FUTRUE 2
#define STS_PUSH_ERR_OUT_RANGE 3
#define STS_PUSH_ERR_NULL 4

#include "redismodule.h"

typedef struct Sample{
    time_t       at;             //发生时间
    double       value;          //值
    size_t       samples;        //样本数
}Sample;

typedef struct Series{
    size_t              size;             //Series中的Sample个数
    size_t              lastIndex;          //最新一个Sample的位置
    size_t              duration;           //Series的Sample步长
                                            //在同一个步长中的Sample会按照aggType合并
    short               aggType;            //聚合类型，max/min/incr/refresh
    
    struct Sample**     data;               //指针数组，数组长度固定为maxLength， 每个元素为sample的引用

    size_t              nextMax; //继续后续的聚合倍率 0 无后续
    size_t              nextMin;
    size_t              nextInc;
    size_t              nextRef;
}Series;
struct Series* createSeries(short aggType, size_t duration, size_t maxLength);
void freeSeries(Series* ss);
int pushSample(Series* ss, Sample* sa);

struct Sample* createSample(time_t at, double value, size_t samples);
void freeSample(Sample* sa);
struct Sample* mergeSample(Sample* thiz, Sample* that, short aggType);
#endif
