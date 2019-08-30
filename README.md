# QAREALTIMEMARKETCOLLECTOR
quantaxis 实时行情采集/分发

本项目从QAREALTIME_SOLUTION 拆出来

```
pip install qarealtime_collector
```


## 系统环境

为了适配 QA_Service的docker

- EventMQ_IP  默认 127.0.0.1
- MONGODB  默认 127.0.01

## 关于采集

适配两种行情采集

- 快期的5挡websocket行情
- ctp直连的tick

- 股票的5挡行情订阅推送
    (包括A股/指数/创业板)


会产生

- 可订阅的实时tick exchange
- 可订阅的实时bar exchange


基于此


策略订阅(行情端) --> 基于QATrader的账户信息 --> 下单到EventMQ 业务总线



## 关于订阅申请:


标准化订阅topic合约流程:
QARC_Stock  (只有股票需要开)
QARC_WEBSERVER

1. 发起订阅请求
2. 开始订阅数据
3. 取消订阅(系统释放资源)

此环节已经被docker集成, 具体参见QUANTAXIS的 qaservice [https://github.com/QUANTAXIS/QUANTAXIS/tree/master/docker/qa-service-future]


期货订阅请求

POST: http://localhost:8011?action=new_handler&market_type=future_cn&code=au1911

股票订阅请求:

POST: http://localhost:8011?action=new_handler&market_type=stock_cn&code=000001


二次采样请求

POST: http://localhost:8011?action=new_resampler&market_type=future_cn&code=au1911&frequence=2min



股票的主推的eventmq的exchange :stocktransction

可以使用 qaps_sub --exchange stocktransaction --model fanout 来测试


## 启动

```bash
nohup QACTPBEE --userid 133496  >> ./output_ctpbee.log 2>&1 &

nohup QARC_Start --code rb1910 >> ./output_qarcCollect.log 2>&1 &

nohup QARC_Resample --code rb1910 --freq 60min >> ./output_resample.log 2>&1 &
```

如果是虚拟行情测试

```
nohup QARC_Random  --code rb1910 --date 20190619 --price 3800 --interval 1

切记: 此命令会污染实时行情源, 切记不能和实时行情同时运行

price是设定的初始价格, 会基于ou行情伪造实时tick

interval是tick间隔, 1 指的是1秒一个
```


## EXCHANGE格式:


期货:

    - {type(realtime/bar)}_{freq(1min/5min/15min/60min)}_{code(rb1910/jm1909)}

    期货的data exchange由3个参数组成:

    1.type : realtime/bar (realtime就是在这个级别下的实时更新)

    2.freq : 1min/ 5min/ 15min/ 30min/ 60min/

    3.code : rb1910/j1909

股票

    - {type(realtime/tick/bar)}_{freq(1min/5min/15min/60min)}_{code(0000001/000002)}

    期货的data exchange由3个参数组成:

    1.type : realtime/bar (realtime就是在这个级别下的实时更新)

    2.freq : 1min/ 5min/ 15min/ 30min/ 60min/

    3.code : rb1910/j1909


## 数据格式


    realtime

    {'open': float,
    'high': float,
    'low': float,
    'close': float,
    'code': str UPPERCASE(大写),
    'datetime': str '2019-08-16 09:25:00:500000'
    'volume': float
    }



