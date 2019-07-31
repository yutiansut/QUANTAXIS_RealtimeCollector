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
