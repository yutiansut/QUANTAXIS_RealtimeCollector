# QAREALTIMEMARKETCOLLECTOR
quantaxis 实时行情采集/分发

从QAREALTIME_SOLUTION 拆出来

适配两种行情采集

- 快期的5挡websocket行情
- ctp直连的tick


会产生

- 可订阅的实时tick exchange
- 可订阅的实时bar exchange


基于此


策略订阅(行情端) --> 基于QATrader的账户信息 --> 下单到EventMQ 业务总线
