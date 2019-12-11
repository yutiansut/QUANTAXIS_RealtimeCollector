# QAREALTIMEMARKETCOLLECTOR

> quantaxis 实时行情采集/分发。本项目从QAREALTIME_SOLUTION 拆出来

- 流程

策略订阅(行情端) --> 基于QATrader的账户信息 --> 下单到EventMQ 业务总线

![image.png](http://pic.yutiansut.com/Flzp4Cr6qSDZmkd9H43-Quugm5oO)

## 数据

> 期货源

- 快期的5挡websocket行情
- ctp直连的tick

> 股票源

- 股票的5挡行情订阅推送(包括A股/指数/创业板)

> RabbitMQ

- 可订阅的实时tick exchange
- 可订阅的实时bar exchange

## 安装

```bash
pip install qarealtime_collector
# ubuntu install rabbitmq-server
sudo apt install rabbitmq-server -y
sudo rabbitmq-plugins enable rabbitmq_management
sudo service rabbitmq-server restart
# 为QAREALTIMEMARKETCOLLECTOR增加一个rabbitmq的账户
sudo rabbitmqctl add_user admin admin
# 用户组具体请根据需要调整
sudo rabbitmqctl set_user_tags admin administrator
# 用户权具体请根据需要调整
sudo rabbitmqctl set_permissions -p / admin '.*' '.*' '.*'
# 期货需要安装QACTPBEE、QACTPBeeBroker
pip install ctpbee==0.24
pip install git+https://github.com/yutiansut/QACTPBeeBroker
sudo locale-gen zh_CN.GB18030
```

## 系统环境

> 为了适配 QA_Service的docker采用了以下默认参数（若非docker请按实际情况修改QARealtimeCollector/setting.py）

```python
mongo_ip = os.environ.get('MONGODB', '127.0.0.1')
eventmq_ip = os.environ.get('EventMQ_IP', '127.0.0.1')
market_data_user = 'admin'
market_data_password = 'admin'
```

## 启动服务

- realtime

```bash
QARC_WEBSERVER
# 股票
QARC_Stock
# 期货
nohup QACTPBEE --userid 133496  >> ./output_ctpbee.log 2>&1 &
# 缺少说明
nohup QARC_Start --code rb1910 >> ./output_qarcCollect.log 2>&1 &
# 缺少说明
nohup QARC_Resample --code rb1910 --freq 60min >> ./output_resample.log 2>&1 &
```

- 虚拟行情

```bash
QARC_WEBSERVER
# 虚拟行情测试, 切记: 此命令会污染实时行情源, 切记不能和实时行情同时运行
# price是设定的初始价格, 会基于ou行情伪造实时tick
# interval是tick间隔, 1 指的是1秒一个
nohup QARC_Random  --code rb1910 --date 20190619 --price 3800 --interval 1
```

## 关于订阅申请

此环节已经被docker集成, 具体参见QUANTAXIS的 qaservice [https://github.com/QUANTAXIS/QUANTAXIS/tree/master/docker/qa-service-future]

> 标准化订阅topic合约流程:

- 1.发起订阅请求

```bash
# 期货订阅请求
curl -X POST "http://127.0.0.1:8011?action=new_handler&market_type=future_cn&code=au1911"
```bash
# 股票订阅请求
curl -X POST "http://127.0.0.1:8011?action=new_handler&market_type=stock_cn&code=000001"
# 二次采样请求
curl -X POST "http://127.0.0.1:8011?action=new_resampler&market_type=future_cn&code=au1911&frequence=2min"
```

- 2.开始订阅数据

```bash
# eventmq: 股票的主推的 exchange 为 stocktransction
qaps_sub --exchange stocktransaction --model fanout
```

- 3.取消订阅(系统释放资源)

## EXCHANGE格式

> 格式: $type_$freq_$code

- example: realtime_1min_rb1910, bar_15min_jm1909

> 期货

|key|value|comment|
|:-|:-|:-|
|type|realtime,bar|realtime就是在这个级别下的实时更新|
|freq|1min,5min, 15min, 30min ,60min|周期|
|code|rb1910,j1909, etc.|期货合约代码|

> 股票

|key|value|comment|
|:-|:-|:-|
|type|realtime,tick, bar|realtime就是在这个级别下的实时更新|
|freq|1min,5min, 15min, 30min ,60min|周期|
|code|000001,000002, etc.|股票代码|

## 数据格式

- realtime

|key|value_type|comment|
|:-|:-|:-|
|open|float|开盘价|
|high|float|最高价|
|low|float|最低价|
|close|float|收盘价|
|code|str|代码，UPPERCASE(大写)|
|datetime|str|2019-08-16 09:25:00:500000|
|volume|float|成交量|

```python
{
    'open': float,
    'high': float,
    'low': float,
    'close': float,
    'code': str,
    'datetime': str '2019-08-16 09:25:00:500000'
    'volume': float
}
```
