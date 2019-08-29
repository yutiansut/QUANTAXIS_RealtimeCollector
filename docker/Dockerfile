FROM daocloud.io/quantaxis/qactpbee:latest

RUN cd /root \
    git clone https://github.com/yutiansut/QUANTAXIS_RealtimeCollector \
    && cd /root/QUANTAXIS_RealtimeCollector && pip install -e .

COPY start_collector.sh ~/


CMD ["bash", "start_collector.sh"]
