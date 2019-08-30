FROM daocloud.io/quantaxis/qactpbee:latest

RUN cd /root \
    && git clone https://github.com/yutiansut/QUANTAXIS_RealtimeCollector \
    && cd /root/QUANTAXIS_RealtimeCollector && pip install -e .



EXPOSE 8011
CMD ["bash", "/root/QUANTAXIS_RealtimeCollector/docker/start_collector.sh"]
