import os


mongo_ip = os.environ.get('MONGODB', '127.0.0.1')
eventmq_ip = os.environ.get('EventMQ_IP', '127.0.0.1')
market_data_user = 'admin'
market_data_password = 'admin'