import asyncio
import os
import sys
import threading

import tornado
from tornado.options import (define, options, parse_command_line,
                             parse_config_file)
from tornado.web import Application, RequestHandler, authenticated

import QUANTAXIS as QA
from qarealtimecollector.collectors import (QARTC_CtpBeeCollector,
                                            QARTC_CTPTickCollector,
                                            QARTC_RandomTick, QARTC_Stock,
                                            QARTC_WsCollector)
from qarealtimecollector.datahandler import QARTC_Resampler
from QAWebServer import QABaseHandler, QAWebSocketHandler


class ONReqSubscribe(QAWebSocketHandler):
    def open(self):
        pass


class REQ_Sources(QABaseHandler):
    user = QA.QA_User(username='quantaxis', password='quantaxis')

    def post(self):
        action = self.get_argument('action')

        """
        action: subscribe/ unsubscribe
        username:
        password:
        client_id: xxxxxx
        freqence: 1min
        code:  RB1910
        market: future_cn/ stock_cn
        model: realtime/bar
        """
        freqence = self.get_argument('freqence')
        code = self.get_argument('code')
        market = self.get_argument('market')
        model = self.get_argument('model')
        self.user.sub_code(code,freqence,market)
        self.user.save()


handlers = [
    (r"/sub",
     REQ_Sources),


]


def main():
    asyncio.set_event_loop(asyncio.new_event_loop())
    define("port", default=8011, type=int, help="服务器监听端口号")

    define("address", default='0.0.0.0', type=str, help='服务器地址')
    define("content", default=[], type=str, multiple=True, help="控制台输出内容")
    parse_command_line()
    apps = Application(
        handlers=handlers,
        debug=True,
        autoreload=True,
        compress_response=True
    )
    port = options.port


    stock_coll = QARTC_Stock(username='quantaxis', password ='quantaxis')

    threading.Thread(target=)

    http_server = tornado.httpserver.HTTPServer(apps)
    http_server.bind(port=options.port, address=options.address)
    """增加了对于非windows下的机器多进程的支持
    """
    http_server.start(1)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()
