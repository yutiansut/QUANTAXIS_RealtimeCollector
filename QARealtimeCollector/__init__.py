__version__ = '0.0.10'
__author__ = 'yutiansut'

import click
import threading
from QARealtimeCollector.clients import QARTC_Clients
from QARealtimeCollector.collectors import (QARTC_CtpBeeCollector,
                                            QARTC_CTPTickCollector,
                                            QARTC_RandomTick, QARTC_Stock,
                                            QARTC_WsCollector)
from QARealtimeCollector.datahandler import QARTC_Resampler


@click.command()
@click.option('--code', default='rb2001')
def start(code):
    r = QARTC_CtpBeeCollector(code)
    r.start()


@click.command()
@click.option('--code', default='rb2001')
def start_ctp(code):
    r = QARTC_CTPTickCollector(code)
    r.start()


@click.command()
@click.option('--code', default='rb2001')
def faststart(code):
    r = QARTC_CtpBeeCollector(code)
    r.start()
    r1 = QARTC_Resampler(code, '1min', 'tb')
    r1.start()
    r2 = QARTC_Resampler(code, '5min', 'tb')
    r2.start()
    r3 = QARTC_Resampler(code, '15min', 'tb')
    r3.start()
    r4 = QARTC_Resampler(code, '30min', 'tb')
    r4.start()
    r5 = QARTC_Resampler(code, '60min', 'tb')
    r5.start()


@click.command()
@click.option('--code', default='rb2001')
@click.option('--freq', default='5min')
@click.option('--model', default='tb')
def resample(code, freq, model):
    r = QARTC_Resampler(code, freq, model)
    r.start()


@click.command()
@click.option('--code', default='rb2001')
@click.option('--date', default='20191119')
@click.option('--price', default=3646)
@click.option('--interval', default=0)
def random(code, date, price, interval):
    r = QARTC_RandomTick(code, date, price, interval)
    r.start()


def stock_collector():
    QARTC_Stock().start()
