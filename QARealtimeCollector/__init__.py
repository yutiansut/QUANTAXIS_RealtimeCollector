__version__ = '0.0.8'
__author__ = 'yutiansut'

import click

from QARealtimeCollector.clients import QARTC_Clients
from QARealtimeCollector.collectors import (QARTC_CtpBeeCollector,
                                            QARTC_CTPTickCollector,
                                            QARTC_RandomTick, QARTC_Stock,
                                            QARTC_WsCollector)
from QARealtimeCollector.datahandler import QARTC_Resampler


@click.command()
@click.option('--code', default='rb1910')
def start(code):
    r = QARTC_CtpBeeCollector(code)
    r.start()


@click.command()
@click.option('--code', default='rb1910')
def start_ctp(code):
    r = QARTC_CTPTickCollector(code)
    r.start()


@click.command()
@click.option('--code', default='rb1910')
@click.option('--freq', default='5min')
@click.option('--model', default='tb')
def resample(code, freq, model):
    r = QARTC_Resampler(code, freq, model)
    r.start()


@click.command()
@click.option('--code', default='rb1905')
@click.option('--date', default='20190327')
@click.option('--price', default=3980)
@click.option('--interval', default=0)
def random(code, date, price, interval):
    r = QARTC_RandomTick(code, date, price, interval)
    r.start()


def stock_collector():
    QARTC_Stock().start()