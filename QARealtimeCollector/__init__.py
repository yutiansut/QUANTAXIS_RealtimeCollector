__version__ = '0.0.2'
__author__ = 'yutiansut'

from QARealtimeCollector.collectors import QARTC_WsCollector, QARTC_Stock, QARTC_CtpBeeCollector
from QARealtimeCollector.clients import QARTC_Clients
from QARealtimeCollector.datahandler import QARTC_Resampler
import click


@click.command()
@click.option('--code', default='rb1910')
def start(code):
    r = QARTC_CtpBeeCollector(code)
    r.start()


@click.command()
@click.option('--code', default='rb1910')
@click.option('--freq', default='5min')
@click.option('--model', default='tb')
def resample(code, freq, model):
    r = QARTC_Resampler(code, freq, model)
    r.start()
