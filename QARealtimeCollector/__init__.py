__version__ = '0.0.1'
__author__ = 'yutiansut'

from QARealtimeCollector.collectors import QARTC_WsCollector, QARTC_Stock, QARTC_CtpBeeCollector
from QARealtimeCollector.clients import QARTC_Clients
import click


@click.command()
@click.option('--code', default='rb1910')
def start(code):
    r = QARTC_CtpBeeCollector(code)
    r.start()
