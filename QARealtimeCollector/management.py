# 用于拉起进程
import datetime
import json
import threading
import time

import pymongo

# 定义一个准备作为线程任务的函数

import QUANTAXIS as QA
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QARealtimeCollector.collectors import QARTC_CtpBeeCollector
from QARealtimeCollector.datahandler import QARTC_Resampler


class QARC_Management(QA_Thread):
    def __init__(self, group):
        super().__init__(name='QAPBManagementGroup {}'.format(group), daemon=False)
        self.group = group
