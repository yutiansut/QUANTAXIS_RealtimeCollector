#  -*- coding: utf-8 -*-
#  The MIT License (MIT)
#
#  Copyright 2019 zhongjy
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy of this software
#  and associated documentation files (the "Software"), to deal in the Software without
#  restriction, including without limitation the rights to use, copy, modify, merge, publish,
#  distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
#  Software is furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all copies or
#  substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    logconf.py                                         :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: zhongjy1992 <zhongjy1992@outlook.com>      +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/03 02:33:45 by zhongjy1992       #+#    #+#              #
#    Updated: 2019/11/10 20:55:34 by zhongjy1992      ###   ########.fr        #
#                                                                              #
# **************************************************************************** #
import os
import logging.config

# 其中name为getlogger指定的名字
standard_format = '[%(asctime)s][%(threadName)s:%(thread)d][task_id:%(name)s][%(filename)s:%(lineno)d][%(levelname)s][%(message)s]'

simple_format = '[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d]%(message)s'

id_simple_format = '[%(levelname)s][%(asctime)s] %(message)s'

# logfile_dir = os.path.dirname(os.path.abspath(__file__))
logfile_dir = os.getcwd()

logfile_name = 'test.log'

if not os.path.isdir(logfile_dir):
    os.mkdir(logfile_dir)

# logfile_path = os.path.join(logfile_dir, logfile_name)


def getLoggingConfigDict(filepath):
    return {
        'version'                 : 1,
        'disable_existing_loggers': False,
        'formatters'              : {
            'verbose' : {
                'format' : "[%(asctime)s] %(levelname)s [%(filename)s:%(lineno)s] %(message)s",
                'datefmt': "%Y-%m-%d %H:%M:%S"
            },
            'simple'  : {
                'format': '%(levelname)s %(message)s'
            },
            'standard': {
                'format': standard_format
            },
        },
        'handlers'                : {
            'null'   : {
                'level': 'DEBUG',
                'class': 'logging.NullHandler',
            },
            'console': {
                'level'    : 'INFO',
                'class'    : 'logging.StreamHandler',
                'formatter': 'verbose'
            },
            'file'   : {
                'level'      : 'INFO',
                'class'      : 'logging.handlers.RotatingFileHandler',
                # 当达到100MB时分割日志
                'maxBytes'   : 1024 * 1 * 100,
                # 最多保留50份文件
                'backupCount': 0,
                # If delay is true, then file opening is deferred until the first call to emit().
                'delay'      : True,
                'filename'   : filepath,
                'formatter'  : 'verbose',
                'encoding'   : 'utf-8'
            },
            'file2'  : {
                'level'      : 'INFO',
                'class'      : 'logging.handlers.TimedRotatingFileHandler',
                'formatter'  : 'verbose',
                'encoding'   : 'utf-8',
                'utc'        : False,
                'filename'   : filepath,
                'when'       : 'h',
                'interval'   : 1,
                'backupCount': 0,
                # 'delay': True,
            }
        },
        'loggers'                 : {
            '': {
                'handlers': ['console', 'file2'],
                'level'   : 'INFO',
                # 向上（更高level的logger）传递
                # 'propagate': True,
            },
        }
    }


def update_log_file_config(logfilepath:str):
    """
    更新日志文件路径
    :param logfilepath: logfile.log, ./log/logfile.log, /tmp/logifle.log
    :return:
    """
    root = os.getcwd()
    logfile_dir = os.path.join(root, './log')
    if logfilepath.startswith('/'):
        logfile_dir = '/'.join(logfilepath.split('/')[:-1])
    elif logfilepath.startswith('./'):
        logfile_dir = os.path.join(root, '/'.join(logfilepath.split('/')[:-1]))
    else:
        logfilepath = os.path.join(logfile_dir, logfilepath)

    logfile_dir = os.path.abspath(logfile_dir)
    if not os.path.exists(logfile_dir):
        os.system('mkdir -p ' + logfile_dir)

    return getLoggingConfigDict(logfilepath)


if __name__ == '__main__':
    import logging

    # 导入上面定义的logging配置
    logging.config.dictConfig(getLoggingConfigDict(logfile_name))
    # 生成一个log实例
    logger = logging.getLogger(__name__)
    # 记录该文件的运行状态
    for i in range(1, 1000000):
        logger.info('It works!')
