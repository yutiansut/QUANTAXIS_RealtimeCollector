# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    common.py                                          :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: zhongjy1992 <zhongjy1992@outlook.com>      +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/10/13 16:08:30 by zhongjy1992       #+#    #+#              #
#    Updated: 2020/03/05 13:31:13 by zhongjy1992      ###   ########.fr        #
#                                                                              #
# **************************************************************************** #
import datetime
import os

from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_trade
from QUANTAXIS.QAUtil.QAParameter import MARKET_TYPE
from joblib import Parallel, delayed
from pandas import concat, date_range, DataFrame, DatetimeIndex


def create_empty_stock_df(code, date: datetime.datetime = None, frequency=1):
    """
    创建空K线表并填0
    :param code:
    :param date:
    :param frequency:
    :return:
    """
    if isinstance(code, list):
        return
    code = fill_stock_code(code)
    cur_date = datetime.datetime.now() if date is None else date
    cur_day = cur_date.isoformat()[:10]
    # TODO confirm , 9:31 - 11:29, 13:00 - 15:00
    # morning = date_range('%s 9:31' % cur_day, periods=119, freq='T').to_list()
    morning = date_range('%s 9:30' % cur_day, end='%s 11:30' % cur_day, freq='%sT' % frequency).to_list()[1:]
    # afternoon = date_range('%s 13:00' % cur_day, periods=121, freq='T').to_list()
    afternoon = date_range('%s 13:00' % cur_day, end='%s 15:00' % cur_day, freq='%sT' % frequency).to_list()[1:]
    # if frequency == 1:
    #     # 1min remove 11:30
    #     morning = morning[:-1]
    # elif frequency in [15, 30, 60, 120]:
    #     # remove 13:00
    #     afternoon = afternoon[1:]
    morning.extend(afternoon)
    # datetime type is Timestamp('2019-10-24 13:00:00', freq='1T')
    df = DataFrame({'datetime': morning})
    df['code'] = code
    df['open'] = 0
    df['high'] = 0
    df['low'] = 0
    df['close'] = 0
    df['vol'] = 0
    df['amount'] = 0
    # df['year'] = cur_date.year
    # df['month'] = cur_date.month
    # df['day'] = cur_date.day
    # hour , minute, month
    return df.set_index(['datetime', 'code'])


def fill_stock_code(data):
    """
    深市代码不足6位补0
    :param data:
    :return:
    """
    if not isinstance(data, str):
        data = str(data)
    length = len(data)
    if length < 6:
        return "0" * (6 - length) + data
    return data


def get_file_name_by_date(filename='stock.%s.log', log_dir='./log'):
    """
    返回填充日期的文件名
    :param filename:
    :param log_dir:
    :return:
    """
    _filename = filename % datetime.datetime.today().isoformat()[:10]
    if log_dir is None:
        return _filename
    else:
        if not os.path.exists(log_dir):
            os.system('mkdir -p %s' % log_dir)
        return os.path.join(log_dir, _filename)


def logging_csv(df, filename, float_format='%.3f', index=False, mode='a'):
    """
    dataframe 输出为csv 格式，追加模型
    :param df: pd.DataFrame
    :param filename:
    :param float_format:
    :param index:
    :param mode: a/w, a+/w+,
    :return:
    """
    if os.path.exists(filename):
        df.to_csv(filename, float_format=float_format, index=index, mode=mode, header=False)
    else:
        df.to_csv(filename, float_format=float_format, index=index, mode=mode, header=True)


def tdx_bar_data_stock_resample(min_data, period=5):
    """
    1min 分钟线采样成 1,5,15,30,60,120 级别的分钟线
    TODO 240时间戳有问题
    :param min_data:
    :param period:
    :return:
    """
    min_data = min_data.reset_index()
    if 'datetime' not in min_data.columns:
        return None

    if isinstance(period, float):
        period = int(period)
    elif isinstance(period, str):
        period = int(period.replace('min', ''))
    elif isinstance(period, int):
        pass
    _period = '%sT' % period
    # TODO 确认时间格式 yyyy-mm-dd HH:MM:SS
    # min_data.datetime = min_data.datetime.apply(datetime.datetime.fromisoformat)
    min_data = min_data.set_index('datetime')
    # 9:30 - 11:30
    min_data_morning = min_data.loc[datetime.time(9, 30):datetime.time(11, 30)]
    min_data_morning.index = DatetimeIndex(min_data_morning.index).to_period('T')
    # 13:00 - 15:00
    min_data_afternoon = min_data.loc[datetime.time(13, 00):datetime.time(15, 00)]
    min_data_afternoon.index = DatetimeIndex(min_data_afternoon.index).to_period('T')

    _conversion = {
        'code' : 'first',
        'open' : 'first',
        'high' : 'max',
        'low'  : 'min',
        'close': 'last',
    }
    if 'vol' in min_data.columns:
        _conversion["vol"] = "sum"
    elif 'volume' in min_data.columns:
        _conversion["volume"] = "sum"
    if 'amount' in min_data.columns:
        _conversion['amount'] = 'sum'
    _base = 0
    if period > 60:
        _base = 60
    res = concat([
        min_data_morning.resample(
            _period, label="right", closed="right", kind="period", loffset="0min", base=30 + _base).apply(
            _conversion),
        min_data_afternoon.resample(
            _period, label="right", closed="right", kind="period", loffset="0min", base=_base).apply(
            _conversion)
    ])
    return res.dropna().reset_index().set_index(["datetime", "code"]).sort_index()


def tdx_bar_data_stock_resample_parallel(min_data, period=5):
    """
    1min 分钟线采样成 1,5,15,30,60,120 级别的分钟线
    TODO 240时间戳有问题
    :param min_data:
    :param period:
    :return:
    """
    min_data = min_data.reset_index()
    if 'datetime' not in min_data.columns:
        return None

    if isinstance(period, float):
        period = int(period)
    elif isinstance(period, str):
        period = int(period.replace('min', ''))
    elif isinstance(period, int):
        pass
    _period = '%sT' % period
    # TODO 确认时间格式 yyyy-mm-dd HH:MM:SS
    # min_data.datetime = min_data.datetime.apply(datetime.datetime.fromisoformat)
    min_data = min_data.set_index('datetime')
    # 9:30 - 11:30
    min_data_morning = min_data.loc[datetime.time(9, 30):datetime.time(11, 30)]
    min_data_morning.index = DatetimeIndex(min_data_morning.index).to_period('T')
    # 13:00 - 15:00
    min_data_afternoon = min_data.loc[datetime.time(13, 00):datetime.time(15, 00)]
    min_data_afternoon.index = DatetimeIndex(min_data_afternoon.index).to_period('T')

    _conversion = {
        'code' : 'first',
        'open' : 'first',
        'high' : 'max',
        'low'  : 'min',
        'close': 'last',
    }
    if 'vol' in min_data.columns:
        _conversion["vol"] = "sum"
    elif 'volume' in min_data.columns:
        _conversion["volume"] = "sum"
    if 'amount' in min_data.columns:
        _conversion['amount'] = 'sum'
    _base = 0
    if period > 60:
        _base = 60
    return [
        min_data_morning.resample(
            _period, label="right", closed="right", kind="period", loffset="0min", base=30 + _base).apply(
            _conversion),
        min_data_afternoon.resample(
            _period, label="right", closed="right", kind="period", loffset="0min", base=_base).apply(
            _conversion)
    ]


def pandas_apply_parallel(df_grouped, func, period: int or str, jobs: int = 2):
    ret_lst = Parallel(n_jobs=jobs)(delayed(func)(group, period) for name, group in df_grouped)
    ret = []
    for i in ret_lst:
        ret.extend(i)
    return concat(ret).dropna().reset_index().set_index(["datetime", "code"]).sort_index()


def tdx_stock_bar_resample_parallel(data, frequency: int or str = "5min", jobs: int = 2):
    return pandas_apply_parallel(
        data.reset_index().groupby('code'), tdx_bar_data_stock_resample_parallel, frequency, jobs)


def util_is_trade_time(
        _time=datetime.datetime.now(),
        market=MARKET_TYPE.STOCK_CN,
        code=None
):
    """判断当前是否为交易时间"""
    date_today = _time.isoformat()[0:10]
    if market is MARKET_TYPE.STOCK_CN:
        if QA_util_if_trade(date_today):
            if _time.hour in [10, 13, 14]:
                return True
            elif _time.hour == 9 and _time.minute >= 15:  # 修改成9:15 加入 9:15-9:30的盘前竞价时间
                return True
            elif _time.hour == 11 and _time.minute <= 32:  # 11:30 -> 11:31 也刷新数据
                return True
            # elif _time.hour == 12 and _time.minute >= 58:  # 12:58 - 13:00 也刷新数据
                # return True
            elif _time.hour == 15 and _time.minute <= 2:   # 15:00 - 15:02 也刷新数据
                return True
            else:
                return False
        else:
            return False
    elif market is MARKET_TYPE.FUTURE_CN:
        date_yesterday = str((_time - datetime.timedelta(days=1)).date())

        is_today_open = QA_util_if_trade(date_today)
        is_yesterday_open = QA_util_if_trade(date_yesterday)

        # 考虑周六日的期货夜盘情况
        if is_today_open == False:  # 可能是周六或者周日
            if is_yesterday_open == False or (_time.hour > 2 or _time.hour == 2 and _time.minute > 30):
                return False

        shortName = ""  # i , p
        for i in range(len(code)):
            ch = code[i]
            if ch.isdigit():  # ch >= 48 and ch <= 57:
                break
            shortName += code[i].upper()

        period = [
            [9, 0, 10, 15],
            [10, 30, 11, 30],
            [13, 30, 15, 0]
        ]

        if (shortName in ["IH", 'IF', 'IC']):
            period = [
                [9, 30, 11, 30],
                [13, 0, 15, 0]
            ]
        elif (shortName in ["T", "TF"]):
            period = [
                [9, 15, 11, 30],
                [13, 0, 15, 15]
            ]

        if 0 <= _time.weekday <= 4:
            for i in range(len(period)):
                p = period[i]
                if ((_time.hour > p[0] or (_time.hour == p[0] and _time.minute >= p[1])) and (
                        _time.hour < p[2] or (_time.hour == p[2] and _time.minute < p[3]))):
                    return True

        # 最新夜盘时间表_2019.03.29
        nperiod = [
            [
                ['AU', 'AG', 'SC'],
                [21, 0, 2, 30]
            ],
            [
                ['CU', 'AL', 'ZN', 'PB', 'SN', 'NI'],
                [21, 0, 1, 0]
            ],
            [
                ['RU', 'RB', 'HC', 'BU', 'FU', 'SP'],
                [21, 0, 23, 0]
            ],
            [
                ['A', 'B', 'Y', 'M', 'JM', 'J', 'P', 'I', 'L', 'V', 'PP', 'EG', 'C', 'CS'],
                [21, 0, 23, 0]
            ],
            [
                ['SR', 'CF', 'RM', 'MA', 'TA', 'ZC', 'FG', 'IO', 'CY'],
                [21, 0, 23, 30]
            ],
        ]

        for i in range(len(nperiod)):
            for j in range(len(nperiod[i][0])):
                if nperiod[i][0][j] == shortName:
                    p = nperiod[i][1]
                    condA = _time.hour > p[0] or (_time.hour == p[0] and _time.minute >= p[1])
                    condB = _time.hour < p[2] or (_time.hour == p[2] and _time.minute < p[3])
                    # in one day
                    if p[2] >= p[0]:
                        if ((_time.weekday >= 0 and _time.weekday <= 4) and condA and condB):
                            return True
                    else:
                        if (((_time.weekday >= 0 and _time.weekday <= 4) and condA) or (
                                (_time.weekday >= 1 and _time.weekday <= 5) and condB)):
                            return True
                    return False
        return False
