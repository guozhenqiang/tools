# -*- coding: UTF-8 -*-
"""
    Author: guozhenqiang
    Email: 1003704757@qq.com
"""
import logging
import commands
import sys
import traceback
import datetime

# 配置缺省日志格式，如调用方未传递，则使用缺省日志格式
logging.basicConfig(level=logging.INFO,
                    format='|%(asctime)s| %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    )


def exe_cmd_system(cmd, exit=True, logger=logging):
    """
    exit参数：True表示cmd执行返回值不等于0报错并退出当前进程，False表示cmd执行返回值不等于0，忽略错误，并且继续执行
    """
    logger.info('>>> [%s]' % cmd)
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        if not exit:
            logger.warning('>>> [%s] error, exit(0);\t error msg: %s' % (cmd, traceback.format_exc()))
        else:
            logger.error('>>> [%s] error, exit(1);\t error msg: %s' % (cmd, traceback.format_exc()))
            sys.exit(1)
    return status, output


def mkdir(directory, exists=False, logger=logging):
    """
    exists参数：False目录存在时报错，True目录存在不报错
    """
    cmd = 'mkdir %s' % directory
    if exists:
        cmd = 'mkdir -p %s' % directory
    exe_cmd_system(cmd, True, logger)


def date_d2s(dt=datetime.date.today(), delta=0, dest_format='%Y-%m-%d'):
    """
    将一个date对象转换成另一个date对象，并且按照给定格式返回字符串
    """
    try:
        dt_new = dt + datetime.timedelta(delta)
        dt_new = dt_new.strftime(dest_format)
        return dt_new
    except Exception as e:
        logging.error('get_date_d2s(dt=%s, delta=%s, format=%s) error! error msg: %s' % (dt, delta, format, e))


def date_s2s(dt, ori_format='%Y-%m-%d', delta=0, dest_format='%Y-%m-%d'):
    """
    将一个date字符串对象转换成另一个date对象，并且按照给定格式返回字符串
    """
    try:
        cur = datetime.datetime.strptime(dt, ori_format)
        dt_new = cur + datetime.timedelta(delta)
        dt_new = dt_new.strftime(dest_format)
        return dt_new
    except Exception as e:
        logging.error('get_date_s2s(dt=%s, ori_format=%s, delta=%s, dest_format=%s) error! error msg: %s' %
                      (dt, ori_format, delta, dest_format, e))


if __name__ == '__main__':
    mkdir('./test', exists=True)
    print date_s2s('2020-05-21', ori_format='%Y-%m-%d', delta=-1)
    pass
