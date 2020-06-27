# -*- coding: UTF-8 -*-
"""
    Author: guozhenqiang
    Email: 1003704757@qq.com
"""
import logging
import commands
import sys
import datetime
import os


# 配置缺省日志格式; 如调用方传递logger，则使用调用方logger;调用方未传递时则使用缺省logging
logging.basicConfig(level=logging.INFO, format='|%(asctime)s| %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')


def exe_cmd_system(cmd, exit=True, logger=logging, debug=7):
    """
    exit参数：True表示cmd执行返回值不等于0报错并退出当前进程，False表示cmd执行返回值不等于0，忽略错误，并且继续执行
    debug: 位图表示，缺省值为7；
           1.显示cmd
           2.显示warning日志信息
           3.显示error日志信息
           4.显示status
           5.显示output
    """
    if debug & 1:
        logger.info('>>> [%s]' % cmd)
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        if not exit:
            if debug & 2:
                logger.warning('>>> [%s] error, exit(0);\n error msg: %s' % (cmd, output))
        else:
            if debug & 4:
                logger.error('>>> [%s] error, exit(1);\n error msg: %s' % (cmd, output))
            sys.exit(1)
    if debug & 8:
        logger.info('status=%s' % status)
    if debug & 16:
        logger.info('output=%s' % output)
    return status, output


def exe_cmd_system_print_precess(cmd, exit=True, logger=logging, debug=7):
    """
    exit参数：True表示cmd执行返回值不等于0报错并退出当前进程，False表示cmd执行返回值不等于0，忽略错误，并且继续执行
    debug: 位图表示，缺省值为7；
           1.显示cmd
           2.显示warning日志信息
           3.显示error日志信息
           4.显示status
           5.显示output
    """
    if debug & 1:
        logger.info('>>> [%s]' % cmd)
    status = os.system(cmd)
    if status != 0:
        if not exit:
            if debug & 2:
                logger.warning('>>> [%s] error, exit(0);' % cmd)
        else:
            if debug & 4:
                logger.error('>>> [%s] error, exit(1);' % cmd)
            sys.exit(1)
    if debug & 8:
        logger.info('status=%s' % status)
    return status


def mkdir(path, exists=False, logger=logging):
    """
    exists参数：False目录存在时报错，True目录存在不报错
    """
    cmd = 'mkdir %s' % path
    if exists:
        cmd = 'mkdir -p %s' % path
    exe_cmd_system(cmd, True, logger)


def rm(path, exit=True, logger=logging):
    """
    exit参数：False错误时不退出，True错误时退出
    """
    cmd = 'rm -r %s' % path
    exe_cmd_system(cmd, exit, logger)


def mv(from_path, to_path, exit=True, logger=logging):
    """
    exit参数：False移动错误时不退出，True移动错误时退出
    """
    cmd ='mv %s %s' % (from_path, to_path)
    exe_cmd_system(cmd, exit, logger)


def cp(from_path, to_path, exit=True, logger=logging):
    """
    exit参数：False拷贝错误时不退出，True拷贝错误时退出
    """
    cmd = 'cp -r %s %s' % (from_path, to_path)
    exe_cmd_system(cmd, exit, logger)


def date_d2s(dt=datetime.date.today(), delta=0, dest_format='%Y-%m-%d'):
    """
    将一个date对象转换成另一个date对象，并且按照给定格式返回字符串
    """
    try:
        dt_new = dt + datetime.timedelta(delta)
        dt_new = dt_new.strftime(dest_format)
        return dt_new
    except Exception as e:
        logging.error('get_date_d2s(dt=%s, delta=%s, format=%s) error! error msg: %s' % (dt, delta, dest_format, e))


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


class Hdfs(object):
    """
        Hdfs集群工具类
        exit: True表示运行错误时退出结束，False表示运行错误时不退出继续运行
    """
    def __init__(self):
        pass

    @staticmethod
    def file_exist(path, logger=logging):
        """
        检查hdfs文件或目录是否存在，存在返回True，不存在返回False
        """
        cmd = 'hadoop fs -test -e %s' % (path)
        status, output = exe_cmd_system(cmd, False, logger)
        return True if status == 0 else False

    @staticmethod
    def file_size(path, exit=True, logger=logging):
        """
        获取hdfs目录下文件总大小
        """
        cmd = 'hadoop fs -du -s %s | cut -f1 -d" "' % path
        status, output = exe_cmd_system(cmd, exit, logger)
        return int(output) if status == 0 else 0

    @staticmethod
    def file_num(path, exit=True, logger=logging):
        """
        获取hdfs目录下文件总数量
        """
        cmd = 'hadoop fs -ls %s | grep %s | wc -l' % (path, path)
        status, output = exe_cmd_system(cmd, exit, logger)
        return int(output) if status == 0 else 0

    @staticmethod
    def rm(path, skip_Trash=True, exit=True, logger=logging):
        """
        skip_Trash: True表示跳过Trash，False表示不跳过Trash
        """
        cmd = 'hadoop fs -rm -r %s' % path
        if skip_Trash:
            cmd = 'hadoop fs -rm -r -skipTrash %s' % path
        exe_cmd_system(cmd, exit, logger)

    @staticmethod
    def mkdir(path, exit=True, logger=logging):
        cmd = 'hadoop fs -mkdir %s' % path
        exe_cmd_system(cmd, exit, logger)

    @staticmethod
    def cp(from_path, to_path, exit=True, logger=logging):
        cmd = 'hadoop fs -cp %s %s' % (from_path, to_path)
        exe_cmd_system(cmd, exit, logger)

    @staticmethod
    def mv(from_path, to_path, exit=True, logger=logging):
        cmd = 'hadoop fs -mv %s %s' % (from_path, to_path)
        exe_cmd_system(cmd, exit, logger)

    @staticmethod
    def put(from_path, to_path, exit=True, logger=logging):
        cmd = 'hadoop fs -put %s %s' % (from_path, to_path)
        exe_cmd_system(cmd, exit, logger)

    @staticmethod
    def get(from_path, to_path, merge=True, exit=True, logger=logging):
        """
        merge: True合并get的文件成一个，False不合并get的文件
        """
        cmd = 'hadoop fs -get %s %s' % (from_path, to_path)
        if merge:
            cmd = 'hadoop fs -getmerge %s %s' % (from_path, to_path)
        exe_cmd_system(cmd, exit, logger)


class Hive(object):
    """
        Hive表工具类
        exit: True表示错误时退出结束，False表示错误时不退出继续运行
    """
    def __init__(self):
        pass

    @staticmethod
    def partition_exist(table, partition_key, partition_value, exit=True, logger=logging):
        """
        检查hdfs文件或目录是否存在，存在返回True，不存在返回False
        """
        hql = 'show partitions %s;' % table
        cmd = 'hive -S -e \"%s\" | grep %s | grep %s | wc -l' % (hql, partition_key, partition_value)
        status, output = exe_cmd_system(cmd, exit, logger)
        return True if status == 0 and int(output) > 0 else False

    @staticmethod
    def del_partition(table, partition_key, partition_value, del_hdfs=True, exit=True, logger=logging, debug=7):
        """
        删除hive表分区
        del_hdfs: True删除分区的hdfs数据, False只删除分区
        """
        hql = "ALTER TABLE %(table)s DROP IF EXISTS PARTITION (%(key)s='%(value)s');" % \
              {'table': table, 'key': partition_key, 'value': partition_value}
        cmd = 'hive -S -e \"%s\"' % hql
        status, output = exe_cmd_system(cmd, exit, logger, debug)
        if status != 0:
            return False
        if not del_hdfs:
            return True
        # 开始删除hive表分区的hdfs地址数据
        table_path = Hive.get_table_hdfs(table, exit=False)
        if table_path is None:
            return False
        table_path_full = '%s/%s=%s' % (table_path, partition_key, partition_value)
        Hdfs.rm(table_path_full, skip_Trash=True, exit=True)
        return True

    @staticmethod
    def exe_hql(hql, static=False, exit=True, logger=logging, debug=7):
        """
        static: True打印mapreduce日志，False不打印mapreduce日志
        """
        cmd = 'hive -e \"%s\"' % hql
        if static:
            cmd = 'hive -S -e \"%s\"' % hql
            status, output = exe_cmd_system(cmd, exit, logger, debug)
            return True if status == 0 else False
        status = exe_cmd_system_print_precess(cmd, exit, logger, debug)
        return True if status == 0 else False

    @staticmethod
    def partition_num(table, exit=True, logger=logging):
        """
        返回hive表下分区总数量
        """
        hql = 'show partitions %s;' % table
        cmd = 'hive -S -e \"%s\" | wc -l' % hql
        status, output = exe_cmd_system(cmd, exit, logger)
        return int(output) if status == 0 else 0

    @staticmethod
    def msck(table, static=False, exit=True, logger=logging):
        """
        static: True打印mapreduce日志，False不打印mapreduce日志
        """
        hql = 'msck repair table %s;' % table
        cmd = 'hive -e \"%s\"' % hql
        if static:
            cmd = 'hive -S -e \"%s\"' % hql
        exe_cmd_system_print_precess(cmd, exit, logger)

    @staticmethod
    def get_table_hdfs(table, exit=True, logger=logging):
        """
        获取hive表实际存放的hdfs地址
        """
        hql = 'show create table %s;' % table
        cmd = 'hive -S -e \"%s\"' % hql
        status, output = exe_cmd_system(cmd, exit, logger)
        if status != 0:
            return None
        key = 'LOCATION'
        path = None
        lines = output.split('\n')
        i = -1
        for i in range(len(lines)):
            if lines[i] == key:
                break
        if i not in range(len(lines)):
            return path
        path = lines[i+1].strip().strip('\'')
        return path


if __name__ == '__main__':
    # mkdir('./test', exists=True)
    # print date_s2s('2020-05-21', ori_format='%Y-%m-%d', delta=-1)
    # rm('./test', exists=False)
    # mv('./test', '../', exit=False)
    # cp('./util.py', '../py3')
    # Hdfs.file_exist('deeff')
    # Hive.del_partition('ads', 'dt', '2020-05-20')
    # Hive.msck('ad_search.midpage_re_purchase_base_data')
    # print Hive.get_table_hdfs('ad_search.midpage_base_imp')
    print Hive.del_partition('ad_search.midpage_re_purchase_base_data', 'dt', '2020-05-11')
    pass

