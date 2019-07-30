#!/usr/bin/env python
# encoding: utf-8
import getopt
import os,sys,commands,time
from subprocess import *

__author__ = 'wzgchen'
'''
兼容版本：
mysql5.7 binlog
'''

def binlogsql(**kwargs):
    binlog = kwargs['binlog']
    binsql = kwargs['binsql']
    db = kwargs['db']
    bt = kwargs['bt']
    et = kwargs['et']
    bp = kwargs['bp']
    ep = kwargs['ep']
    if db:
        if bt:
            if bp:
                print '错误：参数冲突'
                usage()
            if et:
                cmd = "mysqlbinlog --no-defaults -vv %s --database=%s --start-datetime='%s' --stop-datetime='%s'>%s" % (binlog,db,bt,et,binsql)
            else:
                cmd = "mysqlbinlog --no-defaults -vv %s --database=%s --start-datetime='%s' >%s" % (binlog,db,bt,binsql)        
        elif bp:
            if bt:
                print '错误：参数冲突'
                usage()
            if ep:
                cmd = "mysqlbinlog --no-defaults -vv %s --database=%s --start-position=%s --stop-position=%s>%s" % (binlog,db,bp,ep,binsql)
            else:
                cmd = "mysqlbinlog --no-defaults -vv %s --database=%s --start-position=%s>%s" % (binlog,db,bp,binsql)
        else:
            cmd = "mysqlbinlog --no-defaults -vv %s --database=%s>%s" % (binlog,db,binsql)
    else:
        if bt:
            if bp:
                print '错误：参数冲突'
                usage()
            if et:
                cmd = "mysqlbinlog --no-defaults -vv %s  --start-datetime='%s' --stop-datetime='%s'>%s" % (binlog,bt,et,binsql)
            else:
                cmd = "mysqlbinlog --no-defaults -vv %s  --start-datetime='%s' >%s" % (binlog,bt,binsql)
        elif bp:
            if bt:
                print '错误：参数冲突'
                usage()
            if ep:
                cmd = "mysqlbinlog --no-defaults -vv %s  --start-position=%s --stop-position=%s>%s" % (binlog,bp,ep,binsql)
            else:
                cmd = "mysqlbinlog --no-defaults -vv %s  --start-position=%s>%s" % (binlog,bp,binsql)
        else:
            cmd = "mysqlbinlog --no-defaults -vv %s >%s" % (binlog,binsql)
        
    (status, result) = commands.getstatusoutput(cmd)
    if status!=0:
        sys.exit()
    else:
        return status

def binlog_analyse(**kwargs):
    try:
        tsize = kwargs['tsize']
        ttime = kwargs['ttime']
        if tsize:
            tsize = int(kwargs['tsize'])*1048576
        else:
            tsize = 1048576
        if ttime:
            ttime = int(kwargs['ttime'])
        else:
            ttime = 1
        binsql = kwargs['binsql']
        trx_num = 0
        ins_num = 0
        upd_num = 0
        del_num = 0
        ins_dict = {}
        upd_dict = {}
        del_dict = {}
        btrx = []
        ltrx = []
        print '\n'
        print 'Start analyse {}......'.format(binsql)
        print '---------------------------start---------------------------------------\n'
        print '###########################scan big trx################################\n'

        with open(binsql, 'r') as binlog_file:
            has_status=False
            s_ins_num=0
            s_upd_num=0
            s_del_num=0
            for line in binlog_file:
                if line.find('Table_map:') != -1:
                    l = line.index('server')
                    n = line.index('Table_map')
                    s = line.index('end_log_pos')
                    c = line.index('CRC32')

                    begin_time = line[:l:].rstrip(' ').replace('#', '20')
                    begin_pos  = line[s+11:c].strip()
                    begin_time = begin_time[0:4] + '-' + begin_time[4:6] + '-' + begin_time[6:]

                    time1 = time.strptime(begin_time, "%Y-%m-%d  %H:%M:%S")
                    begin_time_dt = time.mktime(time1)

                    db_name = line[n::].split(' ')[1].replace('`', '').split('.')[0]
                    tb_name = line[n::].split(' ')[1].replace('`', '').split('.')[1]
                    obj = db_name+'.'+tb_name
                    has_status=True
                    continue

                if has_status:
                    if line.startswith('### INSERT INTO'):
                        ins_num += 1
                        s_ins_num +=1
                        if obj not in ins_dict:
                            ins_dict[obj] = 1
                        else:
                            ins_dict[obj] += 1

                    if line.startswith('### UPDATE'):
                        upd_num += 1
                        s_upd_num += 1
                        if obj not in upd_dict:
                            upd_dict[obj] = 1
                        else:
                            upd_dict[obj] += 1
                    if line.startswith('### DELETE FROM'):
                        del_num +=1
                        s_del_num +=1
                        if obj not in del_dict:
                            del_dict[obj] = 1
                        else:
                            del_dict[obj] += 1
                    if  line.find('Xid =') != -1:

                        l = line.index('server')
                        end_time = line[:l:].rstrip(' ').replace('#', '20')
                        end_time = end_time[0:4] + '-' + end_time[4:6] + '-' + end_time[6:]
                        
                        time2 = time.strptime(end_time, "%Y-%m-%d  %H:%M:%S")
                        end_time_dt = time.mktime(time2)

                        s = line.index('end_log_pos')
                        c = line.index('CRC32')
                        end_pos  = line[s+11:c].strip()
                        trx_size = int(end_pos) - int(begin_pos)
                        trx_exec_time = end_time_dt-begin_time_dt
                        trx_num += 1
                        has_status=False
                        if  trx_size>=tsize:
                            tstr1 =  '%s.%s insert:%s,update:%s,delete:%s' % (db_name,tb_name,s_ins_num,s_upd_num,s_del_num)
                            tstr2 =  'begin_time:%s,trx_exec_time:%s,trx_size:%s\n' % (begin_time,trx_exec_time,trx_size)
                            btrx.append(tstr1)
                            btrx.append(tstr2)
                        elif trx_exec_time >=ttime:
                            lstr1 =  '%s.%s insert:%s,update:%s,delete:%s' % (db_name,tb_name,s_ins_num,s_upd_num,s_del_num)
                            lstr2 =  'begin_time:%s,trx_exec_time:%s,trx_size:%s\n' % (begin_time,trx_exec_time,trx_size)
                            ltrx.append(lstr1)
                            ltrx.append(lstr2)
                        
                        if btrx:
                            print '***********************big  trx size>=%sm************************\n'  % (tsize/1048576)
                        
                            for x in btrx:
                                print x 
                        if ltrx:
                            print '***********************lock trx time>=%ss************************\n'  % (ttime)
                            for x in ltrx:
                                print x 

                        s_ins_num=0
                        s_upd_num=0
                        s_del_num=0
                        btrx = []
                        ltrx = []


        print '\n'
        print '###########################trx total summary###########################'
        print 'total trx:%s,insert:%s,update:%s,delete:%s' % (trx_num,ins_num,upd_num,del_num)
        print '\n'

        ins_sort = sorted(ins_dict.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
        upd_sort = sorted(upd_dict.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
        del_sort = sorted(del_dict.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
        if ins_sort:
            for x in ins_sort:
                res =  'insert操作: {:50s}  {:50s}'.format(x[0],str(x[1]))
                print res
        if upd_sort:
            for x in upd_sort:
                res = 'update操作: {:50s}  {:50s}'.format(x[0],str(x[1]))
                print res
        if del_sort:
            for x in del_sort:
                res = 'delete操作: {:50s}  {:50s}'.format(x[0],str(x[1]))
                print res
    
        print '---------------------------end----------------------------------------\n'
        print 'Clean binsql {}......\n'.format(binsql)
        os.remove(binsql)

    except Exception as e:
        print e
        return 'funtion exec error:%s' % e


def get_parse_binlog():
    binlog = ""
    start_datetime = ""
    stop_datetime = ""
    start_position = ""
    stop_position = ""
    trx_size = ""
    trx_time = ""
    database= ""
    tmpdir = ""
    try:
        options, args = getopt.getopt(sys.argv[1:], "f:d:", ["help","binlog=","start-datetime=","stop-datetime=","start-position=","stop-position=","database=","tmpdir=","trx_size=","trx_time="])
    except getopt.GetoptError:
        print "参数输入有误!!!!!"
        options = []
    if options == [] or options[0][0] in ("--help"):
        usage()
        sys.exit()

    for name, value in options:
        if name == "-f" or name == "--binlog":
            binlog = value
        if name == "--start-datetime":
            start_datetime = value
        if name == "--stop-datetime":
            stop_datetime = value
        if name == "--start-position":
            start_position = value
        if name == "--stop-position":
            stop_position = value
        if name == "-d" or name == "--database":
            database = value
        if name == "--tmpdir":
            tmpdir = value
        if name == "--trx_size":
            trx_size = value
        if name == "--trx_time":
            trx_time = value 

        if binlog == '' :
            print "错误:请指定binlog文件名!"
            usage() 
    kw = {}
    kt = {}
    if tmpdir:
        binsql = tmpdir+binlog.split('/')[-1] + '.sql'
    else:
        binsql = '/data/'+binlog.split('/')[-1] + '.sql'
    kw['binlog']=binlog
    kw['binsql']=binsql
    kw['bt']=start_datetime
    kw['et']=stop_datetime
    kw['bp']=start_position
    kw['ep']=stop_position
    kw['db']=database
    kw['tmpdir']=tmpdir
    binlogsql(**kw)

    kt['binsql']=binsql
    kt['tsize']=trx_size
    kt['ttime']=trx_time
    binlog_analyse(**kt)



def usage():
    usage_info="""==========================================================================================
Command line options :
    --help                  # OUT : print help info
    -f, --binlog            # IN  : binlog file. (required:/opt/mysql/data3306/mysql-bin.000043)
    --start-datetime        # IN  : start datetime. (default '2018-10-18 09:46:00')
    --stop-datetime         # IN  : stop datetime. default '2018-10-18 15:49:24'
    --start-position        # IN  : start position. (default '4')
    --stop-position         # IN  : stop position. (default '18446744073709551615')
    --trx_size              # IN  : exceed transaction size. (default 1m)
    --trx_time              # IN  : exceed transaction time. (default 1s)
    --tmpdir                # IN  : binlog temp analyse file at tempdir. (default '/data/')
    -d, --database          # IN  : specify database (No default value).
Sample :
   shell> python binlog_trx_parse_ga.py --binlog=/opt/mysql/data3306/mysql-bin.000043  --database=sh --start-position=289  --stop-position=434
   shell> python binlog_trx_parse_ga.py -f /opt/mysql/data3306/mysql-bin.000043  --database=sh --start-datetime="2018-10-18 09:46:42"  --stop-datetime="2018-10-18 10:04:16"
   shell> python binlog_trx_parse_ga.py --binlog=/opt/mysql/data3306/mysql-bin.000043  --database=sh --start-position=289  --stop-position=434 --tmpdir=/root/
==============================================================================:============"""

    print usage_info
    sys.exit()

if __name__ == '__main__':
    get_parse_binlog()
