1.可以统计binlog表操作频率 <br>
2.可以查看binlog中的锁     <br>
3.可以查看binlog中的大事务 <br>

Command line options :<br>
    --help                  # OUT : print help info <br>
    -f, --binlog            # IN  : binlog file. (required:mysql-bin.000043) <br>
    --t                     # OUT : binlog start time and  end time point. (default off) <br>
    --start-datetime        # IN  : start datetime. (default '2018-10-18 09:46:00') <br>
    --stop-datetime         # IN  : stop datetime. default '2018-10-18 15:49:24' <br>
    --start-position        # IN  : start position. (default '4',chioce before Table_map at) <br>
    --stop-position         # IN  : stop position. (default '18446744073709551615',choce before xid at) <br>
    --trx_size              # IN  : exceed transaction size. (default 1m) <br>
    --trx_time              # IN  : exceed transaction time. (default 1s) <br>
    --tmpdir                # IN  : binlog temp analyse file at tempdir. (default '/tmp/') <br>
    -d, --database          # IN  : specify database (No default value). <br>
Sample :<br>
   # 查看binlog时间起始点
   shell> python binlog_trx_parse_rm.py -f mysql-bin.004595 -h 192.168.1.101 -uadmin -p123 -P3306 -t
   # 根据时间点进行分析
   shell> python binlog_trx_parse_rm.py -f mysql-bin.004595 -h 192.168.1.101 -uadmin -p123 -P3306 --start-datetime="2018-11-04 14:46:42"  --stop-datetime="2018-11-04 15:00:00" <br>
   shell> python binlog_trx_parse_rm.py -f mysql-bin.004595 -h 192.168.1.101 -uadmin -p123 -P3306 --start-datetime="2018-11-04 14:46:42"  --stop-datetime="2018-11-04 15:00:00" -d sh
   # 根据post点进行分析
   shell> shell> python binlog_trx_parse_rm.py -f mysql-bin.004595 -h 192.168.1.101 -uadmin -p123 -P3306  --start-position=6471  --stop-position=9995
