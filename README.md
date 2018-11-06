Command line options :<br>
    --help                  # OUT : print help info
    -f, --binlog            # IN  : binlog file. (required:mysql-bin.000043)
    --t                     # OUT : binlog start time and  end time point. (default off)
    --start-datetime        # IN  : start datetime. (default '2018-10-18 09:46:00')
    --stop-datetime         # IN  : stop datetime. default '2018-10-18 15:49:24'
    --start-position        # IN  : start position. (default '4',chioce before Table_map at)
    --stop-position         # IN  : stop position. (default '18446744073709551615',choce before xid at)
    --trx_size              # IN  : exceed transaction size. (default 1m)
    --trx_time              # IN  : exceed transaction time. (default 1s)
    --tmpdir                # IN  : binlog temp analyse file at tempdir. (default '/tmp/')
    -d, --database          # IN  : specify database (No default value).
Sample :
   # 查看binlog时间起始点
   shell> python binlog_trx_parse_rm.py -f mysql-bin.004595 -h 192.168.1.101 -uadmin -p123 -P3306 -t
   # 根据时间点进行分析
   shell> python binlog_trx_parse_rm.py -f mysql-bin.004595 -h 192.168.1.101 -uadmin -p123 -P3306 --start-datetime="2018-11-04 14:46:42"  --stop-datetime="2018-11-04 15:00:00" <br>
   shell> python binlog_trx_parse_rm.py -f mysql-bin.004595 -h 192.168.1.101 -uadmin -p123 -P3306 --start-datetime="2018-11-04 14:46:42"  --stop-datetime="2018-11-04 15:00:00" -d sh
   # 根据post点进行分析
   shell> shell> python binlog_trx_parse_rm.py -f mysql-bin.004595 -h 192.168.1.101 -uadmin -p123 -P3306  --start-position=6471  --stop-position=9995
