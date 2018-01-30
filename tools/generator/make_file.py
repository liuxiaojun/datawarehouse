# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import pymysql
from helper import change_file_type


file_path = '/tmp/codegen'

def echo_content(h,u,p,d,t,m):
    conn = pymysql.connect(host=h,user=u,passwd=p,db=d,charset='utf8',port=3306)
    cursor = conn.cursor()
    cursor.execute ("SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';" %(d,t))
    row = cursor.fetchall()
    create_table_content(row, d, t, m)
    ods_content(row, h, u, p, d, t, m)
    dwd_content(row, h, d, t, m)
    dwb_content(row, h, d, t, m)
    cursor.close()
    conn.close()

def create_table_content(row, d, t, m):
    print 'ods_content'
    global file_path
    with open(file_path+'/'+d+'/'+'/'+t+'/'+d+'_'+t+'.sql', 'w') as f:
        ### ods
        f.write("DROP table IF EXISTS ods.ods_%s_%s_%s_d;\n" %(d,t,m))
        f.write("CREATE table IF NOT EXISTS ods.ods_%s_%s_%s_d(\n" %(d,t,m))
        for line in sorted(row, reverse = False):
            f.write("    %s %s COMMENT '%s'," %(line[1], change_file_type(line[2]), line[3]))
            f.write("\n")
        f.write(")\n")
        f.write("PARTITIONED BY (dt string, dc string, tz string)\n")
        f.write("ROW FORMAT DELIMITED\n")
        f.write(r"    FIELDS TERMINATED BY '\t'")
        f.write("\n")
        f.write(r"    LINES TERMINATED BY '\n'")
        f.write("\n\n\n")
        
        ###dwd
        f.write("DROP table IF EXISTS dwd.etl_%s_%s_%s_d;\n" %(d,t,m))
        f.write("CREATE table IF NOT EXISTS dwd.etl_%s_%s_%s_d(\n" %(d,t,m))
        for line in sorted(row, reverse = False):
            f.write("    %s %s COMMENT '%s'," %(line[1], change_file_type(line[2]), line[3]))
            f.write("\n")
        f.write(")\n")
        f.write("PARTITIONED BY (dt string, dc string, tz string)\n")
        f.write("ROW FORMAT DELIMITED\n")
        f.write(r"    FIELDS TERMINATED BY '\t'")
        f.write("\n")
        f.write(r"    LINES TERMINATED BY '\n'")
        f.write("\n")
        f.write("location '/user/hive/warehouse/ods.db/ods_%s_%s_%s_d'" %(d,t,m))
        f.write("\n\n\n")

        ###dwb
        f.write("DROP table IF EXISTS dwb.fact_%s_%s_%s_d;\n" %(d,t,m))
        f.write("CREATE table IF NOT EXISTS dwb.fact_%s_%s_%s_d(\n" %(d,t,m))
        for line in sorted(row, reverse = False):
            f.write("    %s %s COMMENT '%s'," %(line[1], change_file_type(line[2]), line[3]))
            f.write("\n")
        f.write("    dc string\n")
        f.write(")\n")
        f.write("PARTITIONED BY (dt string, tz string)\n")
        f.write("ROW FORMAT DELIMITED\n")
        f.write(r"    FIELDS TERMINATED BY '\t'")
        f.write("\n")
        f.write(r"    LINES TERMINATED BY '\n'")
        f.write("\n")

def ods_content(row, h, u, p, d, t, m):
    global file_path
    with open(file_path+'/'+d+'/'+'/'+t+'/ods_'+d+'_'+t+'_'+m+'_d.sh', 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("for args in $@\n")
        f.write("do\n")
        f.write("    if [ ${args%=*} == 'bizdate' ]; then\n")
        f.write("      bizdate=${args#*=};\n")
        f.write("    fi\n")
        f.write("done\n")
        f.write("\n")
        f.write('date=`date -d "$bizdate" +"%Y-%m-%d"`\n')
        f.write('p_date=`date -d "$bizdate" +"%Y%m%d"`\n')
        f.write("b_date=`date -d \"$bizdate\" +\"%Y-%m-%d 00:00:00\"` \n")
        f.write("a_date=`date -d \"$bizdate 1 day\" +\"%Y-%m-%d 00:00:00\"` \n")
        f.write("\n")
        f.write("timezone='+0800'\n")
        f.write("datacenter='beijing'\n")
        f.write("hive2service='bigdata.hdp.mobike.cn:10001'\n")
        f.write("\n")
        f.write("sql=\"SELECT \n")
        for line in sorted(row, reverse = False):
            f.write("%s, " %line[1])
        f.write(" FROM %s " %t)

        if m == 'a':
            f.write(" WHERE \$CONDITIONS;\" \n\n")
        else:
            f.write(" WHERE create_time>='${b_date}' and create_time<'${a_date}'  and \$CONDITIONS;\"  \n\n")

        f.write("sqoop import \ \n")
        f.write("--connect jdbc:mysql://%s:3306/%s?tinyInt1isBit=false \  \n" %(h,d))
        f.write("--username %s  \ \n" %u)
        f.write("--password %s  \ \n" %p)
        f.write("--query \"$sql\" \ \n")
        f.write("--target-dir /user/hive/warehouse/ods.db/ods_%s_%s_%s_d/dt=${p_date}/dc=${datacenter}/tz=${timezone} \ \n" %(d,t,m)) 
        f.write("--num-mappers 1 \ \n")
        f.write(r"--null-string '\\N'  --null-non-string '\\N'  \ ")
        f.write("\n")
        f.write("--hive-overwrite  \ \n")
        f.write("--delete-target-dir  \ \n")
        f.write(r"--hive-drop-import-delims --fields-terminated-by '\t' ")
        f.write("\n\n")        


        f.write("`beeline -u jdbc:hive2://${hive2service} -n hdp-dw -e \"ALTER TABLE ods.ods_%s_%s_%s_d ADD IF NOT EXISTS PARTITION (dt='$bizdate',dc='$datacenter',tz='$timezone')\"`" %(d,t,m))
        f.write("\n ")

def dwd_content(row, h, d, t, m):
    with open(file_path+'/'+d+'/'+'/'+t+'/etl_'+d+'_'+t+'_'+m+'_d.sh', 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("for args in $@\n")
        f.write("do\n")
        f.write("    if [ ${args%=*} == 'bizdate' ]; then\n")
        f.write("      bizdate=${args#*=};\n")
        f.write("    fi\n")
        f.write("done\n")
        f.write("\n")
        f.write('date=`date -d "$bizdate" +"%Y-%m-%d"`\n')
        f.write('p_date=`date -d "$bizdate" +"%Y%m%d"`\n')
        f.write("timezone='+0800'\n")
        f.write("datacenter='beijing'\n")
        f.write("hive2service='bigdata.hdp.mobike.cn:10001'\n")
        f.write("\n")
        f.write("`beeline -u jdbc:hive2://${hive2service} -n hdp-dw -e \"ALTER TABLE dwd.etl_%s_%s_%s_d ADD IF NOT EXISTS PARTITION (dt='$p_date',dc='$datacenter',tz='$timezone')\"`" %(d,t,m))
        f.write("\n")

def dwb_content(row, h, d, t, m):
    with open(file_path+'/'+d+'/'+'/'+t+'/fact_'+d+'_'+t+'_'+m+'_d.sh', 'w') as f:
        f.write("set mapred.job.name = fact_%s_%s_%s_d;\n" %(d,t,m))
        f.write("set hive.exec.dynamic.partition = true;\n")
        f.write("set hive.exec.dynamic.partition.mode = nonstrict;\n")
        f.write("set hive.merge.mapfiles = true;\n")
        f.write("set hive.merge.mapredfiles = true;\n")
        f.write("set hive.merge.size.per.task = 256000000;\n")
        f.write("set hive.merge.smallfiles.avgsize = 16000000;\n")
        f.write("set hive.exec.max.dynamic.partitions = 2000;\n")
        f.write("\n")
        f.write("INSERT overwrite TABLE dwb.fact_%s_%s_%s_d partition(dt='${bizdate}',tz)\n" %(d,t,m))
        f.write("SELECT\n")
        for line in sorted(row, reverse = False):
            f.write(" %s," %line[1])
        f.write(" dc, ")
        f.write("tz\n")
        f.write("\n")  
        f.write("FROM dwd.etl_%s_%s_%s_d\n" %(d,t,m))
        f.write("WHERE dt='${bizdate}'; \n")
        f.write("\n")
       
