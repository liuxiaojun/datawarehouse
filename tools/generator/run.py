# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

import getopt
import sys
import pymysql


from create_file import init 
from make_file import echo_content

h,u,p,d,t,m ='','','','','',''

def gorun():
    init(d,t)
    echo_content(h,u,p,d,t,m)

if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], 'h:u:p:d:t:m:', ['host=', 'user=', 'password=', 'database=', 'table=','method='])
    for key, value in opts:
        if key == '--help':
            print '代码生成器'
            print '参数：'
            print '-help\t显示帮助'
            print '-h\t数据库连接地址'
            print '-u\t数据库用户名'
            print '-p\t数据库密码'
            print '-d\t数据库名称'
            print '-t\t数据表名称'
            print '-m\t数据提取方式 i or a'
            sys.exit(0)

        if key in ['-h', '--host']:
            h = value
        if key in ['-u', '--user']:
            u = value
        if key in ['-p', '--password']:
            p = value
        if key in ['-d', '--database']:
            d = value
        if key in ['-t', '--table']:
            t = value
        if key in ['-m', '--methed']:
            m = value
    gorun()
