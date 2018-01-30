# coding=utf-8

import sys
reload(sys)
sys.setdefaultencoding('utf8')

import os

file_path = '/tmp/codegen'

def init(d,t):
    global file_path
    file_path = file_path+'/'+d+'/'+t
    dirExists = os.path.exists(file_path)
    if not dirExists:
        os.makedirs(file_path)
