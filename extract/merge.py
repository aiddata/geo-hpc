# generic merge for sciclone extract jobs


import sys
import os
import json
import time

from collections import OrderedDict

from extract_utility import *



input_json_path = sys.argv[1]

if not os.path.isfile(input_json_path):
    sys.exit("merge.py has terminated : invalid json path")

input_json_path = os.path.abspath(input_json_path)

input_file = open(input_json_path, 'r')
input_json = json.load(input_file, object_pairs_hook=OrderedDict)
input_file.close()



Ts = int(time.time())
T_start = time.localtime()
print 'Merge Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', T_start)


merge_obj = MergeObject(input_json, os.path.dirname(input_json_path), interface=True)
merge_obj.build_merges()


# stop job timer
T_run = int(time.time() - Ts)
T_end = time.localtime()
print '\n\n'
print 'Merge Start: ' + time.strftime('%Y-%m-%d  %H:%M:%S', T_start)
print 'Merge End: '+ time.strftime('%Y-%m-%d  %H:%M:%S', T_end)
print 'Merge Runtime: ' + str(T_run//60) +'m '+ str(int(T_run%60)) +'s'

