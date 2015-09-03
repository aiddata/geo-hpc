# generic local merge script for use with sciclone extracts


import sys
import os
import time

import pandas as pd


bnd_name = sys.argv[1]

data_name = sys.argv[2]

extract_type = sys.argv[3]

output = sys.argv[4]


# different method for listing years to ignore/accept
# comment / uncomment "ignore = ..." lines as needed
# always use 4 digit integers to specify years

# specify ignore
ignore = []

# ignore range
# ignore = range(1900, 1982)

# specify accept by using exceptions in ignore range 
# (manually adjust range if years fall outside of 1800-2100)
accept = []
# ignore = [i in range(1800, 2100) if i not in accept]


# convert years to strings
ignore = [str(e) for e in ignore]


extract_base = "/sciclone/aiddata10/REU/extracts/"

extract_dir = extract_base + "/" + bnd_name + "/cache/" + data_name +"/"+ extract_type 


# validate inputs by checking directories exist
if not os.path.isdir(extract_base):
    sys.exit("Directory for extracts does not exist. You may not be connected to sciclone ("+extract_base+")")
elif not os.path.isdir(extract_base + "/" + bnd_name):
    sys.exit("Directory for specified bnd_name does not exist (bnd_name: "+bnd_name+")")
elif not os.path.isdir(extract_base + "/" + bnd_name + "/cache/" + data_name):
    sys.exit("Directory for specified dataset does not exists (data_name: "+data_name+")")
elif not os.path.isdir(extract_dir):
    sys.exit("Directory for specified extract type does not exist (extract_type: "+extract_type+")")
elif not os.path.isdir(os.path.dirname(output)):
    sys.exit("Directory for output file does not exists ("+output+")")
elif not output.endswith(".csv") or "" in os.path.splitext(os.path.basename(output)):
    sys.exit("Output must include file name with .csv extension")


Ts = int(time.time())


# find and sort all relevant extract files
rlist = [fname for fname in os.listdir(extract_dir) if fname[5:9] not in ignore and os.path.isfile(extract_dir +"/"+ fname) and fname.endswith(".csv")]
rlist = sorted(rlist)


# exit if no extracts found
if len(rlist) == 0:
    sys.exit("No extracts found")


print "Merging extracts..."

merge = 0

for item in rlist:

    result_csv = extract_dir +"/"+ item
    
    result_df = pd.read_csv(result_csv, quotechar='\"', na_values='', keep_default_na=False)

    if not isinstance(merge, pd.DataFrame):
        merge = result_df.copy(deep=True)
        merge.rename(columns={"ad_extract": item[:-4]}, inplace=True)

    else:
        merge[item[:-4]] = result_df["ad_extract"]

merge.to_csv(output, index=False)


print 'Merge completed for ' + bnd_name +', '+ data_name +', '+ extract_type
print 'Results output to ' + output

T_run = int(time.time() - Ts)
print 'Merge Runtime: ' + str(T_run//60) +'m '+ str(int(T_run%60)) +'s'
