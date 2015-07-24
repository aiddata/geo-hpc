

import sys
import os
import pandas as pd
from copy import deepcopy


path_base = sys.argv[1]


# list of years to ignore/accept
# list of all [year, file] combos

ignore = []
years = [name for name in os.listdir(path_base+"/output") if os.path.isdir(os.path.join(path_base+"/output", name)) and name not in ignore]

# accept = ["1983","1984"]
# years = [ name for name in os.listdir(path_base+"/output") if os.path.isdir(os.path.join(path_base+"/output", name)) and name in accept]


rlist = [[year, day] for year in years for day in os.listdir(path_base+"/output/"+year) if os.path.isdir(os.path.join(path_base+"/output/"+year, day))]

rlist = sorted(rlist)


merge = 0

if len(rlist) > 0:

    for item in rlist:

        year = item[0]
        day = item[1]

        result_csv = path_base + "/output/" + year +"/"+ day + "/extract_" + year +"_"+ day + ".csv"

        result_df = pd.read_csv(result_csv, quotechar='\"', na_values='', keep_default_na=False)

        if not isinstance(merge, pd.DataFrame):
            merge = deepcopy(result_df)
            merge.rename(columns={"ad_extract": "ad_"+year +"_"+ day}, inplace=True)

        else:
            merge["ad_"+year] = result_df["ad_extract"]


    merge_output = path_base + "/extract_merge.csv"
    merge.to_csv(merge_output, index=False)

