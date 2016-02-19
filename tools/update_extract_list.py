
# update asdf/det extract tracker with all possible extract
# combinations available from the asdf


import time
import pymongo


# connect to mongodb
client = pymongo.MongoClient()
asdf = client.asdf.data
extracts = client['det-test'].extracts


# lookup all boundary datasets
boundaries = asdf.find({"type": "boundary"})

# get boundary names
bnds = [b['resources'][0]['name'] for b in boundaries]

# lookup all raster datasets
rasters = asdf.find({"type": "raster"})


items = []

# build list of dicts for all combinations of boundary names, 
# rasters names/reliabiity and respective raster extract types
for raster in rasters:

    extract_types = raster['options']['extract_types']

    items += [
        {
            'boundary': b, 
            'raster': r['name'], 
            'reliability': r['reliability'], 
            'extract_type': e
        } 
        for r in raster['resources'] 
        for e in extract_types
        for b in bnds
    ]


# check if unique extract combinations exist in tracker 
# and add if they do not
add_count = 0
for i in items:

    # check if it exists in extracts queue
    exists = extracts.find(i)

    if exists.count() == 0:
        
        add_count += 1

        ctime = int(time.time())

        i["status"] = 0
        i["classification"] = "auto-external"
        i["priority"] = -1

        i["submit_time"] = ctime
        i["update_time"] = ctime

        # add to extracts queue
        extracts.insert(i)


print 'Added ' + str(add_count) + ' items to extract queue (' + str(len(items)) + ' total possible).'



# example extract tracker document

# { 
#     "_id" : ObjectId("566baebf6050d566eca1f25d"), 
    
#     "boundary" : "npl_adm3", 

#     "raster" : "selv", 
#     "extract_type" : "mean", 
#     "reliability" : false,

#     "status" : 0, 
#     "classification" : "automated", 
#     "priority" : -1

#     "submit_time" : 1449897663, 
#     "update_time" : 1450383510, 
# }
