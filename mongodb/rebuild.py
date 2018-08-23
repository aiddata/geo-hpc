

import json
import pymongo
from bson.objectid import ObjectId
from bson import Int64

client = pymongo.MongoClient("128.239.20.76")
c_det = client.asdf.det

branc = "master"
id_list = [
    # "5ac39c01c15e003de25a9fce"
]


for request_id in id_list:
    path = "/sciclone/aiddata10/geo/{0}/outputs/det/results/{1}/request_details.json".format(master, request_id)
    request = json.load(open(path))
    request["status"] = Int64(1)
    request["stage"][3]["time"] = 1522850400
    request["_id"] = ObjectId(request["_id"])
    for ix in range(len(request['release_data'])):
        del request['release_data'][ix]['$$hashKey']
    for ix in range(len(request['raster_data'])):
        del request['raster_data'][ix]['$$hashKey']
        for ixf in range(len(request['raster_data'][ix]['files'])):
             del request['raster_data'][ix]['files'][ixf]['$$hashKey']
    c_det.insert(request)


# from pprint import pprint
# pprint(request)
