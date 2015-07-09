from copy import deepcopy
import pymongo


# update database(s)
class update_mongo():
    
    # existing core database indexes:
    # 
    # spatial is 2dsphere spatial index
    # > db.data.createIndex( { spatial : "2dsphere" } )
    # path is unique index
    # > db.data.createIndex( { base : 1 }, { unique: 1 } )
    # name is unique index
    # > db.data.createIndex( { name : 1 }, { unique: 1 } )


    def __init__(self):
        # connect to mongodb
        self.client = pymongo.MongoClient()
        self.db = self.client.daf
        self.c_data = self.db.data
        # self.c_tmp = self.db.tmp


    def update_core(self, in_data):
        print "update_core"
        # self.c_data.replace_one({"base": in_data["base"]}, in_data, upsert=True)
        try:
            self.c_data.replace_one({"base": in_data["base"]}, in_data, upsert=True)
            print "update_core: good"
            return 0
        except:
            print "update_core: bad"
            return 1



        # vb = c_data.find({"base": in_data["base"]})
        # update = False
        # if vb.count() > 0:
        #     update = True


        # if update:
            
        #     try:
        #         # update (replace) 
        #         c_data.update()

        #     except:
        #         quit("Error updating.")
 

        # else:

        #     try:
        #         # insert 
        #         c_data.insert(in_data)

        #     except pymongo.errors.DuplicateKeyError, e:
        #         print e
        #         quit("Error inserting - Dataset with same name or path exists.")




        # # check insert and notify user

        # vn = c_data.find({"name": in_data["name"]})

        # if vb.count() < 1 or vn.count() < 1:
        #     # quit( "Error - No items with name or path found in database.")
        #     return 1
        # elif vb.count() > 1 or vn.count() > 1:
        #     # quit( "Error - Multiple items with name or path found in database.")
        #     return 2
        # else:
        #     # print "Success - Item successfully inserted into database.\n"
        #     return 0


    # update/create boundary tracker(s)
    # *** add error handling for all inserts (above and below) ***
    # *** remove previous inserts if later insert fails, etc. ***
    def update_trackers(self, in_data):

        if in_type == "boundary":
            # if dataset is boundary
            # create new boundary tracker collection
            c_bnd = db[in_data["name"]]
            c_bnd.create_index("name", unique=True)

            # add each non-boundary dataset item to new boundary collection with "unprocessed" flag
            dsets =  c_data.find({"type": {"$ne": "boundary"}})
            for dset in dsets:
                dset['status'] = -1
                c_bnd.insert(dset)

        else:
            # if dataset is not boundary
            # add dataset to each boundary collection with "unprocessed" flag
            dset = deepcopy(in_data)
            dset['status'] = -1
            bnds = c_data.find({"type": "boundary"},{"name": 1})
            for bnd in bnds:
                c_bnd = db[bnd['name']]
                c_bnd.insert(dset)


