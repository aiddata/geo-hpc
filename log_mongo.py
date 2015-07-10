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

        if not "data" in self.db.collection_names():
            self.c_data = self.db.data

            self.c_data.create_index("base", unique=True)
            self.c_data.create_index("name", unique=True)
            self.c_data.create_index("spatial", pymongo.GEOSPHERE)

        else:
            self.c_data = self.db.data
        

        # self.c_tmp = self.db.tmp


    # update main database 
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



    # update/create boundary tracker(s)
    # *** add error handling for all inserts (above and below) ***
    # *** remove previous inserts if later insert fails, etc. ***
    def update_trackers(self, in_data, new_boundary=0, update_boundary=0):

        if in_data["type"] == "boundary" and in_data["options"]["group_class"] == "actual":

            # drop boundary tracker if geometry has changed
            if update_boundary:
                print "update existing boundary with new geom"
                self.db.drop_collection(in_data["group"])


            if new_boundary or update_boundary:
                # if dataset is boundary and a group actual
                # create new boundary tracker collection
                c_bnd = db[in_data["group"]]
                c_bnd.create_index("name", unique=True)
                c_bnd.create_index("base", unique=True)

                # add each non-boundary dataset item to new boundary collection with "unprocessed" flag
                dsets =  c_data.find({"type": {"$ne": "boundary"}})
                for dset in dsets:
                    dset['status'] = -1
                    c_bnd.insert(dset)

        elif in_data["type"] != "boundary":
            # if dataset is not boundary
            # add dataset to each boundary collection with "unprocessed" flag
            dset = deepcopy(in_data)
            dset['status'] = -1
            bnds = c_data.find({"type": "boundary", "options.group_class": "actual"}, {"options": 1})
            for bnd in bnds:
                c_bnd = db[bnd["options"]["group"]]
                # c_bnd.insert(dset)
                c_bnd.replace_one({"base": dset["base"]}, dset, upsert=True)


        return 0




    # def update_core(self, in_data):

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



