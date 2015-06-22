from copy import deepcopy
import pymongo


# update database(s)
class update_mongo():
    

    # loc is 2dsphere spatial index
    # > db.data.createIndex( { loc : "2dsphere" } )
    # path is unique index
    # > db.data.createIndex( { path : 1 }, { unique: 1 } )
    # name is unique index
    # > db.data.createIndex( { name : 1 }, { unique: 1 } )



    def init(self):
        # connect to mongodb
        self.client = pymongo.MongoClient()
        self.db = self.client.daf
        self.c_data = self.db.data
        self.c_ckan = self.db.ckan



    def update_core(self, col, data):

        # insert 
        try:
            c_data.insert(data)
        except pymongo.errors.DuplicateKeyError, e:
            print e
            quit("Dataset with same name or path exists.")


        # check insert and notify user
        vp = c_data.find({"path": in_path})
        vn = c_data.find({"name": in_name})

        if vp.count() < 1 or vn.count() < 1:
            quit( "Error - No items with name or path found in database.")
        elif vp.count() > 1 or vn.count() > 1:
            quit( "Error - Multiple items with name or path found in database.")
        else:
            print "Success - Item successfully inserted into database.\n"



    # update/create boundary tracker(s)
    # *** add error handling for all inserts (above and below) ***
    # *** remove previous inserts if later insert fails, etc. ***

    def update_trackers(self, type):

        if in_type == "boundary":
            # if dataset is boundary
            # create new boundary tracker collection
            # each each non-boundary dataset item to new boundary collection with "unprocessed" flag
            dsets =  c_data.find({"type": {"$ne": "boundary"}})
            c_bnd = db[in_name]
            c_bnd.create_index("name", unique=True)
            for dset in dsets:
                dset['status'] = -1
                c_bnd.insert(dset)

        else:
            # if dataset is not boundary
            # add dataset to each boundary collection with "unprocessed" flag
            bnds = c_data.find({"type": "boundary"},{"name": 1})
            dset = deepcopy(data)
            dset['status'] = -1
            for bnd in bnds:
                c_bnd = db[bnd['name']]
                c_bnd.insert(dset)



    # update ckan mongodb (contains all existing datapackage.json as documents)
    def update_ckan(self):
        print "ckan"
