
import os
import pymongo

# import pandas as pd
# from copy import deepcopy


# existing core database indexes:
# 
# spatial is 2dsphere spatial index
# > db.data.createIndex( { spatial : "2dsphere" } )
# path is unique index
# > db.data.createIndex( { base : 1 }, { unique: 1 } )
# name is unique index
# > db.data.createIndex( { name : 1 }, { unique: 1 } )
class MongoUpdate():
    """Update MongoDB collection(s)

    Attributes:

        client : MongoDB client connection
        asdf : "asdf" Mongo database
        c_data : "data" collection of "asdf" db
        releases : "releases" Mongo database

    """
    def __init__(self):
        # connect to mongodb
        self.client = pymongo.MongoClient()
        self.asdf = self.client.asdf

        if not "data" in self.asdf.collection_names():
            self.c_data = self.asdf.data

            self.c_data.create_index("base", unique=True)
            self.c_data.create_index("name", unique=True)
            self.c_data.create_index([("spatial", pymongo.GEOSPHERE)])

        else:
            self.c_data = self.asdf.data
        

        self.releases = self.client.releases

        # self.c_tmp = self.asdf.tmp


    def update_core(self, in_data):
        """Update main data collection (db:asdf, collection:data).
        
        Args:
            in_data (Dict): data for dataset to be added to main asdf data collection
        Returns:
            0 - success
            1 - error
        """
        print "update_core"
        # self.c_data.replace_one({"base": in_data["base"]}, in_data, upsert=True)
        try:
            self.c_data.replace_one({"base": in_data["base"]}, in_data, upsert=True)
            print "update_core: good"
            return 0
        except:
            print "update_core: bad"
            return 1


    # to do:
    # *** add error handling for all inserts (above and below) ***
    # *** remove previous inserts if later insert fails, etc. ***
    # - remove core insert if tracker fails
    # - only insert partial of core document 
    #       - essential identifying info only: name, path, type, others?
    #       - no meta info in trackers so we do not have to update them if meta changes
    def update_trackers(self, in_data, new_boundary=0, update_geometry=0, update_data=0):
        """Update boundary tracker(s).

        Args:
            in_data (Dict): contains dataset information. varies by dataset type
            new_boundary (bool): indicates if this is a new boundary being created
            update_geometry (bool): indicates if geometry for dataset needs to be updated
            update_data (bool): indicates whether this is being run as part of (meta) data 
                                update only, in which case, do not modify the tracker information
        Returns:
            0 on succesful update
        """
        if in_data["type"] == "boundary" and in_data["options"]["group_class"] == "actual":

            # drop boundary tracker if geometry has changed
            if update_geometry:
                print "update existing boundary with new geom"
                self.asdf.drop_collection(in_data["options"]["group"])


            if new_boundary or update_geometry or in_data["options"]["group"] not in self.asdf.collection_names():
                # if dataset is boundary and a group actual
                # create new boundary tracker collection
                c_bnd = self.asdf[in_data["options"]["group"]]
                c_bnd.create_index("name", unique=True)
                # c_bnd.create_index("base", unique=True)
                c_bnd.create_index([("spatial", pymongo.GEOSPHERE)])

                # add each non-boundary dataset item to new boundary collection with "unprocessed" flag
                dsets = self.c_data.find({"type": {"$ne": "boundary"}})
                for full_dset in dsets:
                    dset = {
                        'name': full_dset["name"],
                        'spatial': full_dset["spatial"],
                        'scale': full_dset["scale"],
                        'status': -1
                    }
                    c_bnd.insert(dset)

        elif in_data["type"] != "boundary":
            # if dataset is not boundary

            if update_geometry or not update_data:
                # add dataset to each boundary collection with "unprocessed" flag
                
                dset = {
                    'name': in_data["name"],
                    'spatial': in_data["spatial"],
                    'scale': in_data["scale"],
                    'status': -1
                }

                bnds = self.c_data.find({"type": "boundary", "options.group_class": "actual"}, {"options": 1})
                for bnd in bnds:
                    c_bnd = self.asdf[bnd["options"]["group"]]
                    # c_bnd.insert(dset)
                    c_bnd.replace_one({"name": dset["name"]}, dset, upsert=True)


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


