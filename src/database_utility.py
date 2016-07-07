
import os
import pymongo
from bson.objectid import ObjectId

from resource_utility import gen_nested_release


class MongoUpdate():
    """Update MongoDB collection(s)

    # existing core database indexes:
    #
    # spatial is 2dsphere spatial index
    # > db.data.createIndex( { spatial : "2dsphere" } )
    # path is unique index
    # > db.data.createIndex( { base : 1 }, { unique: 1 } )
    # name is unique index
    # > db.data.createIndex( { name : 1 }, { unique: 1 } )

    Attributes:

        client : MongoDB client connection
        db_asdf : "asdf" Mongo database
        c_asdf : "data" collection of "asdf" db

    """
    def __init__(self, client):
        # connect to mongodb
        self.client = client
        self.db_asdf = self.client.asdf

        if not "data" in self.db_asdf.collection_names():
            self.c_asdf = self.db_asdf.data

            self.c_asdf.create_index("base", unique=True)
            self.c_asdf.create_index("name", unique=True)
            self.c_asdf.create_index([("spatial", pymongo.GEOSPHERE)])

        else:
            self.c_asdf = self.db_asdf.data


    def update(self, doc, update=False, existing=None):

        if existing is not None:
            search_name = existing['name']
        else:
            search_name = doc['name']

        if doc["type"] == "release" and update != "partial":
            if existing != None:
                db_releases = self.client.releases
                db_releases.drop_collection(existing['name'])

            self.release_to_mongo(doc["name"], doc["base"])

        self.update_core(doc, search_name)

        if update != "partial":
            self.update_trackers(doc, search_name, existing)

        return 0


    def update_core(self, doc, search_name):
        """Update main data collection (db:asdf, collection:data).

        Args:
            doc (Dict): data for dataset to be added to
                        main asdf data collection
        Returns:
            0 - success
            1 - error
        """
        try:
            self.c_asdf.update_one({"name": search_name},
                                   {"$set": doc},
                                   upsert=True)
            return 0
        except:
            return 1


    def update_trackers(self, doc, search_name, existing=None):
        """Update boundary tracker(s) and related collections.

        to do:
        *** add error handling for all inserts (above and below) ***
        *** remove previous inserts if later insert fails, etc. ***
        - remove core insert if tracker fails
        - only insert partial of core document
              - essential identifying info only: name, path, type, others?
              - no meta info in trackers so we do not have to update them
                if meta changes

        Args:
            doc (Dict): contains dataset information. varies by
                        dataset type
        Returns:
            0 on succesful update
        """
        db_trackers = self.client.trackers

        if doc["type"] == "boundary":

            # update tracker
            if doc["options"]["group_class"] == "actual":
                new_group = doc["options"]["group"]

                existing_groups = db_trackers.collection_names()
                update_existing = new_group in existing_groups

                if update_existing:
                    print "Clearing tracker for existing boundary group"

                # drop existing boundary tracker collection
                # and create a new one (with proper indexes)
                db_trackers.drop_collection(new_group)

                # drop old group if needed
                if (existing is not None and
                        existing["options"]["group_class"] == "actual"):

                    db_trackers.drop_collection(existing["options"]["group"])


                c_bnd = db_trackers[new_group]
                c_bnd.create_index("name", unique=True)
                c_bnd.create_index([("spatial", pymongo.GEOSPHERE)])

                # add each non-boundary dataset item to new
                # tracker collection with "unprocessed" flag
                dsets = self.c_asdf.find({
                    "type": {"$ne": "boundary"},
                    "active": 1
                })
                for full_dset in dsets:
                    dset = {
                        'name': full_dset["name"],
                        'spatial': full_dset["spatial"],
                        'scale': full_dset["scale"],
                        'status': -1
                    }
                    c_bnd.update_one({"name": dset["name"]},
                                     {"$set": dset},
                                     upsert=True)

            # update extracts
            # clear all existing extracts using boundary
            c_extracts = self.db_asdf.extracts
            delete_extracts = c_extracts.remove_many({
                'boundary': doc['name']
            })
            if delete_extracts.deleted_count > 0:
                print "Clearing extracts for existing boundary"


        elif doc["type"] != "boundary":

            # update trackers

            # add dataset to each boundary collection
            # with "unprocessed" flag
            dset = {
                'name': doc["name"],
                'spatial': doc["spatial"],
                'scale': doc["scale"],
                'status': -1
            }

            bnds = self.c_asdf.find({
                "type": "boundary",
                "options.group_class": "actual"
            }, {"options": 1})

            for bnd in bnds:
                c_bnd = self.db_asdf[bnd["options"]["group"]]
                # c_bnd.insert(dset)
                c_bnd.update_one({"name": search_name},
                                 {"$set": dset},
                                 upsert=True)

            # update msr
            # clear all existing msr using release
            if doc["type"] == "release":
                c_msr = self.db_asdf.msr
                delete_msr = c_msr.remove_many({
                    'dataset': search_name
                })
                if delete_msr.deleted_count > 0:
                    print "Clearing msr for boundary"


            # update extracts
            # clear all existing extracts using dataset
            c_extracts = self.db_asdf.extracts
            delete_extracts = c_extracts.remove_many({
                'raster': {'$regex': r'^{0}'.format(search_name)}
            })
            if delete_extracts.deleted_count > 0:
                print "Clearing extracts for dataset"


        return 0


    def release_to_mongo(name=None, path=None):

        if name is None:
            quit("No name provided")

        if not os.path.isdir(path):
            quit("Invalid release directory provided.")

        db_releases = self.client.releases

        db_releases.drop_collection(name)
        c_release = db_releases[name]

        release_generator = gen_nested_release(path)

        for doc in release_generator:
            # add to collection
            c_release.insert(doc)

        return 0
