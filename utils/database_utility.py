
import os
import pymongo
from bson.objectid import ObjectId

from resource_utility import gen_nested_release

import fiona

# build path to extract_utility and add to sys.path
import sys
branch_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(branch_dir, 'geo-hpc/extract-scripts'))
sys.path.insert(0, os.path.join(os.path.dirname(branch_dir), 'geo-hpc/extract-scripts'))

from extract_utility import FeatureTool


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
            print "Updating release..."
            if existing != None:
                db_releases = self.client.releases
                db_releases.drop_collection(existing['name'])

            self.release_to_mongo(doc["name"], doc["base"])

        print "Updating core..."
        self.update_core(doc, search_name)

        if update != "partial":
            try:
                print "Updating trackers..."
                self.update_trackers(doc, search_name, existing)
            except Exception as e:
                print "Error updating trackers. Removing core entry..."
                self.c_asdf.delete_one({"name": search_name})
                raise e

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


            # update extracts
            # clear all existing extracts using boundary
            c_extracts = self.db_asdf.extracts
            delete_extracts = c_extracts.delete_many({
                'boundary': doc['name']
            })
            if delete_extracts.deleted_count > 0:
                print "Clearing extracts for existing boundary"


        elif doc["type"] != "boundary":

            # update trackers

            for bnd_group in db_trackers.collection_names():
                c_bnd = db_trackers[bnd_group]
                c_bnd.delete_one({"name": search_name})

            ###
            # 2017-01-20
            # commented below sections out since whether we clear out existing
            # msr/extract items depends on whether we are reingesting a
            # dataset due to some processing/data issue that will impact the
            # results of msr/extract vs something that does not warrant
            # completely rerunning all msr/extract
            #
            # if it is an issue with the data it should probably be treated as new
            # version of data. essentially: do not just delete all the msr/extract
            # unless it is absolutely necessary
            #
            # probably need to revisit this so we have a better way to handle
            # rather than commenting/uncommenting this block of code as needed
            ###


            # # update msr
            # # clear all existing msr using release
            # if (doc["type"] == "release" and
            #         "msr" in self.db_asdf.collection_names()):
            #     c_msr = self.db_asdf.msr
            #     delete_msr = c_msr.delete_many({
            #         'dataset': search_name,
            #         'status':0
            #     })
            #     if delete_msr.deleted_count > 0:
            #         print "Clearing unprocessed msr items for release dataset from msr queue"


            # # update extracts
            # # clear all existing extracts using dataset
            # if "extracts" in self.db_asdf.collection_names():
            #     c_extracts = self.db_asdf.extracts
            #     delete_extracts = c_extracts.delete_many({
            #         'data': {'$regex': r'^{0}'.format(search_name)}
            #     })
            #     if delete_extracts.deleted_count > 0:
            #         print "Clearing extracts for dataset"


        return 0


    def release_to_mongo(self, name=None, path=None):

        if name is None:
            quit("No name provided")

        if not os.path.isdir(path):
            quit("Invalid release directory provided.")

        db_releases = self.client.releases

        # db_releases.drop_collection(name)
        c_release = db_releases[name]

        # c_release.create_index([("locations.spatial", pymongo.GEOSPHERE)])

        release_generator = gen_nested_release(path)

        bulk_insert = c_release.initialize_unordered_bulk_op()

        for doc in release_generator:
            # add to collection
            # c_release.insert(doc)
            bulk_insert.insert(doc)


        bulk_result = bulk_insert.execute()
        print bulk_result

        return 0


    def features_to_mongo(self, bnd_name):
        """Add features for given boundary to feature collection
        """

        # lookup bnd_name and get path
        bnd_info = self.db_asdf.data.find_one({'name': bnd_name})
        if bnd_info is None:
            msg = "Could not find boundary matching name ({0})".format(
                bnd_name)
            raise Exception(msg)

        bnd_path = os.path.join(bnd_info['base'], bnd_info['resources'][0]['path'])

        # open boundary via fiona and get iterator/list
        feats = fiona.open(bnd_path, 'r')


        # initialize featuretool instance and run
        ftool = FeatureTool(client=self.client, bnd_name=bnd_name)
        run_data = ftool.run(feats, add_extract=False)

        return run_data




