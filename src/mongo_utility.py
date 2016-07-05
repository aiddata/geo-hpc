
import os
import pymongo
import pandas as pd


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


    def update_core(self, doc):
        """Update main data collection (db:asdf, collection:data).

        Args:
            doc (Dict): data for dataset to be added to
                        main asdf data collection
        Returns:
            0 - success
            1 - error
        """
        try:
            self.c_asdf.replace_one({"base": doc["base"]},
                                    doc,
                                    upsert=True)
            return 0
        except:
            return 1


    def update_trackers(self, doc, new_boundary=0,
                        update_geometry=0, update_data=0):
        """Update boundary tracker(s).

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
            new_boundary (bool): indicates if this is a new boundary
                                 being created
            update_geometry (bool): indicates if geometry for dataset needs
                                    to be updated
            update_data (bool): indicates whether this is being run as
                                part of (meta) data update only, in
                                which case, do not modify the tracker information
        Returns:
            0 on succesful update
        """
        db_trackers = self.client.trackers

        if (doc["type"] == "boundary" and
                doc["options"]["group_class"] == "actual"):

            # drop boundary tracker if geometry has changed
            if update_geometry:
                print "update existing boundary with new geom"
                db_trackers.drop_collection(doc["options"]["group"])


            existing_groups = db_trackers.collection_names()
            new_tracker = (new_boundary or update_geometry or
                           doc["options"]["group"] not in existing_groups)

            if new tracker:

                # if dataset is boundary and a group actual
                # create new boundary tracker collection
                c_bnd = self.db_asdf[doc["options"]["group"]]
                c_bnd.create_index("name", unique=True)
                # c_bnd.create_index("base", unique=True)
                c_bnd.create_index([("spatial", pymongo.GEOSPHERE)])

                # add each non-boundary dataset item to new
                # boundary collection with "unprocessed" flag
                dsets = self.c_asdf.find({"type": {"$ne": "boundary"}})
                for full_dset in dsets:
                    dset = {
                        'name': full_dset["name"],
                        'spatial': full_dset["spatial"],
                        'scale': full_dset["scale"],
                        'status': -1
                    }
                    c_bnd.insert(dset)

        elif doc["type"] != "boundary":
            # if dataset is not boundary

            if update_geometry or not update_data:
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
                    c_bnd.replace_one({"name": dset["name"]},
                                       dset,
                                       upsert=True)


        return 0


    def release_to_mongo_wrapper(name=None, path=None):

        if name is None:
            quit("No name provided")

        if path is None:
            quit("No release directory provided")

        if not os.path.isdir(path):
            quit("Invalid release directory provided.")

        db_releases = self.client.releases

        db_releases.drop_collection(name)
        c_release = db_releases[name]

        release_generator = release_to_mongo(path, c_release)

        for doc in release_generator:
            # add to collection
            c_release.insert(doc)

        return 0


    def release_to_mongo(path=None):
        """Yield nested project dicts for geocoded research releases.

        Convert flat tables from release into nested dicts which can be
        inserted into a mongodb collection.

        Args:
            path (str): path to root directory of Level 1 geocoded
                        research release
        """
        if not os.path.isdir(path):
            quit("Invalid release directory provided.")


        files = ["projects", "locations", 'transactions']

        tables = {}
        for table in files:
            file_path = path+"/data/"+table+".csv"

            if not os.path.isfile(file_path):
                raise Exception("no valid table type found for: " + file_path)

            tables[table] = pd.read_csv(file_path, sep=',', quotechar='\"')
            tables[table]["project_id"] = tables[table]["project_id"].astype(str)


        # add new data for each project
        for project_row in tables['projects'].iterrows():

            project = dict(project_row[1])
            project_id = project["project_id"]

            transaction_match = tables['transactions'].loc[
                tables['transactions']["project_id"] == project_id]

            if len(transaction_match) > 0:
                project["transactions"] = [dict(x[1])
                                           for x in transaction_match.iterrows()]

            else:
                print "No transactions found for project id: " + str(project_id)


            location_match = tables['locations'].loc[
                tables['locations']["project_id"] == project_id]

            if len(location_match) > 0:
                project["locations"] = [dict(x[1])
                                        for x in location_match.iterrows()]

            else:
                print "No locations found for project id: " + str(project_id)


            yield project


