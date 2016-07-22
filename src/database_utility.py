
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
            try:
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


            # update msr
            # clear all existing msr using release
            if (doc["type"] == "release" and
                    "msr" in self.db_asdf.collection_names()):
                c_msr = self.db_asdf.msr
                delete_msr = c_msr.delete_many({
                    'dataset': search_name
                })
                if delete_msr.deleted_count > 0:
                    print "Clearing msr for boundary"


            # update extracts
            # clear all existing extracts using dataset
            if "extracts" in self.db_asdf.collection_names():
                c_extracts = self.db_asdf.extracts
                delete_extracts = c_extracts.delete_many({
                    'raster': {'$regex': r'^{0}'.format(search_name)}
                })
                if delete_extracts.deleted_count > 0:
                    print "Clearing extracts for dataset"


        return 0


    def release_to_mongo(self, name=None, path=None):

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


    def features_to_mongo(self):
        # open boundary via fiona and get iterator/list
        # initialize feature class
        # run feature class
        pass












import json
import hashlib

# from shapely.geometry import shape


def json_sha1_hash(hash_obj):
    hash_json = json.dumps(hash_obj,
                           sort_keys = True,
                           ensure_ascii=True,
                           separators=(', ',': '))
    hash_builder = hashlib.sha1()
    hash_builder.update(hash_json)
    hash_sha1 = hash_builder.hexdigest()
    return hash_sha1


class FeatureExtractTool():
    """
    """

    def __init__(self, c_features, bnd_name, data_name=None,
                 ex_method=None, classification=None, ex_version=None):

        self.c_features = c_features
        self.bnd_name = bnd_name

        self.data_name = data_name
        self.ex_method = ex_method
        self.classification = classification
        self.ex_version = ex_version


    def set_extract_fields(self, data_name, ex_method,
                           classification, ex_version):
        """set extract fields

        used to set extract fields if not set during class instance init
        """
        self.data_name = data_name
        self.ex_method = ex_method
        self.classification = classification
        self.ex_version = ex_version


    def has_extract_fields(self):
        """check if extract fields are set

        returns true if none of the extract fields are set to None
        """
        extract_fields = [
            self.data_name,
            self.ex_method,
            self.classification,
            self.ex_version
        ]

        valid = None not in extract_fields
        return valid


    def build_extract_object(self):
        """
        """
        if self.ex_method == 'reliability' :
            ex_value = {
                'sum': feat['properties']['exfield_sum'],
                'reliability': feat['properties']['exfield_reliability'],
                'potential': feat['properties']['exfield_potential']
            }
        else:
            ex_value = feat['properties']['exfield_' + self.ex_method]


        temporal = 'na'
        dataset = self.data_name
        if self.classification == "msr":
            dataset = self.data_name[:self.data_name.rindex('_')]

        elif '_' in self.data_name:
            tmp_temp = self.data_name[self.data_name.rindex('_')+1:]
            if tmp_temp.isdigit():
                temporal = tmp_temp
                dataset = self.data_name[:self.data_name.rindex('_')]


        feature_extracts = [{
            'data': self.data_name,
            'dataset': dataset,
            'temporal': temporal,
            'method': self.ex_method,
            'classification': self.classification,
            'version': self.ex_version,
            'value': ex_value
        }]

        return feature_extracts


    def run(self, run_data, add_extract=False):
        """
        """
        if add_extract and not self.has_extract_fields:
            raise Exception('extract fields not set')


        # update extract result database
        for idx, feat in enumerate(run_data):
            geom = feat['geometry']
            geom_hash = json_sha1_hash(geom)

            # feature_id = idx

            feature_properties = {
                key: feat['properties'][key]
                for key in feat['properties']
                if not key.startswith('exfield_')
            }


            feature_extracts = []
            if add_extract:
                feature_extracts = self.build_extract_object()


            # check if geom / geom hash exists
            search = self.c_features.find_one({'hash': geom_hash})


            exists = search is not None
            if exists and add_extract:

                extract_search_params = {
                    'hash': geom_hash,
                    'extracts.data': self.data_name,
                    'extracts.method': self.ex_method,
                    'extracts.version': self.ex_version
                }

                extract_search = self.c_features.find_one(extract_search_params)
                extract_exists = extract_search is not None

                if extract_exists:
                    search_params = extract_search_params
                    update_params = {
                        '$set': {'extracts.$': feature_extracts[0]}
                    }

                else:
                    search_params = {'hash': geom_hash}
                    update_params = {
                        '$push': {'extracts': {'$each': feature_extracts}}
                    }


                if not self.bnd_name in search['datasets']:
                    # add dataset to datasets
                    if not '$push' in update_params:
                        update_params['$push'] = {}
                    if not '$set' in update_params:
                        update_params['$set'] = {}

                    update_params['$push']['datasets'] = self.bnd_name

                    prop_sub_doc = 'properties.' + self.bnd_name
                    update_params['$set'][prop_sub_doc] = feature_properties


                update = self.c_features.update_one(search_params, update_params)


            else:
                # simplified_geom = shape(geom).simplify(0.01)

                feature_insert = {
                    'geometry': geom,
                    # 'simplified': simplified_geom,
                    'hash': geom_hash,
                    # 'id': feature_id,
                    'properties': {self.bnd_name: feature_properties},
                    'datasets': [self.bnd_name],
                    'extracts': feature_extracts
                }
                # insert
                insert = self.c_features.insert(feature_insert)


            if add_extract:
                yield feat
