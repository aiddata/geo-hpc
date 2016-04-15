
import sys
import os
import errno
import pymongo
import json
from distutils.version import StrictVersion

class ReleaseTools():
    """Tools for finding research releases.

    Attributes:

        all_releases (list): list of tuples for each release
        is_connected (bool): if connection to mongodb has already been
                             established
        asdf (mongo collection): mongodb asdf "data" collection


    Format of typle for all_releases is (name, version, path).
    If name or version cannot be found (outdated release) the format should
    be  (None, None, path).
    """
    def __init__(self):

        self.all_releases = []

        self.dst_dir = "/sciclone/aiddata10/REU/data/releases"

        try:
            os.makedirs(self.dst_dir)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        self.is_connected = False

        self.method = None


    def connect_mongo(self, branch):
        """Connect to mongo db. """

        branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

        if not os.path.isdir(branch_dir):
            raise Exception('Branch directory does not exist')


        config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
        sys.path.insert(0, config_dir)

        import config_utility

        config = config_utility.BranchConfig(branch=branch)

        # -------------------------------------

        # check mongodb connection
        if config.connection_status != 0:
            sys.exit("connection status error: " + str(config.connection_error))

        # -------------------------------------

        client = pymongo.MongoClient(config.server)

        self.asdf = client[config.asdf_db].data

        self.is_connected = True


    def set_asdf_releases(self, branch):
        """Set asdf releases based mongo db collection.

        Args:
            branch (str): active branch
        """
        if not self.is_connected:
            self.connect_mongo(branch)

        # get names of all research releases from asdf
        self.all_releases = [
            (i['name'], i['version'], i['base'])
            for i in self.asdf.find({'type':'release', 'active': 1},
                                    {'name':1, 'version':1, 'base':1})
        ]

        self.method = "asdf"


    def set_dir_releases(self, src_dir):
        """Set asdf releases based directory contents.

        Args:
            src_dir (str): directory containing release folders
        """
        self.all_releases = []
        for i in os.listdir(src_dir):

            tmp_dir =  os.path.join(src_dir, i)
            tmp_dp_path = os.path.join(src_dir, i, "datapackage.json")

            try:
                tmp_dp = json.load(open(tmp_dp_path, 'r'))
                tmp_item = (tmp_dp["name"], tmp_dp["version"], tmp_dir)

            except:
                tmp_item = (None, None, tmp_dir))

            self.all_releases.append(tmp_item)


        self.method = "dir"


    def set_user_releases(self, user_list):
        """Set asdf releases based on user list of releases.

        Args:
            user_list (list): release names
        """
        self.all_releases = user_list

        self.method = "user"


    def get_latest_releases(self):
        """Get latest releases from all release names

        Returns:
            latest_releases (list): latest release names
        """
        valid_releases = [i for i in self.all_releases if not None in i]

        # preambles from name which identify country/group data pertains to
        all_preambles = [i[0].split('_')[0] for i in valid_releases]

        # duplicates based on matching preambles
        # means there are multiple versions
        duplicate_preambles = [
            i for i in set(all_preambles)
            if all_preambles.count(i) > 1
        ]

        # unique list of latest dataset names
        # initialized here with only datasets that have a single version
        latest_releases = [
            i for i in valid_releases
            if not i[0].startswith(tuple(duplicate_preambles))
        ]

        # iterate over each group of conflicting datasets based on preamble
        for i in duplicate_preambles:

            # get full names using preamble
            conflict_releases = [k for k in valid_releases if k[0].startswith(i)]

            latest_version = None

            # find which dataset is latest version
            for j in conflict_releases:
                tmp_version = str(j[1])

                if latest_version == None:
                    latest_version = tmp_version

                elif StrictVersion(tmp_version) > StrictVersion(latest_version):
                    latest_version = tmp_version


            # add latest version dataset to final list
            latest_releases += [
                j for j in conflict_releases
                if str(j[1]) == latest_version
            ]


        return latest_releases


