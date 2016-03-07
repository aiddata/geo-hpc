"""Contains configuration setting utilies.

BranchConfig (class): used to access setting from config json for specific branch
"""

import os
import json
import pymongo

class BranchConfig():
    """Get branch config settings from config json for specified branch.

    Attributes:

        branch (str): branch name
        valid_branches
        parent

        config_json
        branch_settings
        branch_keys
        connection_status

        **settings
    """

    def __init__(self, branch=None):
        self.branch = None
        self.valid_branches = ['master', 'develop']
        self.parent = os.path.dirname(os.path.abspath(__file__))

        if branch != None:
            self.set_branch(branch)


    def set_branch(self, branch):
        """Validate given branch, set branch attribute, call load_settings

        Raise exception if branch is invalid.

        Args:
            branch (str): branch name
        """
        if branch in self.valid_branches:
            self.branch = branch
            self.load_settings()
            self.check_connection()
        else:
            raise Exception('Error BranchConfig: invalid branch')


    def load_settings(self):
        """Load setting for branch from config json

        Raise exception if config json does not exist.
        """
        # config file from branch's asdf
        config_path = self.parent + '/config.json'
        config_exists = os.path.isfile(config_path)

        if config_exists:

            config_file = open(config_path, 'r')
            self.config_json = json.load(config_file)
            config_file.close()

        else:
            raise Exception("Error BranchConfig: could not find config json")


        try:
            self.branch_settings = self.config_json[self.branch]

            self.branch_keys = self.branch_settings.keys()

            for attr in self.branch_keys:
                setattr(self, attr, self.branch_settings[attr])

        except:
            raise Exception("Error BranchConfig: could not add config settings to BranchConfig")


    def check_connection(self):
        """Test mongodb connection
        """
        try:
            connection_timeout_ms = 10000

            client = pymongo.MongoClient(
                self.server, serverSelectionTimeoutMS=connection_timeout_ms)

            client.server_info()
            self.connection_status = 0

        except pymongo.errors.ServerSelectionTimeoutError as err:
            # print "Error (ServerSelectionTimeoutError) connecting to mongodb ("+str(config.server)+")"
            # print err
            self.connection_status = 1
            self.connection_error = err

        except Exception as err:
            # print "Other error connecting to mongodb ("+str(config.server)+")"
            # print err
            self.connection_status = 2
            self.connection_error = err
