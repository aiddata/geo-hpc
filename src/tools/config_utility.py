"""Contains configuration setting utilies.

BranchConfig (class): used to access setting from config json for specific branch
"""

import os
import json

class BranchConfig():
    """Get branch config settings from config json for specified branch.

    Attributes:

        branch (str): branch name
        valid_branches
        parent

        config_json
        branch_settings
        branch_keys

        **settings
    """

    def __init__(branch=None):
        self.branch = None
        self.valid_branches = ['master', 'develop']

        if branch != None:
            self.set_branch(branch)

        self.parent = os.path.dirname(os.path.abspath(__file__))


    def set_branch(self, branch):
        """Validate given branch, set branch attribute, call load_settings

        Raise exception if branch is invalid.

        Args:
            branch (str): branch name
        """
        if branch in valid_branches:
            self.branch = branch
            self.load_settings()
        else:
            raise Exception('Error BranchConfig: invalid branch')


    def load_settings():
        """Load setting for branch from config json

        Raise exception if config json does not exist.
        """
        # config file from branch's asdf
        config_exists = os.path.isfile(self.parent +'/config.json')        


        if config_exists:


            config_file = open(input_json_path, 'r')
            self.config_json = json.load(config_file)
            config_file.close()

            self.branch_settings = self.config_json[self.branch]

            self.branch_keys = self.branch_settings.keys()

            for attr in self.branch_keys:
                setattr(self, attr, self.branch_settings[attr])

        else:
            raise Exception("Error BranchConfig: could not find config json")


