"""
"""
import os

class BranchConfig():
    """
    """

    def __init__(branch=branch):
        self.branch = branch
        self.home = os.path.expanduser('~')
        self.parent = os.path.dirname(os.path.abspath(__file__))

        self.setup()


    def setup():
        
        active_exists = os.path.isfile(self.home +'/active/config.json')        
        
        local_exists = os.path.isfile(self.parent +'/config.json')        


        if active_exists:
            # main config file
            pass

        elif local_exists:
            # config from local repo
            # should default repo config be acceptable?
            pass

        else:
            # assume local db?
            pass



    def function():
        pass
