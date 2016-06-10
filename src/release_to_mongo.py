
import sys
import os
import pandas as pd
import pymongo


# convert flat tables from release datasets into nested mongo database
def run(name=None, path=None, client=None, config=None):

    if name is None:
        quit("No name provided")

    # get release base path
    if path is not None:

        if not os.path.isdir(path):
            quit("Invalid base directory provided.")

    else:
        quit("No base directory provided")


    if client is None:
        quit("No mongodb client connection provided")

    # if config is None:
    #     quit("No config object provided")



    db_releases = client.releases

    db_releases.drop_collection(name)
    c_release = db_releases[name]

    files = ["projects", "locations", 'transactions']

    tables = {}
    for table in files:
        file_path = path+"/data/"+table+".csv"

        if os.path.isfile(file_path):
            tables[table] = pd.read_csv(file_path, sep=',', quotechar='\"')

        else:
            raise Exception("no valid table type found for: " + file_path)

        tables[table]["project_id"] = tables[table]["project_id"].astype(str)


    # add new data for each project
    for project_row in tables['projects'].iterrows():

        project = dict(project_row[1])
        project_id = project["project_id"]


        transaction_match = tables['transactions'].loc[tables['transactions']["project_id"] == project_id]

        if len(transaction_match) > 0:
            project["transactions"] = [dict(x[1]) for x in transaction_match.iterrows()]

        else:
            print "No transactions found for project id: " + str(project_id)


        location_match = tables['locations'].loc[tables['locations']["project_id"] == project_id]

        if len(location_match) > 0:
            project["locations"] = [dict(x[1]) for x in location_match.iterrows()]

        else:
            print "No locations found for project id: " + str(project_id)


        # add to collection
        c_release.insert(project)



if __name__ == '__main__':

    # if calling script directly, use following input args:
    #   release name - same as in asdf (required)
    #   absolute path to release (required)

    # import sys
    # import os

    branch = sys.argv[0]

    branch_dir = os.path.join(os.path.expanduser('~'), 'active', branch)

    if not os.path.isdir(branch_dir):
        raise Exception('Branch directory does not exist')


    config_dir = os.path.join(branch_dir, 'asdf', 'src', 'tools')
    sys.path.insert(0, config_dir)

    from config_utility import BranchConfig

    config = BranchConfig(branch=branch)

    # -------------------------------------

    # check mongodb connection
    if config.connection_status != 0:
        sys.exit("connection status error: " + str(config.connection_error))

    # -----------------------------------------------------------------------------

    client = pymongo.MongoClient(config.server)

    run(name=sys.argv[1], path=sys.argv[2], client=client, config=config)
