# process queue requests


import sys

from utility import utility
from cache import cache



utility = utility()
cache = cache()


request_id = 0
run_extract = False


if len(sys.argv) == 2 and utility.check_id(sys.argv[1]):
    request_id = sys.argv[1]
else:
    run_extract = True
    request_id = utility.get_next()


