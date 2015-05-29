
# calculate runtime based on iterations, data prep time, mean surf time,
# single iteration time, number of nodes and processors per nodes
# 
# returns string containing estimated hours and minutes in format: HH:MM:00

# ====================================================================================================

# runtime estimation formula (in minutes)
# 
#   init_time + mean_surface_time + ( avg_iteration_time * ( iterations / processes ) ) / 60
# 
# where:
#   init_time =  2 (minutes)
#   mean_surface_time = 12 (minutes)
#   avg_iteration_time = 24 (seconds)
#   iterations = number of iterations being run
#   processes = (number of nodes * cores per node) - 1
# 
# example:
#   for a 1000 iteration job running on 4 c11 nodes which have 8 cores each
# 
#   runtime = 2 + 12 + ( 24 ( 1000 / 31 ) ) / 60 
#   runtime = 26.9 minutes
# 
#   actual recorded runtime for same conditions = 26.2 minutes
# 
#   small difference is due to rounding up the estimated 
#   init_time and mean_surface_time which adds 30-45 seconds

# ====================================================================================================


import sys
import math

arg = sys.argv

# runtime components in minutes

# data prep/init (in minutes)
t_data_prep = float(sys.argv[1])

# mean surf (in minutes)
t_mean_surf = float(sys.argv[2])

# single iteration (in minutes)
t_iteration = float(sys.argv[3]) / 60.0

# number of nodes and processors per node
nodes = int(sys.argv[4])
ppn = int(sys.argv[5])

# number of iterations
iterations = int(sys.argv[6])

# --------------------------------------------------

cores = nodes * ppn - 1

runtime = t_data_prep + t_mean_surf + t_iteration * (iterations / cores) 

adj_run = runtime * 1.10

hours = int(math.floor(adj_run / 60))
minutes = int(math.ceil(adj_run - hours * 60))

if hours == 0 and minutes == 0:
    print "error"
    sys.exit("runcalc.py - runtime estimate is zero")

if hours < 10:
    hours = "0" + str(hours)

if minutes < 10:
    minutes = "0" + str(minutes)


print str(hours) +":"+ str(minutes) + ":00"

