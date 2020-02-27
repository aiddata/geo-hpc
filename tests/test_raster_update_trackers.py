

import time
import random
import itertools
import pymongo
import fiona
import rasterio
import numpy as np
from shapely.geometry import Point, shape, box
from shapely.ops import cascaded_union
from pprint import pprint


bnd_path = '/sciclone/aiddata10/REU/geo/data/boundaries/nyc/nyc_congressional_districts/Congressional_Districts.geojson'

with fiona.open(bnd_path, 'r') as bnd_src:
    minx, miny, maxx, maxy = bnd_src.bounds
    total_area = sum([shape(i['geometry']).area for i in bnd_src])


dset_path = '/sciclone/aiddata10/REU/geo/data/rasters/ambient_air_pollution_2013/fus_calibrated/fus_calibrated_2000.tif'
dset_path = '/sciclone/aiddata10/REU/geo/data/rasters/gpw/gpw_v4/count/gpw-v4-population-count-adjusted-to-2015-unwpp-country-totals_2000.tif'

raster_src = rasterio.open(dset_path)

pixel_size = raster_src.meta['transform'][1]
nodata = raster_src.meta['nodata']

xsize = (maxx - minx) / pixel_size
ysize = (maxy - miny) / pixel_size

total_pixel_count = xsize * ysize

# -----
# this section creates the sample of pixels within extents of boundary data
# *** potential flaw here is that samples are only within the extet, but
#     not necessarily within the actual boundary. For data such as islands
#     which have small areas but cover large extents, and which are surrounded
#     by nodata vals, this could be an issue

# minimum ratio of valid pixels required
valid_sample_thresh = 0.05
# maximum number of pixels to test
pixel_limit = 50000

# init as > than limit to force one run of loop
sampled_pixel_count = pixel_limit + 1
step_size = pixel_size * 1

while sampled_pixel_count > pixel_limit:
    xvals = np.arange(minx, maxx, step_size)
    yvals = np.arange(miny, maxy, step_size)
    samples = list(itertools.product(xvals, yvals))
    sampled_pixel_count = len(samples)
    # increase step size until sample pixel count is small enough
    step_size = pixel_size * 2

# -----

values = [val[0] for val in raster_src.sample(samples)]

raster_src.close()

clean_values = [i for i in values if i != nodata and i is not None]

distinct_values = set(clean_values)

# percent of samples resulting in clean value
if len(clean_values) > len(samples)*valid_sample_thresh and len(distinct_values) > 1:
    result = True
else:
    print '\t\t\tPixel check did not pass'
