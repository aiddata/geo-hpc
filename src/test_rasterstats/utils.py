# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
import sys
import os
import warnings
from rasterio import features
from affine import Affine
from shapely.geometry import box, MultiPolygon, Polygon
from .io import window_bounds
import numpy as np


DEFAULT_STATS = ['count', 'min', 'max', 'mean']
VALID_STATS = DEFAULT_STATS + \
    ['sum', 'std', 'median', 'majority', 'minority', 'unique', 'range', 'nodata'] + \
    ['weighted_sum', 'weighted_count', 'weighted_mean']
#  also percentile_{q} but that is handled as special case

def get_percentile(stat):
    if not stat.startswith('percentile_'):
        raise ValueError("must start with 'percentile_'")
    qstr = stat.replace("percentile_", '')
    q = float(qstr)
    if q > 100.0:
        raise ValueError('percentiles must be <= 100')
    if q < 0.0:
        raise ValueError('percentiles must be >= 0')
    return q


def rasterize_geom(geom, like, all_touched=False):
    geoms = [(geom, 1)]
    rv_array = features.rasterize(
        geoms,
        out_shape=like.shape,
        transform=like.affine,
        fill=0,
        all_touched=all_touched)
    return rv_array


# https://stackoverflow.com/questions/8090229/
#   resize-with-averaging-or-rebin-a-numpy-2d-array/8090605#8090605
def rebin_sum(a, shape, dtype):
    sh = shape[0],a.shape[0]//shape[0],shape[1],a.shape[1]//shape[1]
    return a.reshape(sh).sum(-1, dtype=dtype).sum(1, dtype=dtype)


def rasterize_pctcover(geom, atrans, shape):
    scale = 10

    pixel_size = atrans[0]/scale
    topleftlon = atrans[2]
    topleftlat = atrans[5]

    new_affine = Affine(pixel_size, 0, topleftlon,
                    0, -pixel_size, topleftlat)

    new_shape = (shape[0]*scale, shape[1]*scale)

    rasterized = features.rasterize(
        [(geom, 1)],
        out_shape=new_shape,
        transform=new_affine,
        fill=0,
        all_touched=True)

    min_dtype = np.min_scalar_type(scale**2)
    rv_array = rebin_sum(rasterized, shape, min_dtype)
    return rv_array.astype('float32') / (scale**2)


def stats_to_csv(stats, file_object=None):
    """Ouput stats to csv string or file.

    Does not work with generator object for stats, must use list.
    If invalid file_object is given, creates a temporary file.
    If writing to file, returns path to file. Otherwise returns
    csv output as string.
    """
    if file_object is None:

        if sys.version_info[0] >= 3:
            from io import StringIO as IO
        else:
            from cStringIO import StringIO as IO

        csv_fh = IO()

    else:
        if isinstance(file_object, file):
            csv_fh = file_object
        else:
            warnings.warn("invalid file object given, generating temp file instead",
                          UserWarning)
            import tempfile
            tmp_file = tempfile.mkstemp()
            csv_fh = open(tmp_file[1], 'w')

    import csv

    keys = set()
    for stat in stats:
        for key in list(stat.keys()):
            keys.add(key)

    fieldnames = sorted(list(keys), key=str)

    csvwriter = csv.DictWriter(csv_fh, delimiter=str(","), fieldnames=fieldnames)
    csvwriter.writerow(dict((fn, fn) for fn in fieldnames))
    for row in stats:
        csvwriter.writerow(row)

    if file_object is None:
        contents = csv_fh.getvalue()
        csv_fh.close()
        return contents
    else:
        abs_path = os.path.abspath(csv_fh)
        csv_fh.close()
        return abs_path


def check_stats(stats, categorical):
    if not stats:
        if not categorical:
            stats = DEFAULT_STATS
        else:
            stats = []
    else:
        if isinstance(stats, str):
            if stats in ['*', 'ALL']:
                stats = VALID_STATS
            else:
                stats = stats.split()
    for x in stats:
        if x.startswith("percentile_"):
            get_percentile(x)
        elif x not in VALID_STATS:
            raise ValueError(
                "Stat `%s` not valid; "
                "must be one of \n %r" % (x, VALID_STATS))

    run_count = False
    if categorical or 'majority' in stats or 'minority' in stats or 'unique' in stats:
        # run the counter once, only if needed
        run_count = True

    valid_weights = any([s.startswith('weighted_') for s in stats])

    return stats, run_count, valid_weights


def remap_categories(category_map, stats):
    def lookup(m, k):
        """ Dict lookup but returns original key if not found
        """
        try:
            return m[k]
        except KeyError:
            return k

    return {lookup(category_map, k): v
            for k, v in stats.items()}


def key_assoc_val(d, func, exclude=None):
    """return the key associated with the value returned by func
    """
    vs = list(d.values())
    ks = list(d.keys())
    key = ks[vs.index(func(vs))]
    return key


def boxify_points(geom, rast):
    """
    Point and MultiPoint don't play well with GDALRasterize
    convert them into box polygons 99% cellsize, centered on the raster cell
    """
    if 'Point' not in geom.type:
        raise ValueError("Points or multipoints only")

    buff = -0.01 * min(rast.affine.a, rast.affine.e)

    if geom.type == 'Point':
        pts = [geom]
    elif geom.type == "MultiPoint":
        pts = geom.geoms
    geoms = []
    for pt in pts:
        row, col = rast.index(pt.x, pt.y)
        win = ((row, row + 1), (col, col + 1))
        geoms.append(box(*window_bounds(win, rast.affine)).buffer(buff))

    return MultiPolygon(geoms)
