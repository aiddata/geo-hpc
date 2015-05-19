
from __future__ import print_function

import sys
import random

import pandas as pd
from shapely.geometry import Polygon, Point, shape, box


# ====================================================================================================
# functions


# check csv delim and return if valid type
def getCSV(path):
	if path.endswith('.tsv'):
		return pd.read_csv(path, sep='\t', quotechar='\"', na_values='', keep_default_na=False)
	elif path.endswith('.csv'):
		return pd.read_csv(path, quotechar='\"', na_values='', keep_default_na=False)
	else:
		sys.exit('getCSV - file extension not recognized.\n')


# get project and location data in path directory
# requires a field name to merge on and list of required fields
def getData(path, merge_id, field_ids, only_geo):

	amp_path = path+"/projects.tsv"
	loc_path = path+"/locations.tsv"

	# make sure files exist
	# 

	# read input csv files into memory
	amp = getCSV(amp_path)
	loc = getCSV(loc_path)


	if not merge_id in amp or not merge_id in loc:
		sys.exit("getData - merge field not found in amp or loc files")

	amp[merge_id] = amp[merge_id].astype(str)
	loc[merge_id] = loc[merge_id].astype(str)

	# create projectdata by merging amp and location files by project_id
	if only_geo:
		tmp_merged = amp.merge(loc, on=merge_id)
	else:
		tmp_merged = amp.merge(loc, on=merge_id, how="left")

	if not "longitude" in tmp_merged or not "latitude" in tmp_merged:
		sys.exit("getData - latitude and longitude fields not found")

	for field_id in field_ids:
		if not field_id in tmp_merged:
			sys.exit("getData - required code field not found")

	return tmp_merged


# --------------------------------------------------


# defin tags enum
def enum(*sequential, **named):
    # source: http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
	enums = dict(zip(sequential, range(len(sequential))), **named)
	return type('Enum', (), enums)


# gets geometry type based on lookup table
# depends on lookup and not_geocoded
def geomType(is_geo, code):

	try:
		is_geo = int(is_geo)
		code = str(int(code))

		if is_geo == 1: 
			if code in lookup:
				tmp_type = lookup[code]["type"]
				return tmp_type

			else:
				print("lookup code not recognized: " + code)
				return "None"

		elif is_geo == 0:
			return not_geocoded

		else:
			print("is_geocoded integer code not recognized: " + str(is_geo))
			return "None"

	except:
		return not_geocoded



# finds shape in set of polygons which arbitrary polygon is within
# returns 0 if item is not within any of the shapes
def getPolyWithin(item, polys):
	c = 0
	for shp in polys:
		tmp_shp = shape(shp)
		if item.within(tmp_shp):
			return tmp_shp

	return c


# checks if arbitrary polygon is within country (adm0) polygon
# depends on adm0
def inCountry(shp):
	return shp.within(adm0)


# build geometry for point based on code
# depends on lookup and adm0
def getGeom(code, lon, lat):
	tmp_pnt = Point(lon, lat)
	
	if not inCountry(tmp_pnt):
		print("point not in country")
		return 0

	elif lookup[code]["type"] == "point":
		return tmp_pnt

	elif lookup[code]["type"] == "buffer":
		try:
			tmp_int = float(lookup[code]["data"])
			tmp_buffer = tmp_pnt.buffer(tmp_int)

			if inCountry(tmp_buffer):
				return tmp_buffer
			else:
				return tmp_buffer.intersection(adm0)

		except:
			print("buffer value could not be converted to float")
			return 0

	elif lookup[code]["type"] == "adm":
		try:
			tmp_int = int(lookup[code]["data"])
			return getPolyWithin(tmp_pnt, adm_shps[tmp_int])

		except:
			print("adm value could not be converted to int")
			return 0

	else:
		print("code type not recognized")
		return 0


# returns geometry for point
# depends on agg_types and adm0
def geomVal(agg_type, code, lon, lat):
	if agg_type in agg_types:
		
		code = str(int(code))
		tmp_geom = getGeom(code, lon, lat)

		if tmp_geom != 0:
			return tmp_geom

		return "None"

	elif agg_type == "country":

		return adm0

	else:
		print("agg_type not recognized: " + str(agg_type))
		return "None"


# --------------------------------------------------


# random point gen function
def get_random_point_in_polygon(poly):

	INVALID_X = -9999
	INVALID_Y = -9999

	(minx, miny, maxx, maxy) = poly.bounds
	p = Point(INVALID_X, INVALID_Y)
	px = 0
	while not poly.contains(p):
		p_x = random.uniform(minx, maxx)
		p_y = random.uniform(miny, maxy)
		p = Point(p_x, p_y)
	return p


# generate random point geom or use actual point
def addPt(agg_type, agg_geom):
	if agg_type == "point":
		return agg_geom
	else:
		tmp_rnd = get_random_point_in_polygon(agg_geom)
		return tmp_rnd
