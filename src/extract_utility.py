"""Contains ExtractObject, ValidateObject, MergeObject classes and related functions"""

import sys
import os
import warnings
import time
from numpy import isnan

# import rasterstats as rs
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_rasterstats as rs

import pandas as pd

# import geopandas as gpd
import fiona
from shapely.geometry import shape


# -----------------------------------------------------------------------------


def str_to_range(value):
    """Generate year range based on year string range segment input.

    Args:
        value (str): year string range segment (see parse_year_string() documentation for year string details)
    Returns:
        year range (List[int]):
    """
    if not isinstance(value, str):
        raise Exception("str_to_range: input must be str")

    range_split = value.split(':')

    if len(range_split) != 2:
        raise Exception("str_to_range: result of split must be 2 items")

    try:
        start = int(range_split[0])
        end = int(range_split[1]) + 1
    except:
        raise Exception("str_to_range: invalid years")

    return range(start, end)


def parse_year_string(value):
    """Generate list of years based on year string input.

    Years string are a simple method of specifying years and year ranges using
    a single string.
    Through parsing of the year string, a list of years is generated.

    The 4 components of a year string are:
        1) single years -   simply notated using the 4 digit year (eg: 1995),
                            this add an individual year to the list

        2) year ranges -    ranges are two years separated by a colon
                            (eg: 1990:2000). year ranges add all years
                            starting with & including the first year and
                            going up to & including the last year to the
                            year list

        3) negation -       placing an exclamation point (!) in front of any
                            year or year range will remove the specified year
                            or year range from the list

        4) separator -      the separator or pipe (|) is used to separate each
                            portion of your year string

    Year strings are parsed sequentially, meaning that each pipe separated
    portion of the year string will be parse in order and will override any
    previous segments. The resulting list is "positive" which means that only
    accepted years are included (ie: a year string with only negations will
    be empty.)

    Examples:
    - 1980|1990:1992 = ['1980', '1990', '1991', '1992']
    - 1980:1982 = ['1980', '1981', '1982']
    - 1980:1982|!1981 = ['1980', '1982']
    - 1985:1987|!1980:1990 = []

    Args:
        value (str): year string (see above for details on year strings)
    Returns:
        year list (List[str]): list of strings generated based on year string
    """
    statements = [x for x in str(value).split('|') if x != ""]

    tmp_years = []

    for i in statements:
        if i.startswith('!'):

            if ':' in i:
                tmp_range = str_to_range(i[1:])
                tmp_years = [y for y in tmp_years if y not in tmp_range]
            else:
                try:
                    year = int(i[1:])
                except:
                    raise Exception("parse_year_string: invalid year")

                tmp_years = [y for y in tmp_years if y != year]
        else:

            if ':' in i:
                tmp_range = str_to_range(i)
                tmp_years += tmp_range
            else:
                try:
                    year = int(i)
                except:
                    raise Exception("parse_year_string: invalid year")

                tmp_years += [year]


    return map(str, tmp_years)


def get_years(value):
    """Get years.

    Defines how to handle empty year strings

    Args:
        value (str): string which may be a year string or empty
    """
    if value == None:
        value = ""

    statements = [x for x in str(value).split('|') if x != ""]

    if len(statements) == 0:
        tmp_years = map(str, range(1000,10000))

    else:
        tmp_years = parse_year_string(value)

    return tmp_years


# -----------------------------------------------------------------------------


class ExtractObject():
    """Contains variables and functions needed to validate and run extracts.

    Attributes (static):

        _extract_options (dict): dictionary where keys are available extract
                                 options and values are their associated
                                 letter indicators
        _vector_extensions (List[str]): valid file extensions for vector files
        _raster_extensions (List[str]): valid file extensions for raster files


    Attributes (args):

        _builder (bool): indicates whether ExtractObject is being called by
                         builder (prevents portions of code from being run
                         when extracts are not actually going to be run)


    Attributes (variable):

        _vector_path (str): path to vector file
        _vector_extension (str): extension for vector file
        _vector_info (Tuple(str, str)): standardized tuple containing vector
                                        path/layer info

        _extract_type (str): selected extract type (mean, max, etc.)

        _base_path (str): base path for datasets to be extracted

        # default_years (List[int]): default year list to use when no years
                                     are provided
        _years (List[str]): list of years generated by parsing year string

        _file_mask (str): file mask used to parse date information from
                          data file
        _run_option (str): automatically generated. used to identify temporal
                           type of dataset (based on file mask)

        # _raster_path (str): path to raster file

    """
    # available extract types and associated identifiers
    _extract_options = {
        # "var": "v",
        # "std": "d",
        "sum": "s",
        "max": "x",
        # "min": "m",
        "mean": "e",
        "count": "n",
        "categorical": "c"
    }

    # accepted vector file extensions
    _vector_extensions = [".geojson", ".shp"]

    # accepted raster file extensions
    _raster_extensions = [".tif", ".asc"]

    def __init__(self, builder=False):
        self._builder = builder

        self._vector_path = None
        self._vector_extension = None
        self._vector_info = (None, None)

        self._extract_type = None

        self._base_path = None

        # self.default_years = range(1000, 10000)
        self._years = []

        self._file_mask = None
        self._run_option = None

        self._reliability = False
        self._reliability_geojson = None
        # self._raster_path = None


    def set_vector_path(self, value):
        """Set vector file path.

        should this have more advanced vector checks? (ie load and test)

        Args:
            value (str): vector file path
        """
        if not os.path.isfile(value):
            raise Exception("set_vector_path: vector does not exist " +
                            "(" + value + ")")

        self._vector_path = value
        self._set_vector_extension(value)

        vector_dirname = os.path.dirname(value)
        vector_filename, self._vector_extension = os.path.splitext(
            os.path.basename(value))

        # break vector down into path and layer
        # different for shapefiles and geojsons
        if self._vector_extension == ".geojson":
            self._vector_info = (value, "OGRGeoJSON")

        elif self._vector_extension == ".shp":
            self._vector_info = (vector_dirname, vector_filename)

        else:
            raise Exception("invalid vector extension " +
                            "(" + self._vector_extension + ")")


    def _set_vector_extension(self, value):
        """Set vector file extension.

        Args:
            value (str): vector file extension
        """
         # check extension
        if not value.endswith(tuple(self._vector_extensions)):
            raise Exception("invalid vector extension (" + value + ")")

        self._vector_extension = value


    def _check_file_mask(self):
        """Run general validation of file_mask based on base_path

        Makes sure that:
        1) temporally invariant file masks (ie "None") are not used with
        a base path that indicates temporal data (ie base_path is directory)
        2) temporal file masks (ie not "None") are not used with a base path
        that indicates temporally invariant data (ie base_path is file)
        """
        if (self._file_mask == "None" and self._base_path != None and
                not self._base_path.endswith(tuple(self._raster_extensions))):
            raise Exception("check_file_mask: invalid use of None file_mask " +
                            "based on base_path")
        elif (self._file_mask not in [None, "None"] and
                self._base_path != None and
                self._base_path.endswith(tuple(self._raster_extensions))):
            raise Exception("check_file_mask: invalid use of temporal " +
                            "file_mask based on base_path")

    def _check_reliability(self):
        """Verify that files needed to run reliability exist.
        """
        if self._reliability != False and self._base_path != None:
            if os.path.isfile(self._base_path):
                base_path_dir = os.path.dirname(self._base_path)
            else:
                base_path_dir = self._base_path
            if not os.path.isfile(base_path_dir + "/unique.geojson"):
                raise Exception("check_reliability: reliability geojson" +
                                " does not exist.")
            else:
                self._reliability_geojson = base_path_dir + "/unique.geojson"


    def set_base_path(self, value):
        """Set data base path.

        Args:
            value (str): base path where year/day directories for processed
                         data are located
        """
        # validate base_path
        if not os.path.exists(value):
            raise Exception("base_path is not valid ("+ value +")")

        self._base_path = value

        self._check_file_mask()
        self._check_reliability()


    def set_reliability(self, value):
        if value in [True, 1, "True"]:
            self._reliability = True
            self._check_reliability()
        else:
            self._reliability = False


    def set_years(self, value):
        """Set years.

        If year string is empty, accept all years found when searching for data.

        Args:
            value (str): year string
        """
        self._years = get_years(value)


    def set_file_mask(self, value):
        """Set file mask.

        Args:
            value (str): file mask
        """
        if value == "None":

            tmp_run_option = 1

        elif "YYYY" in value:

            if "MM" in value and not "DDD" in value:
                tmp_run_option = 3
            elif "DDD" in value and not "MM" in value:
                tmp_run_option = 4
            elif not "MM" in value and not "DDD" in value:
                tmp_run_option = 2
            else:
                raise Exception("set_file_mask: ambiguous temporal string " +
                                "("+str(value)+")")

        else:
            raise Exception("set_file_mask: invalid file mask " +
                            "("+str(value)+")")


        self._file_mask = value
        self._run_option = str(tmp_run_option)
        self._check_file_mask()


    def set_extract_type(self, value, category_map=None):
        """Set extract type.

        Args:
            value (str): extract type
        """
        # validate input extract type
        if value not in self._extract_options.keys():
            raise Exception("invalid extract type (" + value + ")")

        if value == "categorical":
            if not isinstance(category_map, dict):
                raise Exception("invalid category map (" +
                                str(category_map) + ")")

            for k, v in category_map.items():
                if not isinstance(v, (int, float)):
                    raise Exception("invalid category map value (" + str(v) +
                                    ") for key '" + str(k) + "'")

            # swap because it is reverse in original json
            # add ad_ prefix for grep during output
            category_map = dict([(v, k) for k, v in category_map.items()])


        self._extract_type = str(value)
        self._cmap = category_map


    def gen_data_list(self):
        """Generate data list (qlist).

        should this have more advanced raster checks? (ie load and test)
        """
        # temporally invariant dataset
        if self._run_option == "1":

            qlist = [[['',''], self._base_path]]

        # year
        elif self._run_option == "2":

            id_char = "Y"

            qlist = [
                [
                    [
                        "".join([x for x,y in zip(name, self._file_mask)
                        if y == id_char and x.isdigit()])
                    ], name
                ]
                for name in os.listdir(self._base_path)
                if not os.path.isdir(os.path.join(self._base_path, name))
                and name.endswith(tuple(self._raster_extensions))
                and "".join([
                    x for x,y in zip(name, self._file_mask)
                    if y == id_char and x.isdigit()
                ]) in self._years
            ]

        # year month/day
        elif self._run_option in ["3", "4"]:

            if self._run_option == "3":
                # year month
                id_char = "M"
            else:
                # year day
                id_char = "D"


            years = [
                        name for name in os.listdir(self._base_path)
                        if os.path.isdir(os.path.join(self._base_path, name))
                        and name in self._years
                    ]

            # list of all [[year, month], name] combos
            qlist = []

            for year in years:
                path_year = self._base_path +"/"+ year
                qlist += [
                    [
                        [
                            year, "".join([
                                x for x,y in zip(year+"/"+name, self._file_mask)
                                if y == id_char and x.isdigit()
                            ])
                        ], year+"/"+name
                    ]
                    for name in os.listdir(path_year)
                    if not os.path.isdir(os.path.join(path_year, name))
                    and name.endswith(tuple(self._raster_extensions))
                ]

        else:
            raise Exception("Invalid run_option value: " +
                            str(self._run_option))

        # if len(qlist) == 0:
        #     raise Exception("No rasters found based on input parameters")

        # sort qlist
        qlist = sorted(qlist)

        return qlist


    # def set_raster_extension(self, value):
    #     """Set

    #     Args:
    #         value (str):
    #     """


    def format_extract(self, stats):

        if self._extract_type == "categorical":
            for feat in stats:
                for i in self._cmap.values():
                    colname = 'excat_' + i
                    if (colname not in feat['properties'].keys() or
                            isnan(feat['properties'][colname])):
                        feat['properties'][colname] = 0

                yield feat

        else:
            for feat in stats:
                colname = 'ad_' + self._extract_type
                if colname in feat['properties'].keys():
                    try:
                        if feat['properties'][colname] in [None, 'nan', 'NaN'] or isnan(feat['properties'][colname]):
                            feat['properties'][colname] = 'NA'
                    except:
                        print feat['properties'][colname]
                        print type(feat['properties'][colname])
                        feat['properties'][colname] = 'NA'


                    feat['properties']['ad_extract'] = feat['properties'][colname]
                    del feat['properties'][colname]

                else:
                    warnings.warn('Extract field missing from feature properties')
                    print str(feat['properties'])
                    feat['properties']['ad_extract'] = 'NA'

                yield feat


    def run_extract(self, raster, output):
        """Run python extract using rasterstats

        Args:
            raster (str): path of raster file relative to base path
            output (str): absolute path for csv output of extract
        """
        Te_start = int(time.time())

        # try:

        if self._extract_type == "categorical":
            raw_stats = rs.gen_zonal_stats(self._vector_path, raster,
                            prefix="excat_", stats="count",
                            categorical=True, category_map=self._cmap,
                            all_touched=True, weights=True,
                            geojson_out=True)

        else:
            raw_stats = rs.gen_zonal_stats(self._vector_path, raster,
                            prefix="ad_", stats=self._extract_type,
                            all_touched=True, weights=False,
                            geojson_out=True)


        stats = self.format_extract(raw_stats)

        self.export_extract(stats, output)


        # except Exception as extract_error:
        #     print "error running extract for " + output
        #     print extract_error
        #     if os.path.isfile(output+".csv"):
        #         os.remove(output+".csv")

        #     return (1, 'Error runing extract')


        Te_run = int(time.time() - Te_start)

        return (0, 'completed extract ('+ output +') in '+ str(Te_run) +' seconds')


    def export_extract(self, stats, output):

        extract_fh = open(output + ".csv", "w")

        # open reliability csv
        if self._reliability:
            rel_fh = open(output[:-1] + "r.csv", "w")

        import csv

        header = True
        for feat in stats:
            ex_data = feat['properties']

            if header:
                header = False

                fieldnames = sorted(list(ex_data.keys()), key=str)

                extract_csvwriter = csv.DictWriter(extract_fh,
                                                   delimiter=str(","),
                                                   fieldnames=fieldnames)
                extract_csvwriter.writeheader()

                # reliability header
                if self._reliability:
                    rel_fieldnames = fieldnames + ['ad_sum', 'ad_max']
                    rel_csvwriter = csv.DictWriter(rel_fh,
                                                   delimiter=str(","),
                                                   fieldnames=rel_fieldnames)
                    rel_csvwriter.writeheader()

            try:
                extract_csvwriter.writerow(ex_data)
            except:
                for k in ex_data:
                    if isinstance(ex_data[k], unicode):
                        ex_data[k] = ex_data[k].encode('utf-8')

                extract_csvwriter.writerow(ex_data)


            # run reliability calcs and write to csv
            if self._reliability:

                # reliability geojson
                # mean surface features with aid info
                rgeo = fiona.open(self._reliability_geojson)

                feat_id = feat['id']
                feat_geom = shape(feat['geometry'])

                max_dollars = 0
                for r in rgeo:
                    r_intersects = shape(r['geometry']).intersects(feat_geom)

                    if r_intersects:
                        unique_dollars = r['properties']['unique_dollars']
                        max_dollars += unique_dollars


                # calculate reliability statistic
                ex_data['ad_max'] = max_dollars
                ex_data['ad_sum'] = ex_data['ad_extract']
                ex_data['ad_extract'] = ex_data['ad_sum'] / ex_data['ad_max']

                rel_csvwriter.writerow(ex_data)


        extract_fh.close()

        # close reliability csv
        if self._reliability:
            rel_fh.close()



# -----------------------------------------------------------------------------


# class ValidateObject():
#     """Contains vars and funcs needed to validate results of an extract job.

#     Make sure everything all extracts that were expected to be run for a job
#     were actually run and completed.

#     Attributes:

#         _something1 (str): x
#         _something2 (List[str]): x

#     """

#     def __init__(self, interface=False):

#         self._interface = interface


# -----------------------------------------------------------------------------


class MergeObject():
    """Contains vars and funcs needed to merge results of an extract job.

    Merges all available extracts results given boundary, dataset, extract
        type and year string.
    Does not perform source data checks (see ExtractObject) or job
        validation (see ValidateObject).

    When run as part of job chain (ie: Extract > Validate > Merge) users can
    confirm via ExtractObject (and ValidateObject?) that all available
    datasets meeting job config specification were extracted and thus will
    be merged.

    When running merge on its own, only available extracts are used
    (ie: we do not check if there is some dataset that exists, but has
    not yet been extracted)

    Attributes:

        merge_json (Dict): contents of job/config json
        merge_output_dir (str): path to output for merge results
        interface (bool): indicates if merge is being called by user from
                          merge script or automatically run by extract job
        merge_list (List[Dict]): list of Dicts which each contain merge
                                 bnd_name and file_list

    """

    def __init__(self, merge_json, merge_output_dir, interface=False):

        self.merge_json = merge_json

        self.merge_output_dir = os.path.abspath(merge_output_dir)

        self.interface = interface

        self.merge_list = []


    def build_merge_list(self):
        """build merge list

        maybe the validation object should handle building this list
        - or maybe we do not actually need a validation object if merge does
          everything validation was going to do
        - is there anything beyond checking the resulting extract
        csvs that needs to (or can) be done?

        """
        tmp_merge_list = []

        # if not interface, use job qlists
        if not self.interface:

            bnd_list = set([i['settings']['bnd_name']
                            for i in self.merge_json['job']['datasets']])

            for bnd_name in bnd_list:

                bnd_merge_list = []

                for i in self.merge_json['job']['datasets']:
                    if i['settings']['bnd_name'] == bnd_name:

                        data_name = i['name']
                        extract_type = i['settings']['extract_type']
                        output_base = i['settings']['output_base']
                        data_mini = i['settings']['data_mini']

                        extract_abbr = ExtractObject._extract_options[extract_type]

                        bnd_merge_list += [
                            os.path.join(
                                output_base,
                                bnd_name,
                                'cache',
                                data_name,
                                extract_type,
                                data_mini +'_'+ ''.join(j[0]) + extract_abbr + '.csv'
                            )
                            for j in i['qlist']
                        ]

                        # add reliability calc results
                        bnd_merge_list += [
                            os.path.join(
                                output_base,
                                bnd_name,
                                'cache',
                                data_name,
                                extract_type,
                                data_mini +'_'+ ''.join(j[0]) + 'r.csv'
                            )
                            for j in i['qlist']
                            if i['settings']['reliability'] == True
                        ]


                # add merge list for boundary as new item in tmp merge list
                tmp_merge_list.append({
                    'bnd_name': bnd_name,
                    'file_list': bnd_merge_list
                })


        # if interface build from merge data
        else:

            # generate data needed to build file list

            required_options = ["bnd_name", "extract_type", "output_base", "years"]
            missing_defaults = [i for i in required_options
                                if i not in self.merge_json['defaults'].keys()]

            if len(missing_defaults) > 0:
                print ("MergeObject warning: required option(s) missing " +
                       "from defaults ("+str(missing_defaults)+")")

            merge_data = []

            for dataset_options in self.merge_json['data']:

                dataset_name = dataset_options['name']
                print dataset_name

                tmp_config = {}
                tmp_config['name'] = dataset_name

                for k in required_options:
                    if k in dataset_options:
                        tmp_config[k] = dataset_options[k]
                    else:
                        tmp_config[k] = self.merge_json['defaults'][k]

                merge_data.append(tmp_config)

            # build file list

            bnd_list = set([i['bnd_name'] for i in merge_data])

            for bnd_name in bnd_list:

                bnd_merge_list = []

                for i in merge_data:
                    if i['bnd_name'] == bnd_name:

                        dset_years = get_years(i['years'])

                        data_name = i['name']
                        extract_type = i['extract_type']
                        extract_base = i['output_base']

                        # --------------------------------------------------

                        extract_dir = (extract_base + "/" + bnd_name +
                                       "/cache/" + data_name + "/" +
                                       extract_type)

                        print "\tChecking for extracts in: " + extract_dir

                        # validate inputs by checking directories exist
                        if not os.path.isdir(extract_base):
                            sys.exit("Directory for extracts does not " +
                                     "exist. You may not be connected to " +
                                     "sciclone ("+extract_base+")")
                        elif not os.path.isdir(extract_base + "/" + bnd_name):
                            sys.exit("Directory for specified bnd_name does " +
                                     "not exist (bnd_name: "+bnd_name+")")
                        elif not os.path.isdir(
                            extract_base + "/" + bnd_name + "/cache/" +
                            data_name):
                                sys.exit("Directory for specified dataset " +
                                         "does not exists (data_name: " +
                                         data_name+")")
                        elif not os.path.isdir(extract_dir):
                            sys.exit("Directory for specified extract type " +
                                     "does not exist (extract_type: " +
                                     extract_type+")")


                        # find and sort all relevant extract files
                        rlist = [
                            fname for fname in os.listdir(extract_dir)
                             if (len(fname) == 10 or fname[5:9] in dset_years)
                             and os.path.isfile(extract_dir +"/"+ fname)
                             and fname.endswith(".csv")
                        ]
                        rlist = sorted(rlist)

                        # exit if no extracts found
                        if len(rlist) == 0:
                            sys.exit("No extracts found for: " + extract_dir)

                        bnd_merge_list += [extract_dir +"/"+ item for item in rlist]


                # add merge list for boundary as new item in tmp merge list
                tmp_merge_list.append({
                    'bnd_name': bnd_name,
                    'file_list': bnd_merge_list
                })


        # set actual merge list
        self.merge_list = tmp_merge_list


    def run_merge(self):
        """Run merge

        """
        # Ts = int(time.time())

        for i in self.merge_list:
            bnd_name = i['bnd_name']
            file_list = i['file_list']

            print "Starting merge process for bnd_name = " + bnd_name

            # if interface ask for output path
            if self.interface == True:

                while True:
                    # ask for path
                    sys.stdout.write("Absolute file path for output? \n> ")
                    answer = raw_input()

                    # make sure directory exists
                    if os.path.isdir(os.path.dirname(answer)):

                        # make sure file name ends with .csv
                        if answer.endswith(".csv"):
                            merge_output_csv = answer
                            break
                        else:
                            sys.stdout.write("Invalid file extension (must " +
                                             "end with \".csv\").\n")

                    else:
                        sys.stdout.write("Directory specified does not " +
                                         "exist.\n")


            else:
                merge_output_csv =  (self.merge_output_dir + "/merge_" +
                                     bnd_name + ".csv")


            merge = 0

            for result_csv in file_list:

                result_df = pd.read_csv(result_csv, quotechar='\"',
                                        na_values='', keep_default_na=False)

                tmp_field = result_csv[result_csv.rindex('/')+1:-4]

                if not isinstance(merge, pd.DataFrame):
                    merge = result_df.copy(deep=True)

                    if tmp_field.endswith("c"):
                        cat_fields = [
                            cname for cname in list(merge.columns)
                            if cname.startswith("excat_")
                        ]
                        for c in cat_fields:
                            new_cat_field = tmp_field + c[5:]
                            merge.rename(columns={c: new_cat_field},
                                         inplace=True)
                    else:
                        merge.rename(columns={"ad_extract": tmp_field},
                                     inplace=True)

                else:
                    if tmp_field.endswith("c"):
                        cat_fields = [
                            cname for cname in list(result_df.columns)
                            if cname.startswith("excat_")
                        ]
                        for c in cat_fields:
                            new_cat_field = tmp_field + c[5:]
                            merge[new_cat_field] = result_df[c]

                    else:
                        merge[tmp_field] = result_df["ad_extract"]



            if isinstance(merge, pd.DataFrame):

                merge.to_csv(merge_output_csv, index=False)

                print '\tMerge completed for ' + bnd_name
                print '\tResults output to ' + merge_output_csv

            else:
                print ('\tWarning: no extracts merged for bnd_name = ' +
                       bnd_name)


        # T_run = int(time.time() - Ts)
        # print 'Merge Runtime: ' + str(T_run//60) +'m '+ str(int(T_run%60)) +'s'



