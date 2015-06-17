from osgeo import gdal,ogr,osr


class resource_utils():

    def __init__(self):
        
        self.dp = {}

        self.file_list = []
        
        self.resources = {}
        
        self.temporal = {
            "start": 0,
            "end": 0,
            "name": ""
        }

        self.spatial = ""


    def run_file_check(self, f):
        if not f.endswith('.' + self.dp["file_extension"]):
            return False

        return True



    # get bounding box
    # http://gis.stackexchange.com/questions/57834/how-to-get-raster-corner-coordinates-using-python-gdal-bindings

    def GetExtent(self, gt, cols, rows):
        ''' Return list of corner coordinates from a geotransform

            @type gt:   C{tuple/list}
            @param gt: geotransform
            @type cols:   C{int}
            @param cols: number of columns in the dataset
            @type rows:   C{int}
            @param rows: number of rows in the dataset
            @rtype:    C{[float,...,float]}
            @return:   coordinates of each corner
        '''
        ext=[]
        xarr=[0,cols]
        yarr=[0,rows]

        for px in xarr:
            for py in yarr:
                x = gt[0] + (px*gt[1]) + (py*gt[2])
                y = gt[3] + (px*gt[4]) + (py*gt[5])
                ext.append([x,y])
                # print x,y
            yarr.reverse()
        return ext


    def ReprojectCoords(self, coords, src_srs, tgt_srs):
        ''' Reproject a list of x,y coordinates.

            @type geom:     C{tuple/list}
            @param geom:    List of [[x,y],...[x,y]] coordinates
            @type src_srs:  C{osr.SpatialReference}
            @param src_srs: OSR SpatialReference object
            @type tgt_srs:  C{osr.SpatialReference}
            @param tgt_srs: OSR SpatialReference object
            @rtype:         C{tuple/list}
            @return:        List of transformed [[x,y],...[x,y]] coordinates
        '''
        trans_coords=[]
        transform = osr.CoordinateTransformation(src_srs, tgt_srs)
        for x,y in coords:
            x,y,z = transform.TransformPoint(x,y)
            trans_coords.append([x,y])
        return trans_coords


    def check_envelope(self, new, old):
        if len(new) == len(old) and len(new) == 4:
            # update envelope if polygon extends beyond bounds

            if new[0] < old[0]:
                old[0] = new[0]
            if new[1] > old[1]:
                old[1] = new[1]
            if new[2] < old[2]:
                old[2] = new[2]
            if new[3] > old[3]:
                old[3] = new[3]

        elif len(old) == 0 and len(new) == 4:
            # initialize envelope
            for x in new:
                old.append(x)

        else:
            quit("Invalid polygon envelope.\n")

        return old

    def raster_envelope(self, path):
        ds=gdal.Open(path)

        gt = ds.GetGeoTransform()
        cols = ds.RasterXSize
        rows = ds.RasterYSize
        ext = self.GetExtent(gt,cols,rows)

        src_srs = osr.SpatialReference()
        src_srs.ImportFromWkt(ds.GetProjection())
        #tgt_srs=osr.SpatialReference()
        #tgt_srs.ImportFromEPSG(4326)
        tgt_srs = src_srs.CloneGeogCS()

        geo_ext = self.ReprojectCoords(ext, src_srs, tgt_srs)
        # geo_ext = [[-155,50],[-155,-30],[22,-30],[22,50]]

        return geo_ext

    def vector_envelope(self, path):
        ds = ogr.Open(path)
        lyr_name = in_path[path.rindex('/')+1:path.rindex('.')]
        lyr = ds.GetLayerByName(lyr_name)
        env = []

        for feat in lyr:
            temp_env = feat.GetGeometryRef().GetEnvelope()
            env = self.check_envelope(temp_env, env)
            # print temp_env

        # env = [xmin, xmax, ymin, ymax]
        geo_ext = [[env[0],env[3]], [env[0],env[2]], [env[1],env[2]], [env[1],env[3]]]
        # print "final env:",env
        # print "bbox:",geo_ext

        return geo_ext


    # def vector_list(self, list=[]):
    #     env = []
    #     for file in list:
    #         f_env = vectory_envelope(file)
    #         env = self.check_envelope(f_env, env)
            
    #     geo_ext = [[env[0],env[3]], [env[0],env[2]], [env[1],env[2]], [env[1],env[3]]]
    #     return geo_ext

