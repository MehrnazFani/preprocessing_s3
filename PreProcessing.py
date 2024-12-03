import rasterio
from rasterio import features
from rasterio.transform import Affine
from rasterio.crs import CRS
import os,sys,math,time
import rasterio,affine,json
from rtree import index
from pathlib import Path
from time import process_time
from multiprocessing import Pool
from shapely.geometry import LineString,Polygon
import geopandas as gpd
import datetime
import boto3
from io import BytesIO

# Initialize the S3 client
s3 = boto3.client('s3')
bucket_name = "geomate-data-repo-dev"

class PreProcessing:   
    # the construction method records start time of processing for final calculation of processing time
    def __init__(self):
        self.start_time = datetime.datetime.now()
        
        
    # this function calculates the difference of the time method is called and the construction time of PreProcessing object
    def processing_time(self):
        d = datetime.datetime.now() - self.start_time
        print('Processing time: ' + str(d.seconds) + ' secs')
    
    # this method creates masks from tiles information file, boundary in which create masks and spatial data for rasterization
    # data need to be buffered when input data is linestring or point. if buffer_dist is zero, no buffer operation will be performed
    def create_masks_multiprocessing(self,workers,tiles_info_adrs,output_path,city_name,boundary_adrs,check_boundary,buffer_dist,spatial_data_adrs):
        # output directory will be generated if it doesn't exist
        Path(output_path).mkdir(parents=True, exist_ok=True)
        #s3.put_object(Bucket=bucket_name, Key=output_path)  # Simulate folder creation in S3 by uploading an empty object

        # if city_name is not empty it will be prepended to the output mask name
        if city_name != '':
            self.output_path = output_path + city_name + '_'
        else:
            self.output_path = output_path
            
        # spatial data and boundary files will be reprojected to tiles info file projection system
        #self.tiles = gpd.GeoDataFrame.from_file(tiles_info_adrs)
        s3_key = tiles_info_adrs
        response = s3.get_object(Bucket=bucket_name, Key=s3_key) # Fetch the file from S3 into memory
        file_data = response['Body'].read()  # Read the content of the file into memory
        file_like_object = BytesIO(file_data) # Use BytesIO to treat the in-memory content as a file-like object
        self.tiles = gpd.read_file(file_like_object) # Read the GeoJSON (or other vector format) directly into a GeoDataFrame
        
        #gs = gpd.GeoSeries.from_file(spatial_data_adrs)
        s3_key = spatial_data_adrs  # Change the file path accordingly
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)  # Fetch the file from S3 into memory
        file_data = response['Body'].read()  # Read the content of the file into memory
        file_like_object = BytesIO(file_data) # Use io.BytesIO to treat the in-memory content as a file-like object
        gs = gpd.read_file(file_like_object).geometry # Read the GeoJSON (or other vector format) directly into a GeoSeries

        print('Tile crs is '  + str(self.tiles.crs) + ', and data crs is ' + str(gs.crs))

        gs = gs.to_crs(self.tiles.crs)
        
        
        if buffer_dist>0:
            gs = gs.buffer(buffer_dist)

        self.shapes = [ [feature,255] for feature in gs  ]
        # spatial data will be indexed and stored in a file.
        # previously generated index file will be removed
        try:
            if os.path.isfile('./rtree.idx.dat') or os.path.islink('./rtree.idx.dat'):
                os.unlink('./rtree.idx.dat')  
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % ('./rtree.idx.dat', e))
        try:
            if os.path.isfile('./rtree.idx.idx') or os.path.islink('./rtree.idx.idx'):
                os.unlink('./rtree.idx.idx')   
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % ('./rtree.idx.idx', e))
        # creating index
        bounds_index = index.Index('./rtree.idx')
        for i in range(len(self.shapes)):
            bounds_index.insert(i, self.shapes[i][0].bounds)
        bounds_index.close()

        indxs = range(len(self.tiles['geometry']))
        
        # tiles will be checked to be within the boundary file if exists
        if check_boundary:
            #boundary = gpd.GeoSeries.from_file(boundary_adrs).to_crs(self.tiles.crs)
            s3_key = boundary_adrs  # Change the file path accordingly
            response = s3.get_object(Bucket=bucket_name, Key=s3_key)  # Fetch the file from S3 into memory
            file_data = response['Body'].read()  # Read the content of the file into memory
            file_like_object = BytesIO(file_data) # Use io.BytesIO to treat the in-memory content as a file-like object
            boundary = gpd.read_file(file_like_object).to_crs(self.tiles.crs)# Read the GeoJSON (or other vector format) directly into a GeoSeries

            indxs = [ i for i in range(len(self.tiles['geometry'])) if any(boundary.contains(self.tiles['geometry'][i])) ]
        # creating masks in multiprocess mode    
        with Pool(processes=workers) as pool:
            pool.map(self._rasterize_vector, indxs)
            
            
    # this methods collects raster files metadata information for further use or check
    def get_images_info_multiprocessing(self, workers, in_dir, out_name):
        # we don't consider boundary check so it will be false
        self.check_boundary = False
        # Since we are going to collect the metadata only, we put high_memory mode to false. 
        # In this mode rasters data are not returned in "_get_metadata" method
        self.high_memory = False
        self.in_dir = in_dir
        ### Fetching name of images in input directory; reading boundary file
        fnames = [ name for name in os.listdir(in_dir) if ('.jp2' in name or '.tif' in name or '.sid' in name) and '.tif.' not in name and '.sid.' not in name]
        
        ### Gathering images properties
        # '_get_metadata' method reads rasters and returns their metadata
        print('Gathering images properties')
        with Pool(processes=workers) as pool:
            rasters_info = pool.map(self._get_metadata, fnames)
        
        rasters_info = [ info for info in rasters_info if info is not None ]

        tiles_info = {'geometry':[],'crs':[],"pixel_size":[],"zoom_level": [],"fname": [],"width": [], "height": [], 'area_sqkm':[], 'meta':[]}
        
        # collecting gathered metatda and storing in geojson format
        # boundary of each raster will be collected and stored in geometry field
        for raster in rasters_info:
            if raster is not None:
                [left, bottom, right, top] = raster['bounds']
                polyline = LineString([[left,bottom],[left,top],[right,top],[right,bottom],[left,bottom]])
                tiles_info['geometry'].append(polyline)
                tiles_info['area_sqkm'].append(round(raster['width']*raster['height']*raster['pixel_size']**2/1000000,3))
                tiles_info['crs'].append(str(raster['crs']))
                tiles_info['pixel_size'].append(round(raster['pixel_size'],3))
                tiles_info['zoom_level'].append(round(raster['zoom_level'],2))
                tiles_info['fname'].append(raster['fname'])
                tiles_info['width'].append(raster['width'])
                tiles_info['height'].append(raster['height'])
                tiles_info['meta'].append(raster['meta'])
        
        gpd.GeoDataFrame(tiles_info, crs=tiles_info['crs'][0]).to_file(out_name)

        # os.makedirs(path, exist_ok=True)
        # create path on s3
        s3.put_object(Bucket=bucket_name, Key=out_name)  # Simulate folder creation in S3 by uploading an empty object
        # copy the local masks to s3
        # Check if the current path is a file (not a directory)
        local_path = out_name
        for file in os.listdir(in_dir):
            # local path
            local_path = f"{in_dir}{file}" 
            # Construct the full S3 path
            s3_key = f"{in_dir}{file}"
            # Upload the file
            s3.upload_file(local_path, bucket_name, s3_key)
        import shutil
        shutil.rmtree(in_dir)
      
    # this method return metadata of raster file. data will be returned, if memory_mode is high.
    def _get_metadata(self,fname):
        
        try:
            #with rasterio.open(s3_path) as src:
            with rasterio.open(self.in_dir+fname) as src:
                [left, bottom, right, top] = src.bounds
                polygon = Polygon([(left, bottom), (left, top), (right,top), (right,bottom)])

                # if file is not within boundary, method will return null
                if self.check_boundary:
                    if any(self.boundary.intersects(polygon))==False:
                        return None

                data = None

                if self.high_memory:
                    data = src.read()

                pixel_size = (abs(src.transform[0]) + abs(src.transform[4]))/2
                zoom_level = 20 - math.log(pixel_size/0.14929107087105511,2)
                meta = src.meta.copy()
                meta['crs']=str(meta['crs'])
                transform = meta['transform']
                meta['transform'] = ','.join([str(val) for val in  [transform.a,transform.b,transform.c,transform.d,transform.e,transform.f] ])
            
        except Exception as e: 
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(e, fname, exc_tb.tb_lineno)
                return None


        return { "data": data ,"crs":src.crs, "pixel_size": pixel_size, "zoom_level": zoom_level, "fname": fname, "meta": json.dumps(meta),
                "bounds": src.bounds, "transform": src.transform, "width": src.width, "height": src.height  }
    
    
    # this method rasterize the geometry within its boundary
    def _rasterize_vector(self,i):
        
        # reading index file of spatial data to rasterize
        bounds_index = index.Index('./rtree.idx')
        meta = self.tiles['meta'][i]
        polygon = Polygon(self.tiles['geometry'][i].coords)

        meta = json.loads(meta)
        # print(meta)

        affine_params = [float(val) for val in meta['transform'].split(',')]
        meta['transform'] = Affine(affine_params[0],affine_params[1],affine_params[2],affine_params[3],affine_params[4],affine_params[5])
        meta['crs'] = CRS.from_string(meta['crs'])
        
        # searching index for spatial data within tile boundary to rasterize
        this_shapes = [self.shapes[j] for j in list(bounds_index.intersection(polygon.bounds)) if self.shapes[j][0].intersects(polygon)]
        
        with rasterio.open(self.output_path+self.tiles['fname'][i], 'w+', **meta) as out:

            out_arr = out.read(1)
            if len(this_shapes)>0:
                out_arr = features.rasterize(shapes=this_shapes, fill=0, out=out_arr, transform=out.transform)
                
            out.write_band(1, out_arr)
            out.write_band(2, out_arr)
            out.write_band(3, out_arr)



