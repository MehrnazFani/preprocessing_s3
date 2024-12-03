import os,sys,math,time,numpy,getopt
from PreProcessing import PreProcessing
import multiprocessing

# creating preprocessing object
preprocessing = PreProcessing() 
workers = multiprocessing.cpu_count()

buffer_dist = 0.35 # for LaneMarker = 0.35, for RoadSurface = 0
project_name = "LaneMarker_Solids_Detection"
datasets_dir = "datasets_vertical"
city_name = "san-francisco-california"
tile_percnet = "100" 
annot_percent = "2"
tile_info_file_name = "0_generated_tiles.geojson"
annotation_file_name = "san_francisco_LaneMarkers_Solids_annotations.geojson"
boundary_file_name = "San_francisco_march2021_boundary.geojson"
month = "und"
buffer_size = "0.35" # for LaneMarker = "0.35", for RoadSurface = "0"

# path/to/tiles.geojson on s3, excluding s3://bucket_name/
tiles_info_adrs = f"{datasets_dir}/{city_name}/city-images/{month}/tiles/{tile_percnet}_percent/1250_150/{tile_info_file_name}"

# path/to/annotations.geojson on s3, excluding s3://bucket_name/
spatial_data_adrs = f"{datasets_dir}/{city_name}/verified-files/{month}/{project_name}/{annot_percent}_percent/{annotation_file_name}" 

# path/to/boundary.geojson on s3, excluding s3://bucket_name/
boundary_adrs = f"{datasets_dir}/{city_name}/verified-files/{month}/{project_name}/{annot_percent}_percent/{boundary_file_name}"

# directory/to/save/masks on s3, excluding s3://bucket_name/
output_path = f"{datasets_dir}/{city_name}/masks/{month}/{project_name}/{annot_percent}_percent/1250_150/{buffer_size}/"
city_name = ''# it adds prefix to name of the masks so leave it empty
check_boundary = True

print("Tiles info filename is "+str(tiles_info_adrs)+", and data filename is "+spatial_data_adrs+", and boundary filename is "+boundary_adrs+", and Output directory is "+output_path)
print("Buffer value is "+ str(buffer_dist)+", and boundary check is "+ str(check_boundary)+", and number of workers is "+str(workers) + ", and city name is "+ city_name)

# creating masks
preprocessing.create_masks_multiprocessing(int(workers),tiles_info_adrs,output_path,city_name,boundary_adrs, check_boundary,float(buffer_dist),spatial_data_adrs)

# calculating processing time
preprocessing.processing_time()

#creating output metadata file
preprocessing.get_images_info_multiprocessing(int(workers),output_path,output_path+'generated_tiles.geojson')

