"""Test unit for try to communicate with wfs firepoints NASA servise."""


import sys

try:
    from osgeo import ogr, gdal
    # from osgeo import osr
except Exception as err:
    sys.exit(f'ERROR: cannot find GDAL/OGR modules. {err}')

# Set the driver (optional)
wfs_drv = ogr.GetDriverByName('WFS')

# Speeds up querying WFS capabilities for services with alot of layers
gdal.SetConfigOption('OGR_WFS_LOAD_MULTIPLE_LAYER_DEFN', 'NO')

# Set config for paging.
# Works on WFS 2.0 services and WFS 1.0 and 1.1 with some other services.
gdal.SetConfigOption('OGR_WFS_PAGING_ALLOWED', 'YES')
gdal.SetConfigOption('OGR_WFS_PAGE_SIZE', '10000')

# Open the webservice
url = "https://firms.modaps.eosdis.nasa.gov/mapserver/wfs/Russia_Asia/"\
      "2dee5fba9c79a026daf0e4507e0423ee/"
wfs_ds = wfs_drv.Open('WFS:' + url)
if not wfs_ds:
    sys.exit('ERROR: can not open WFS datasource')
else:
    pass

# iterate over available layers
for i in range(wfs_ds.GetLayerCount()):
    layer = wfs_ds.GetLayerByIndex(i)
    srs = layer.GetSpatialRef()
    name = layer.GetName()
    fc = layer.GetFeatureCount()
    sr = srs.ExportToWkt()[0:50]
    print(f"Layer: {name}, Features: {fc}, SR: {sr}...")

    # iterate over features
    feat = layer.GetNextFeature()
    while feat is not None:
        feat = layer.GetNextFeature()
        # do something more..
    feat = None

# Get a specific layer
layer = wfs_ds.GetLayerByName("largelayer")
if not layer:
    sys.exit('ERROR: can not find layer in service')
else:
    pass
