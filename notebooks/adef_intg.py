#!/usr/bin/env python
# coding: utf-8

# # Configuraciones y librerias

# In[ ]:


# Librerias
import os
import time
import geopandas as gpd
import rioxarray as rxr
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import threading
from owslib.wfs import WebFeatureService
import warnings
from shapely.errors import ShapelyDeprecationWarning
import sys

# Ignorar advertencias de GeoPandas relacionadas con CRS
warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS.")

# Opcional: Ignorar advertencias de Shapely si aparecen
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

# Agregar la ruta del directorio padre al sys.path
file = os.path.abspath("adef_intg.ipynb")
sys.path.append(os.path.dirname(os.path.dirname(file)))

from src import utils

# Fechas de interes
start_date = "2022-01-01"
end_date = "2022-12-31"
url_adef_intg = (
    "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/"
    "latest/download/geotiff?grid=10/100000&tile_id=20N_090W&pixel_meaning="
    "date_conf&x-api-key=2d60cd88-8348-4c0f-a6d5-bd9adb585a8c"
)


# In[ ]:


start_time = time.time()
print(
    f"Iniciando el procesamiento de alertas integradas a las: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}"
)


# # Preparar la data para análisis

# ## Preparar los datos auxiliares

# In[ ]:


# Crear la conexion al servicio WFS
url_icf_wfs = "https://geoserver.icf.gob.hn/icfpub/wfs"
wfs_icf = WebFeatureService(url_icf_wfs, version="1.1.0")

# Obtenemos la lista de capas disponibles
layers = wfs_icf.contents
# for layer in layers:
#     print(layer)


# In[ ]:


# Layers de interes
lyr_dep = "icfpub:limite_departamentos_gp"
dep_response = wfs_icf.getfeature(typename=lyr_dep, outputFormat="application/json")
gdf_dep = gpd.read_file(dep_response)


# ## Preparar el tif por el área y fechas de interés

# In[ ]:


print("...Iniciando el enmascaramientos de las alertas integradas")
# Leer el tif de alertas
tif_adef_intg = rxr.open_rasterio(url_adef_intg, lock=True, chunks=True)

# Cortar por el extend de Honduras
if gdf_dep.crs != tif_adef_intg.rio.crs:
    gdf_dep_reproyectado = gdf_dep.to_crs(tif_adef_intg.rio.crs)
    tif_adef_intg_hn = tif_adef_intg.rio.clip_box(*gdf_dep_reproyectado.total_bounds)
    print("Se realizó clip al raster con el GeoDataFrame de departamentos reproyectado")
else:
    tif_adef_intg_hn = tif_adef_intg.rio.clip_box(*gdf_dep.total_bounds)

print("Almacenando el raster cortado en el disco")
tif_adef_intg_hn.rio.to_raster(
    "../data/adef_intg_hn.tif", compress="LZW", tiled=True, lock=threading.Lock()
)


# In[ ]:


utils.mask_adef_hn_by_forest(
    tif_forest14="../data/bosque14_lzw.tif",
    tif_forest18="../data/bosque18_lzw.tif",
    tif_forest24="../data/bosque24_lzw.tif",
    tif_adef_hn="../data/adef_intg_hn.tif",
    tif_forest14_match="../data/bosque14_lzw_match.tif",
    tif_forest18_match="../data/bosque18_lzw_match.tif",
    tif_forest24_match="../data/bosque24_lzw_match.tif",
    tif_out="../results/adef_intg_hn_forest_masked.tif",
)
print("Se realizó el enmascaramiento de las alertas integradas por el bosque")


# In[ ]:


print("...creando el vector de las alertas integradas")
# Crear gpkg de las alertas
utils.tif_to_vector(
    tif="../results/adef_intg_hn_forest_masked.tif",
    out_folder="../results",
    out_file="adef_intg.gpkg",
    layer_name="adef_intg_20250705",
)


# In[ ]:


# Agregar la fecha de la alerta y actualizar los datos de la capa
gdf = gpd.read_file("../results/adef_intg.gpkg", layer="adef_intg_20250705")
gdf = utils.calculate_decompose_date(gdf, "value", "INTEGRATED")
gdf["confidence"] = gdf["value"] // 10000
gdf.to_file(
    "../results/adef_intg.gpkg",
    layer="adef_intg_20250705",
    driver="GPKG",
    index=False,
)


# In[ ]:


time_end = time.time()
print(
    f"Finalizando el procesamiento de alertas integradas a las: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_end))}"
)
elapsed_time = time_end - start_time
hours, remainder = divmod(elapsed_time, 3600)
minutes, seconds = divmod(remainder, 60)
print(
    f"El tiempo de procesamiento fue de: {int(hours)} horas, {int(minutes)} minutos y {seconds:.2f} segundos"
)
