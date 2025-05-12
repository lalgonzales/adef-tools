"""This module contains the function to process the integrated alerts"""

# adef_intg/adef_intg_fn.py
# -*- coding: utf-8 -*-
# Librerias
import os
import time
import warnings

import threading
import geopandas as gpd
import rioxarray as rxr
from shapely.errors import ShapelyDeprecationWarning
from adef_intg import utils_adef

# Ignorar advertencias de GeoPandas relacionadas con CRS
warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS.")

# Opcional: Ignorar advertencias de Shapely si aparecen
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)


def run_adef_process(
    confidence,
    out_folder,
    out_file,
    layer_name,
    start_date,
    end_date,
    base_dir,
    lock_read=None,
    lock_write=None,
):
    """
    Run the ADEF integrated alerts processing pipeline.

    Args:
        confidence (float): Confidence level for filtering alerts.
        out_folder (Path): Path to the output folder where results will be saved.
        out_file (str): Name of the output file (e.g., GeoPackage file).
        layer_name (str): Name of the layer to be created in the output file.
        start_date (str): Start date for filtering alerts (format: 'YYYY-MM-DD').
        end_date (str): End date for filtering alerts (format: 'YYYY-MM-DD').
        base_dir (Path): Base directory containing necessary input data.

    Returns:
        geopandas.GeoDataFrame: Processed GeoDataFrame containing the integrated alerts.
    """
    start_time = time.time()

    print(
        f"🚀 Iniciando el procesamiento de alertas integradas a las: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))} 🌍"
    )
    # Definir la URL de las alertas
    url_adef_intg = (
        "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/"
        "latest/download/geotiff?grid=10/100000&tile_id=20N_090W&pixel_meaning="
        "date_conf&x-api-key=2d60cd88-8348-4c0f-a6d5-bd9adb585a8c"
    )

    # Descargar los tif de bosque
    TIFS = {
        "bosque14_lzw": "https://git.icf.gob.hn/alopez/adef-integ-tools/-/raw/main/data/bosque14_lzw.tif",
        "bosque18_lzw": "https://git.icf.gob.hn/alopez/adef-integ-tools/-/raw/main/data/bosque18_lzw.tif",
        "bosque24_lzw": "https://git.icf.gob.hn/alopez/adef-integ-tools/-/raw/main/data/bosque24_lzw.tif",
    }
    for tif_name, tif_url in TIFS.items():
        tif_path = base_dir / "data" / f"{tif_name}.tif"
        if not tif_path.exists():
            print(f"Descargando {tif_name}...")
            utils_adef.dw_tif(
                url=tif_url,
                tif_out=tif_path,
            )
            print(f"{tif_name} descargado y guardado en {tif_path}")
        else:
            print(f"{tif_name} ya existe en {tif_path}, se omitirá la descarga.")

    # Preparar la data para análisis
    ## Preparar los datos auxiliares
    # Crear la conexion al servicio WFS
    url_icf_wfs = "https://geoserver.icf.gob.hn/icfpub/wfs"

    # Obtener el GeoDataFrame de los departamentos de Honduras
    lyr_dep = "icfpub:limite_departamentos_gp"
    gdf_dep = utils_adef.get_wfs_layer(
        url_icf_wfs,
        lyr_dep,
        version="1.1.0",
    )

    # Preparar el tif por el área y fechas de interés
    print("...Iniciando el enmascaramientos de las alertas integradas")
    # Leer el tif de alertas
    tif_adef_intg = rxr.open_rasterio(url_adef_intg, lock=lock_read, chunks=True)

    # Cortar por el extend de Honduras
    tif_adef_intg_clipped = utils_adef.clip_tif_to_ext(
        tif_adef_intg,
        gdf_dep,
        lock_read=lock_read,
        lock_write=lock_write,
    )
    tif_adef_intg_clipped.name = "adef_intg_clipped"
    if start_date and end_date:
        print("...Filtrando por fechas")
        utils_adef.filter_adef_intg_time(
            tif_adef_intg_clipped,
            ("Range", start_date, end_date),
            base_dir / "data/adef_intg_clipped.tif",
            lock_read=lock_read,
            lock_write=lock_write,
        )
        # tif_clipped = result[0]
        print(f"Se realizó el filtrado por fechas {start_date} - {end_date}")
    else:
        print("...almacenando el tif sin filtrar por fechas")
        tif_adef_intg_clipped.rio.to_raster(
            base_dir / "data/adef_intg_clipped.tif",
            tiled=True,
            lock=lock_write or threading.Lock(),
            compress="DEFLATE",
        )
        # tif_clipped = tif_adef_intg_clipped
        print("No se filtro por fechas")

    utils_adef.mask_adef_hn_by_forest(
        tif_forest18=base_dir / "data/bosque18_lzw.tif",
        tif_forest24=base_dir / "data/bosque24_lzw.tif",
        tif_forest14=base_dir / "data/bosque14_lzw.tif",
        # tif_adef_roi=tif_clipped,
        tif_adef_roi=base_dir / "data/adef_intg_clipped.tif",
        tif_forest14_match=base_dir / "data/bosque14_lzw_match.tif",
        tif_forest18_match=base_dir / "data/bosque18_lzw_match.tif",
        tif_forest24_match=base_dir / "data/bosque24_lzw_match.tif",
        tif_out=base_dir / "results/adef_intg_forest_masked.tif",
        confidence_integ=confidence,
        chunks=True,
        lock_read=lock_read,
        lock_write=lock_write,
    )
    print("Se realizó el enmascaramiento de las alertas integradas por el bosque")

    # Crear el vector de alertas
    print("...creando el vector de las alertas integradas")

    # Crear gpkg de las alertas
    tmp_file = f"tmp_{out_file}"
    utils_adef.tif_to_vector(
        tif=base_dir / "results/adef_intg_forest_masked.tif",
        out_folder=out_folder,
        out_file=tmp_file,
        layer_name=layer_name,
    )
    # Agregar la fecha de la alerta y actualizar los datos de la capa
    gdf = gpd.read_file(os.path.join(out_folder, tmp_file), layer=layer_name)
    print("...agregando la fecha de la alerta y la confianza")
    gdf = utils_adef.calculate_decompose_date(gdf, "value", "INTEGRATED")
    gdf["confidence"] = gdf["value"] // 10000
    gdf = utils_adef.sanitize_gdf_dtypes(gdf)
    gdf.to_file(
        out_folder / out_file,
        layer=layer_name,
        driver="GPKG",
        index=False,
        mode="w",
        metadata={"script": "adef_intg"},
    )
    os.remove(os.path.join(out_folder, tmp_file))
    # Clean duplicates if any
    # gdf = gpd.read_file(out_folder / out_file, layer=layer_name)
    # gdf = gdf.drop_duplicates(subset=["geometry"])
    # gdf = gdf.reset_index(drop=True)
    # gdf.to_file(
    #     out_folder / out_file,
    #     layer=layer_name,
    #     driver="GPKG",
    #     index=False,
    #     mode="w",
    #     metadata={"script": "adef_intg"},
    # )
    time_end = time.time()
    print(
        f"Finalizando el procesamiento de alertas integradas a las: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time_end))}"
    )
    elapsed_time = time_end - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print("===============================")
    print(
        f"El tiempo de procesamiento fue de: {int(hours)} horas, {int(minutes)} minutos y {seconds:.2f} segundos"
    )
    print("===============================")
