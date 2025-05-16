"""This module contains the function to process the integrated alerts"""

# adef_intg/adef_intg_fn.py
# -*- coding: utf-8 -*-
# Librerias
import os
import time
import warnings

import geopandas as gpd
from shapely.errors import ShapelyDeprecationWarning
from adef_tools import utils_adef

# Ignorar advertencias de GeoPandas relacionadas con CRS
warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS.")

# Opcional: Ignorar advertencias de Shapely si aparecen
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

# Determinar los departartamentos de Honduras como vector default obtenido del WFS de ICF


def run_adef_process(
    vector,
    confidence,
    out_folder,
    out_file,
    layer_name,
    start_date,
    end_date,
    base_dir,
    lock_read=None,
    lock_write=None,
    chunks_default=True,
):
    """
    Run the ADEF integrated alerts processing pipeline.

    Args:
        vector (str or Path): Path to the vector file (area of interest) used for clipping.
        confidence (float): Confidence level for filtering alerts.
        out_folder (Path): Path to the output folder where results will be saved.
        out_file (str): Name of the output file (e.g., GeoPackage file).
        layer_name (str): Name of the layer to be created in the output file.
        start_date (str): Start date for filtering alerts (format: 'YYYY-MM-DD').
        end_date (str): End date for filtering alerts (format: 'YYYY-MM-DD').
        base_dir (Path): Base directory containing necessary input data.
        lock_read (threading.Lock, optional): Lock for reading files in multithreaded environments.
        lock_write (threading.Lock, optional): Lock for writing files in multithreaded environments.
        chunks_default (bool, optional): Whether to use chunked reading/writing for large files.

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
    utils_adef.dw_tif(
        url=url_adef_intg,
        tif_out=base_dir / "data" / "adef_intg.tif",
    )
    # Descargar los tif de bosque
    TIFS = {
        "bosque14_lzw": "https://git.icf.gob.hn/alopez/adef-integ-tools/-/raw/main/data/bosque14_lzw.tif",
        "bosque18_lzw": "https://git.icf.gob.hn/alopez/adef-integ-tools/-/raw/main/data/bosque18_lzw.tif",
        "bosque24_lzw": "https://git.icf.gob.hn/alopez/adef-integ-tools/-/raw/main/data/bosque24_lzw.tif",
    }

    print("...Descargando las máscaras de bosque")
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

    # Preparar el tif por el área y fechas de interés
    print("...Iniciando el enmascaramientos de las alertas integradas")
    utils_adef.clip_tif_gdal(
        base_dir / "data" / "adef_intg.tif",
        vector,
        base_dir / "data" / "adef_intg_clipped.tif",
    )

    periods = [
        ("2018-01-01", "2023-12-31"),
        ("2024-01-01", time.strftime("%Y-%m-%d", time.localtime(time.time()))),
    ]

    tif_periods = utils_adef.divide_intg_for_forest(
        base_dir / "data" / "adef_intg_clipped.tif",
        periods,
        lock_read=lock_read,
        lock_write=lock_write,
        # out_folder=base_dir / "data",
    )

    forests_masks_name = ["bosque14_lzw", "bosque18_lzw", "bosque24_lzw"]
    forests_masks = [
        base_dir / "data" / f"{forest_name}.tif" for forest_name in forests_masks_name
    ]

    # Enmascarar el tif de alertas integradas por el bosque
    print("...Enmascarando el tif de alertas integradas por el bosque")
    tifs_masked = {}
    # out_folder_tif_masked = base_dir / "data"
    out_folder_tif_masked = None
    for tif_name_period, tif_period in tif_periods.items():
        tif_name = tif_name_period
        if out_folder_tif_masked is not None:
            tif_data, _ = utils_adef.validate_setting_tif(
                base_dir / "data" / f"{tif_name}.tif",
                lock_read=lock_read,
                chunks=chunks_default,
            )
        else:
            tif_data = tif_period
            tif_data.name = tif_name

        tif_masked = utils_adef.mask_by_forest(
            tif_data,
            forests_masks,
            lock_read=lock_read,
            lock_write=lock_write,
        )
        tif_name_masked = tif_name.replace("clipped", "masked")
        tifs_masked[tif_name_masked] = tif_masked

    # Create the tif masked
    tifs_list = list(tifs_masked.values())
    n_tifs = len(tifs_list)

    tif_masked = tifs_list[0]
    for i in range(1, n_tifs):
        tif_masked = tif_masked + tifs_list[i]
    tif_masked.name = "adef_intg_forest_masked"

    if start_date and end_date:
        print(f"...Filtrando por las fechas {start_date} - {end_date}")
        tif_masked, _, _ = utils_adef.filter_adef_intg_time(
            tif_masked,
            ("Range", start_date, end_date),
            chunks=chunks_default,
            lock_read=lock_read,
        )
        tif_masked.name = "adef_intg_forest_masked"
    print("...filtrando por la confianza y almacenando el tif enmascarado")
    tif_masked = utils_adef.filter_adef_intg_conf(
        tif_masked,
        confidence,
        tif_out=base_dir / "results" / "adef_intg_forest_masked.tif",
        lock_read=lock_read,
        lock_write=lock_write,
        chunks=chunks_default,
    )
    print("Se realizó el enmascaramiento de las alertas integradas por el bosque")

    # Crear el vector de alertas
    print("...creando el vector de las alertas integradas")

    # Crear gpkg de las alertas
    tmp_file = f"tmp_{out_file}"
    driver = out_file.split(".")[-1].lower()
    if driver == "shp":
        driver = "ESRI Shapefile"
    utils_adef.tif_to_vector(
        tif=base_dir / "results/adef_intg_forest_masked.tif",
        out_folder=out_folder,
        out_file=tmp_file,
        layer_name=layer_name,
    )
    # Agregar la fecha de la alerta y actualizar los datos de la capa
    print("...agregando la fecha de la alerta y la confianza")
    try:
        if driver == "parquet":
            gdf = gpd.read_parquet(out_folder / tmp_file)
        elif driver == "feather":
            gdf = gpd.read_feather(out_folder / tmp_file)
        elif "ESRI" in driver:
            gdf = gpd.read_file(out_folder / tmp_file)
        else:
            gdf = gpd.read_file(out_folder / tmp_file, layer=layer_name)
    except Exception as e:
        print(f"Error al leer el archivo de geometrias: {e}")
        raise
    gdf[gdf.geometry.name] = gdf.normalize()
    gdf = utils_adef.calculate_decompose_date(gdf, "value", "INTEGRATED")
    gdf["confidence"] = gdf["value"] // 10000
    gdf = utils_adef.sanitize_gdf_dtypes(gdf)
    print("...comparando con data existente para remover duplicados")
    if os.path.exists(out_folder / out_file):
        if driver == "gpkg":
            layers = gpd.list_layers(out_folder / out_file)
            layers = layers["name"].to_list()
            if layer_name in layers:
                gdf_exists = gpd.read_file(out_folder / out_file, layer=layer_name)
            else:
                gdf_exists = gpd.GeoDataFrame()
                return
        elif driver == "parquet":
            gdf_exists = gpd.read_parquet(out_folder / out_file)
        elif driver == "feather":
            gdf_exists = gpd.read_feather(out_folder / out_file)
        else:
            gdf_exists = gpd.read_file(out_folder / out_file)

        # Clean the geometry column
        gdf_exists[gdf_exists.geometry.name] = gdf_exists.normalize()
        gdf_cleaned = gdf_exists.drop_duplicates(subset=["geometry"])
        if gdf_cleaned.shape[0] < gdf_exists.shape[0]:
            gdf_cleaned = gdf_cleaned.reset_index(drop=True)
            gdf_cleaned.to_file(out_folder / out_file, layer=layer_name, mode="w")
            print(
                f"Se eliminaron {gdf_exists.shape[0] - gdf_cleaned.shape[0]} duplicados del vector"
            )
            gdf = gpd.read_file(
                out_folder / out_file,
                layer=layer_name,
            )
    else:
        gdf_exists = gpd.GeoDataFrame()

    if gdf_exists.shape[0] == 0:
        if driver == "parquet":
            gdf.to_parquet(out_folder / out_file, index=False)
        elif driver == "feather":
            gdf.to_feather(out_folder / out_file, index=False)
        elif "ESRI" in driver:
            gdf.to_file(
                out_folder / out_file,
                driver=driver,
                index=False,
                mode="w",
            )
        else:
            gdf.to_file(
                out_folder / out_file,
                layer=layer_name,
                driver=driver,
                index=False,
                mode="w",
            )
        print("Se agreganron todas las alertas integradas al vector")
    else:
        gdf_diff = gdf[~gdf.geometry.isin(gdf_exists.geometry)]
        if gdf_diff.shape[0] > 0:
            gdf_diff = gdf_diff.reset_index(drop=True)
            gdf_diff.to_file(
                out_folder / out_file,
                layer=layer_name,
                driver=driver,
                index=False,
                mode="a",
            )
            print(f"Agregando {gdf_diff.shape[0]} nuevas alertas integradas al vector")
        else:
            print("No hay nuevas alertas integradas para agregar al vector")

    os.remove(os.path.join(out_folder, tmp_file))

    # Limpiar archvivos temporales
    utils_adef.clean_files(base_dir)
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
