"""This module contains functions for processing and analyzing raster data related to deforestation alerts."""

import os
import sys
import subprocess
import glob
import threading
import pathlib
from pathlib import Path
import rioxarray as rxr
import xarray as xr
import pandas as pd
import geopandas as gpd

# from osgeo import gdal
from owslib.wfs import WebFeatureService

if getattr(sys, "frozen", False):
    BASE_DIR = Path.cwd()
else:
    BASE_DIR = Path(__file__).resolve().parent.parent


def calculate_decompose_date(gdf, gridcode, adef_src, year=None):
    """
    Calculates and decomposes dates based on grid codes and source type.

    Args:
        gdf (GeoDataFrame): A GeoDataFrame containing the data to process.
        gridcode (str): The column name in the GeoDataFrame containing the grid codes.
        adef_src (str): The source type of the data. Can be "GLAD" or "INTEGRATED".
        year (int, optional): The year to use for "GLAD" source type. Defaults to None.

    Raises:
        ValueError: If invalid parameters are provided for the specified source type.

    Returns:
        GeoDataFrame: The updated GeoDataFrame with decomposed date information.
    """
    days_of_week = [
        "LUNES",
        "MARTES",
        "MIÉRCOLES",
        "JUEVES",
        "VIERNES",
        "SÁBADO",
        "DOMINGO",
    ]
    months_of_year = [
        "ENERO",
        "FEBRERO",
        "MARZO",
        "ABRIL",
        "MAYO",
        "JUNIO",
        "JULIO",
        "AGOSTO",
        "SEPTIEMBRE",
        "OCTUBRE",
        "NOVIEMBRE",
        "DICIEMBRE",
    ]

    try:
        if adef_src == "GLAD" and year is not None:
            start_of_year = pd.Timestamp(f"{year}-01-01")
            gdf["fecha"] = start_of_year + pd.to_timedelta(gdf[gridcode] - 1, unit="D")
            gdf["anio"] = year
        elif adef_src == "INTEGRATED" and year is None:
            zero_day = pd.Timestamp("2014-12-31")
            gdf["fecha"] = zero_day + pd.to_timedelta(gdf[gridcode] % 10000, unit="D")
            gdf["anio"] = gdf["fecha"].dt.year
        else:
            raise ValueError(
                "Parámetros inválidos: Para 'GLAD', se necesita 'year'. Para 'INTEGRATED', solo 'adef_src'."
            )

        # Descomponer fecha con métodos vectorizados
        gdf["mes"] = gdf["fecha"].dt.month.map(lambda m: months_of_year[m - 1])
        gdf["dia"] = gdf["fecha"].dt.weekday.map(lambda d: days_of_week[d])
        gdf["semana"] = gdf["fecha"].dt.isocalendar().week

        return gdf

    except Exception as e:
        print(f"Error procesando fechas: {e}")
        raise


def tif_to_vector(tif, out_folder, out_file="vector.gpkg", layer_name=None):
    """
    Converts a raster file (TIF) to a vector file (GeoPackage).

    Args:
        tif (str): Path to the input raster file to be converted.
        out_folder (str): Path to the folder where the output vector file will be saved.
        name_out (str, optional): Name of the output vector file, including the extension.
            Supported formats are GeoPackage (.gpkg), GeoJSON (.json), or Shapefile (.shp).
            Defaults to "vector.gpkg".
        layer_name (str, optional): Name of the layer in the output vector file.
            If not provided, it defaults to the name of the output file without the extension.

    Raises:
        FileNotFoundError: If the input raster file does not exist.

    Returns:
        None
    """
    tif_name = os.path.basename(tif).split(".")[:-1][0]
    if not os.path.exists(tif):
        raise FileNotFoundError(f"The input TIF file does not exist: {tif_name}")

    print(f"Converting {tif_name} to vector...")

    if layer_name is None:
        layer_name = os.path.basename(out_file).split(".")[:-1][0]
    driver = out_file.split(".")[-1].lower()
    if driver == "shp":
        driver = "'ESRI Shapefile'"
    out_vector = os.path.join(out_folder, out_file)
    polygonize_path = get_gdal_polygonize_path()
    print(f"using {polygonize_path} to convert the raster to vector")
    command = (
        ["python"]
        + [polygonize_path]
        + [tif]
        + [out_vector]
        + [layer_name]
        + ["value"]
        + ["-of", driver]
        + ["-overwrite"]
        + ["-mask", tif]
    )
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running gdal_polygonize for {tif_name}: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error while running gdal_polygonize for {tif_name}: {e}")
        raise
    print(f"Vector file saved as {out_file} in {out_folder}")


def filter_adef_intg_time(
    tif, fiter_time, tif_out=None, chunks="auto", lock_read=True, lock_write=None
):
    """
    Filters a raster file (TIF) based on a specified time filter.

    Parameters:
        tif (str or xarray.DataArray): Path to the input TIF file or an xarray DataArray.
        fiter_time (tuple): A tuple specifying the filter type and parameters.
            - For "Last": ("Last", quantity, unit), where `unit` can be "Days", "Months", or "Years".
            - For "Range": ("Range", start_date, end_date), where `start_date` and `end_date` are in "YYYY-MM-DD" format.
        tif_out (str, optional): Path to save the output filtered TIF file. If None, the filtered TIF is not saved. Defaults to None.
        chunks (str, int, dict, or bool, optional): Chunk size for reading the raster files. Defaults to "auto".
            - "auto": Automatically determines the chunk size based on file size and system memory.
            - int: Specifies the size of chunks in pixels.
            - dict: Allows specifying chunk sizes for each dimension (e.g., {"x": 512, "y": 512}).
            - False: Disables chunking and reads the entire file into memory.

    Raises:
        FileNotFoundError: If the input TIF file does not exist.
        ValueError: If the filter type or unit is invalid.
        TypeError: If the TIF is neither a string nor an xarray DataArray.

    Returns:
        xarray.DataArray: The filtered TIF as an xarray DataArray. If `tif_out` is provided, the filtered TIF is also saved to the specified path.
    """

    try:
        # Validate and load the TIF data
        if isinstance(tif, (str, pathlib.Path)):
            tif_name = os.path.basename(tif).split(".")[:-1][0]
            if not os.path.exists(tif):
                raise FileNotFoundError(
                    f"The input TIF file does not exist: {tif_name}"
                )
            tif_data = rxr.open_rasterio(tif, chunks=chunks, lock=lock_read)
        elif isinstance(tif, xr.DataArray):
            if tif.name is not None:
                tif_name = tif.name
                tif_data = tif
            else:
                raise ValueError(
                    "The TIF must have a name. Assign one using `data.name = name`."
                )
        else:
            raise TypeError("The TIF must be a string or an xarray DataArray.")

        # Define the zero day and calculate days from the TIF data
        zero_day = pd.Timestamp("2014-12-31")
        tif_in_days = tif_data % 10000

        # Determine the filter type and apply the corresponding logic
        filter_type = fiter_time[0]
        if filter_type == "Last":
            filter_quantity = fiter_time[1]
            filter_units = fiter_time[2]

            days_to_last = tif_in_days.max().values.item()
            filter_end = zero_day + pd.Timedelta(days=days_to_last)
            if filter_units == "Days":
                difference_days = filter_quantity
                days_to_first = days_to_last - difference_days
            elif filter_units == "Months":
                difference_days = (
                    filter_end - pd.DateOffset(months=filter_quantity)
                ).days
                days_to_first = days_to_last - difference_days
            elif filter_units == "Years":
                difference_days = (
                    filter_end - pd.DateOffset(years=filter_quantity)
                ).days
                days_to_first = days_to_last - difference_days
            else:
                raise ValueError(
                    f"Invalid unit time: {filter_units}\nValid units are: Days, Months, Years"
                )
            filter_start = zero_day + pd.Timedelta(days=days_to_first)
            mask_time = tif_in_days >= days_to_first
            print(f"...Filtering the TIF from {filter_start} to {filter_end}...")
            tif_filtered = tif_data.where(mask_time)
            tif_filtered.name = tif_name

        elif filter_type == "Range":
            filter_start = pd.Timestamp(fiter_time[1])
            filter_end = pd.Timestamp(fiter_time[2])
            days_to_first = (filter_start - zero_day).days
            days_to_last = (filter_end - zero_day).days
            mask_time = (tif_in_days >= days_to_first) & (tif_in_days <= days_to_last)
            print(
                f"...Filtering the TIF for the range {filter_start} to {filter_end}..."
            )
            tif_filtered = tif_data.where(mask_time)
            tif_filtered.name = tif_name
        else:
            raise ValueError(
                f"Invalid filter type: {filter_type} \nValid types are: Last, Range"
            )

        # Save the filtered TIF if an output path is provided
        if tif_out is not None:
            try:
                print(f"Saving the filtered TIF as {tif_out}...")
                if os.path.exists(tif_out):
                    os.remove(tif_out)
                tif_filtered.rio.to_raster(
                    tif_out,
                    tiled=True,
                    compress="DEFLATE",
                    lock=lock_write or threading.Lock(),
                )
                print(f"Filtered TIF saved as {tif_out}")
            except Exception as e:
                print(f"Error saving the filtered TIF: {e}")
                raise
        else:
            filter_start = filter_start.strftime("%Y-%m-%d")
            filter_end = filter_end.strftime("%Y-%m-%d")
            return tif_filtered, filter_start, filter_end
    except FileNotFoundError as fnf_error:
        print(f"File not found: {fnf_error}")
        raise
    except ValueError as val_error:
        print(f"Value error: {val_error}")
        raise
    except TypeError as type_error:
        print(f"Type error: {type_error}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise


def sanitize_gdf_dtypes(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Converts columns with ExtensionDtype types (such as UInt32Dtype, StringDtype, etc.)
    to standard types compatible with export via Fiona or Pyogrio.

    Args:
        gdf (gpd.GeoDataFrame): GeoDataFrame to be corrected.

    Returns:
        gpd.GeoDataFrame: Copy with corrected types.
    """
    gdf = gdf.copy()
    geom_name = gdf.geometry.name
    for col in gdf.columns:
        if pd.api.types.is_extension_array_dtype(gdf[col].dtype):
            if pd.api.types.is_integer_dtype(gdf[col].dtype):
                gdf[col] = gdf[col].astype("Int64").astype("float").astype("Int64")
            elif pd.api.types.is_float_dtype(gdf[col].dtype):
                gdf[col] = gdf[col].astype(float)
            elif pd.api.types.is_string_dtype(gdf[col].dtype):
                gdf[col] = gdf[col].astype(str)
            elif pd.api.types.is_bool_dtype(gdf[col].dtype):
                gdf[col] = gdf[col].astype(bool)
            elif col == geom_name:
                pass
            else:
                print(f"Tipo no manejado: {col} - {gdf[col].dtype}")
    return gdf


def get_wfs_layer(wfs_url, layer_name, version="1.0.0"):
    """
    Retrieves a specific layer from a Web Feature Service (WFS).

    Args:
        wfs_url (str): The URL of the WFS service.
        layer_name (str): The name of the layer to retrieve.
        version (str, optional): The WFS version to use. Defaults to "1.0.0".

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing the data from the specified layer, or None if an error occurs.
    """

    try:
        wfs = WebFeatureService(wfs_url, version=version)
    except Exception as e:
        print(f"Error: Unable to connect to WFS service at {wfs_url}")
        print(f"Exception: {e}")
        raise

    try:
        # Get the list of available layers
        layers = wfs.contents
        if layers:
            pass
    except:
        print(f"Error: Unable to connect to WFS service at {wfs_url}")
        raise

    try:
        # Get the layer data
        response = wfs.getfeature(typename=layer_name, outputFormat="application/json")
        gdf = gpd.read_file(response)
        gdf = sanitize_gdf_dtypes(gdf)
        return gdf
    except Exception as e:
        print(f"Error: Unable to retrieve layer '{layer_name}' from WFS service.")
        print(f"Exception: {e}")
        raise


def clip_tif_to_ext(
    tif, vector, out_tif=None, chunks="auto", lock_read=True, lock_write=None
):
    """
    Clips a raster file (TIF) to the extent of a vector file.

    Args:
        tif (str or xarray.DataArray): Path to the input TIF file to be clipped or an xarray DataArray.
        vector (str or gpd.GeoDataFrame): Path to the vector file used for clipping or a GeoDataFrame.
        out_tif (str, optional): Path to save the output clipped TIF file. If None, the clipped TIF is returned as an xarray DataArray.
        chunks (str, int, dict, or bool, optional): Chunk size for reading the raster files. Defaults to "auto".
            - If "auto", the chunk size is determined automatically based on the file size and system memory.
            - If an integer, it specifies the size of chunks in pixels.
            - If a dictionary, it allows specifying chunk sizes for each dimension (e.g., {"x": 512, "y": 512}).
            - If False, chunking is disabled, and the entire file is read into memory.

    Returns:
        xr.DataArray or None: The clipped raster as an xarray DataArray if out_tif is None, otherwise None.
    """
    # Validate if the input TIF file and vector file exist
    tif, tif_name = validate_setting_tif(tif)

    if isinstance(vector, (str, pathlib.Path)):
        vector_name = os.path.basename(vector).split(".")[:-1][0]
        if not os.path.exists(vector):
            raise FileNotFoundError(f"The vector file does not exist: {vector_name}")
        ext = gpd.read_file(vector)
        if ext.empty:
            raise ValueError("The vector file is empty.")
        if ext.crs != tif.rio.crs:
            ext = ext.to_crs(tif.rio.crs)
        ext = ext.total_bounds
    elif isinstance(vector, gpd.GeoDataFrame):
        vector_name = "Vector"
        if vector.empty:
            raise ValueError("The vector GeoDataFrame is empty.")
        if vector.crs != tif.rio.crs:
            vector = vector.to_crs(tif.rio.crs)
        ext = vector.total_bounds
    else:
        raise TypeError("The vector must be a string or a GeoDataFrame.")
    # Start the clipping process
    print(f"Clipping {tif_name} with {vector_name}...")
    try:
        tif_clipped = tif.rio.clip_box(*ext)
        if out_tif is not None:
            print(f"Saving clipped TIF as {out_tif}...")
            out_tif_name = os.path.basename(out_tif).split(".")[:-1][0]
            tif_clipped.rio.to_raster(
                out_tif,
                tiled=True,
                compress="DEFLATE",
                lock=lock_write or threading.Lock(),
            )
            print(f"Clipped TIF saved as {out_tif_name}")
        else:
            print(f"Clipped TIF {tif_name} with {vector_name} completed.")
            return tif_clipped
    except Exception as e:
        print(f"Error clipping {tif_name} with {vector_name}")
        raise e


def default_vector():
    """
    Returns the default vector layer (departments of Honduras) as a GeoDataFrame.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame of Honduras departments.
    """
    ## Preparar los datos auxiliares
    # Crear la conexion al servicio WFS
    url_icf_wfs = "https://geoserver.icf.gob.hn/icf/wfs"

    # Obtener el GeoDataFrame de los departamentos de Honduras
    lyr_dep = "icf:Limite_HN"
    gdf_dep = get_wfs_layer(
        url_icf_wfs,
        lyr_dep,
        version="1.1.0",
    )
    return gdf_dep


def clean_files(dir_path="."):
    """
    Removes all files matching the pattern 'clipped*.tif' in the current directory.

    Returns:
        None
    """
    files_pattern = ["clipped", "tmp"]
    files = []
    for pattern in files_pattern:
        files.extend(
            glob.glob(os.path.join(dir_path, f"**/*{pattern}*"), recursive=True)
        )
    if not files:
        print("No files to remove")
        return
    for file in files:
        try:
            os.remove(file)
        except:
            print(f"Error removing {file}")
    print("Directory cleaned")
