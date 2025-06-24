"""This module contains functions for processing and analyzing raster data related to deforestation alerts."""

import os
import sys
import glob
from pathlib import Path
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
    Calculate and decompose dates based on grid codes and source type.

    Args:
        gdf (GeoDataFrame): GeoDataFrame containing the data to process.
        gridcode (str): Column name in the GeoDataFrame containing the grid codes.
        adef_src (str): Source type of the data. Can be "GLAD" or "INTEGRATED".
        year (int, optional): Year to use for "GLAD" source type. Defaults to None.

    Raises:
        ValueError: If invalid parameters are provided for the specified source type.

    Returns:
        GeoDataFrame: Updated GeoDataFrame with decomposed date information.
    """
    days_of_week = [
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY",
        "SATURDAY",
        "SUNDAY",
    ]
    months_of_year = [
        "JANUARY",
        "FEBRUARY",
        "MARCH",
        "APRIL",
        "MAY",
        "JUNE",
        "JULY",
        "AUGUST",
        "SEPTEMBER",
        "OCTOBER",
        "NOVEMBER",
        "DECEMBER",
    ]
    try:
        if adef_src == "GLAD" and year is not None:
            start_of_year = pd.Timestamp(f"{year}-01-01")
            gdf["date"] = start_of_year + pd.to_timedelta(gdf[gridcode] - 1, unit="D")
            gdf["year"] = year
        elif adef_src == "INTEGRATED" and year is None:
            zero_day = pd.Timestamp("2014-12-31")
            gdf["date"] = zero_day + pd.to_timedelta(gdf[gridcode] % 10000, unit="D")
            gdf["year"] = gdf["date"].dt.year
        else:
            raise ValueError(
                "Invalid parameters: For 'GLAD', 'year' is required. For 'INTEGRATED', only 'adef_src'."
            )
        # Decompose date using vectorized methods
        gdf["month"] = gdf["date"].dt.month.map(lambda m: months_of_year[m - 1])
        gdf["weekday"] = gdf["date"].dt.weekday.map(lambda d: days_of_week[d])
        gdf["week"] = gdf["date"].dt.isocalendar().week
        return gdf
    except Exception as e:
        print(f"Error processing dates: {e}")
        raise


def sanitize_gdf_dtypes(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Convert columns with ExtensionDtype types (such as UInt32Dtype, StringDtype, etc.)
    to standard types compatible with export via Fiona or Pyogrio.

    Args:
        gdf (gpd.GeoDataFrame): GeoDataFrame to be corrected.

    Returns:
        gpd.GeoDataFrame: Copy with corrected types.
    """
    gdf = gdf.copy()
    for col in gdf.columns:
        if pd.api.types.is_extension_array_dtype(gdf[col].dtype):
            if pd.api.types.is_integer_dtype(gdf[col].dtype):
                gdf[col] = gdf[col].astype("Int64").astype("float").astype("Int64")
            elif pd.api.types.is_string_dtype(gdf[col].dtype):
                gdf[col] = gdf[col].astype(str)
            elif pd.api.types.is_bool_dtype(gdf[col].dtype):
                gdf[col] = gdf[col].astype(bool)
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
        except OSError as e:
            print(f"Error removing {file}: {e}")
    print("Directory cleaned")
