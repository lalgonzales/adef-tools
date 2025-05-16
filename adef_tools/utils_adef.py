"""This module contains functions for processing and analyzing raster data related to deforestation alerts."""

import os
import sys
import platform
import glob
import shutil
import threading
import pathlib
from pathlib import Path
from datetime import timedelta
from datetime import datetime
import requests
import rioxarray as rxr
import xarray as xr
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

# from osgeo import gdal
from owslib.wfs import WebFeatureService

if getattr(sys, "frozen", False):
    BASE_DIR = Path.cwd()
else:
    BASE_DIR = Path(__file__).resolve().parent.parent


def get_safe_lock(name="rio", client=None):
    """
    Returns a Dask distributed lock (if Client is running), or a local threading lock.

    Args:
        name (str): Lock name
        client (Client, optional): Dask client. Optional, will be auto-detected.

    Returns:
        Lock object
    """
    try:
        from dask.distributed import Lock, default_client

        if client is not None:
            return Lock(name, client=client)
        else:
            # Try to get an existing active client
            try:
                return Lock(name, client=default_client())
            except ValueError:
                pass  # No client available
    except ImportError:
        pass

    import threading

    return threading.Lock()


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


def check_tif_attr(
    tif_no_adjust, tif_reference, tif_matched, chunks="auto", lock_read=True
):
    """
    Checks the attributes of a raster file against a reference raster file and determines if adjustments are needed.

    Args:
        tif_no_adjust (str): Path to the raster file that needs to be checked.
        tif_reference (str): Path to the reference raster file.
        tif_matched (str): Path to the output raster file that matches the reference.
        chunks (str, int, dict, or bool, optional): Chunk size for reading the raster files. Defaults to "auto".
            - If "auto", the chunk size is determined automatically based on the file size and system memory.
            - If an integer, it specifies the size of chunks in pixels.
            - If a dictionary, it allows specifying chunk sizes for each dimension (e.g., {"x": 512, "y": 512}).
            - If False, chunking is disabled, and the entire file is read into memory.

    Raises:
        FileNotFoundError: If the raster file to be checked does not exist.
        FileNotFoundError: If the reference raster file does not exist.

    Returns:
        dict or None: A dictionary containing the comparison results if adjustments are needed,
        or None if the raster file already matches the reference.
    """
    # Validate the entries
    tif_to_adjusd, tif_to_adjust_name = validate_setting_tif(
        tif_no_adjust, chunks=chunks, lock_read=lock_read
    )
    tif_reference, tif_reference_name = validate_setting_tif(
        tif_reference, chunks=chunks, lock_read=lock_read
    )

    print(
        f"...comparing the raster {tif_to_adjust_name} based on properties of {tif_reference_name}"
    )
    if os.path.exists(tif_matched):
        tif_adjusted, tif_adjusted_name = validate_setting_tif(tif_matched)
        try:
            crs_test = tif_adjusted.rio.crs == tif_to_adjusd.rio.crs
            transform_test = (
                tif_adjusted.rio.transform() == tif_to_adjusd.rio.transform()
            )
            bounds_test = tif_adjusted.rio.bounds() == tif_to_adjusd.rio.bounds()
            resolution_test = (
                tif_adjusted.rio.resolution() == tif_to_adjusd.rio.resolution()
            )
            if crs_test and transform_test and bounds_test and resolution_test:
                print(
                    f"...the raster {tif_to_adjust_name} is already adjusted as {tif_adjusted_name}. No action will be taken."
                )
                return None

            print(f"The raster {tif_to_adjust_name} does not match...")
            result = {
                "tif_path": tif_no_adjust,
                "crs_test": crs_test,
                "transform_test": transform_test,
                "bounds_test": bounds_test,
                "resolution_test": resolution_test,
            }
            return result
        except Exception as e:
            print(f"Error comparing the raster {tif_to_adjust_name}")
            raise e
    else:
        print(
            f"...the {tif_to_adjust_name} need adjust, proceeding to adjust the raster with {tif_reference_name}"
        )
        result = {
            "tif_path": tif_no_adjust,
            "crs_test": None,
            "transform_test": None,
            "bounds_test": None,
            "resolution_test": None,
        }
        return result


def adjust_tif(tif_no_adjust, tif_reference, tif_out, chunks="auto"):
    """
    Adjusts a raster file to match the spatial attributes of a reference raster file.

    Args:
        tif_no_adjust (str): Path to the raster file that needs to be adjusted.
        tif_reference (str): Path to the reference raster file.
        tif_out (str): Path to the output raster file that will match the reference.
        chunks (str, int, dict, or bool, optional): Chunk size for reading the raster files. Defaults to "auto".
            - If "auto", the chunk size is determined automatically based on the file size and system memory.
            - If an integer, it specifies the size of chunks in pixels.
            - If a dictionary, it allows specifying chunk sizes for each dimension (e.g., {"x": 512, "y": 512}).
            - If False, chunking is disabled, and the entire file is read into memory.

    Returns:
        None
    """
    # Validating and creating the array data
    tif_to_adjust, tif_to_adjust_name = validate_setting_tif(tif_no_adjust)
    tif_ref, tif_ref_name = validate_setting_tif(tif_reference)

    # Check the attributes of the rasters file
    result = check_tif_attr(tif_to_adjust, tif_reference, tif_out, chunks=chunks)
    if result is None:
        return

    # Start the adjustment process
    print(f"...Adjusting the raster {tif_to_adjust_name} with {tif_ref_name}")
    try:
        xmin, ymin, xmax, ymax = tif_ref.rio.bounds()
        gdalwarp_path = get_gdalwarp_path()
        gdal_adjust = (
            f"{gdalwarp_path} "
            f"-t_srs {tif_ref.rio.crs} "
            f"-overwrite "
            f"-te  {xmin} {ymin} {xmax} {ymax} "
            f"-tr {tif_ref.rio.resolution()[0]} {tif_ref.rio.resolution()[1]} "
            f"-r bilinear "
            f"-multi "
            f"-wo NUM_THREADS=ALL_CPUS "
            f"-srcnodata 0 "
            f"-dstnodata 0 "
            f"-co COMPRESS=DEFLATE "
            f"-co TILED=YES "
            f"{tif_no_adjust} "
            f"{tif_out}"
        )
        os.system(gdal_adjust)
        tif_out_name = os.path.basename(tif_out).split(".")[:-1][0]
        print(f"Raster ajustado y guardado en {tif_out_name}")
        return
    except Exception as e:
        print(f"No se pudo ajustar {tif_to_adjust_name} con {tif_ref_name}")
        raise e


def mask_by_tif(
    tif_mask,
    tif_to_mask,
    tif_out=None,
    chunks="auto",
    lock_read=True,
    lock_write=None,
):
    """
    Masks a raster file using another raster file as a mask.

    Args:
        tif_mask (str, path object or xarray.DataArray): Path to the mask raster file or an xarray DataArray.
        tif_to_mask (str, path object or xarray.DataArray): Path to the raster file to be masked or an xarray DataArray.
        tif_out (str, optional): Path to save the output masked raster file. If None, the masked raster is returned as an xarray DataArray.
        chunks (str, int, dict, or bool, optional): Chunk size for reading the raster files. Defaults to "auto".
            - If "auto", the chunk size is determined automatically based on the file size and system memory.
            - If an integer, it specifies the size of chunks in pixels.
            - If a dictionary, it allows specifying chunk sizes for each dimension (e.g., {"x": 512, "y": 512}).
            - If False, chunking is disabled, and the entire file is read into memory.

    Returns:
        xr.DataArray or None: The masked raster as an xarray DataArray if tif_out is None, otherwise None.
    """
    # Validating and creating the array data
    tif_data, tif_to_mask_name = validate_setting_tif(
        tif_to_mask, chunks=chunks, lock_read=lock_read
    )
    mask_data, tif_mask_name = validate_setting_tif(
        tif_mask, chunks=chunks, lock_read=lock_read
    )

    # Start the masking process
    print(f"Starting the masking of {tif_to_mask_name} with {tif_mask_name}...")
    try:
        mask = mask_data == 1
        tif_data = tif_data.where(mask, 0)
        if tif_out is not None:
            tif_data.rio.to_raster(
                tif_out,
                tiled=True,
                compress="DEFLATE",
                lock=lock_write or threading.Lock(),
            )
            tif_out_name = os.path.basename(tif_out).split(".")[:-1][0]
            print(f"Masked TIF saved as {tif_out_name}")
            return
        return tif_data
    except Exception as e:
        print(f"Error masking the TIF {tif_to_mask_name} with {tif_mask_name}")
        raise e


def filter_adef_intg_conf(
    tif, confidence=1, tif_out=None, chunks="auto", lock_read=True, lock_write=None
):
    """
    Filters a raster file (TIF) based on a specified confidence level.

    Args:
        tif (str): Path to the input TIF file to be filtered.
        confidence (int): The confidence level to filter the raster data.
        tif_out (str): Path to save the output filtered raster file.
        chunks (str, int, dict, or bool, optional): Chunk size for reading the raster files. Defaults to "auto".
            - If "auto", the chunk size is determined automatically based on the file size and system memory.
            - If an integer, it specifies the size of chunks in pixels.
            - If a dictionary, it allows specifying chunk sizes for each dimension (e.g., {"x": 512, "y": 512}).
            - If False, chunking is disabled, and the entire file is read into memory.

    Returns:
        None
    """
    # Validate and create array data
    tif_data, tif_name = validate_setting_tif(tif, chunks=chunks, lock_read=lock_read)

    # Start the filtering process
    print(
        f"Filtering the TIF {tif_name} with confidence level {confidence} and greater..."
    )
    try:
        mask = tif_data // 10000 >= confidence
        adef_intg_conf = tif_data.where(mask)
        adef_intg_conf.name = tif_name
        if tif_out is not None:
            tif_out_name = os.path.basename(tif_out).split(".")[:-1][0]
            adef_intg_conf.rio.to_raster(
                tif_out,
                tiled=True,
                compress="DEFLATE",
                lock=lock_write or threading.Lock(),
            )
            print(f"Filtered TIF saved as {tif_out_name}")
        print(f"Filtered TIF {tif_name} with confidence level {confidence} completed.")
        return adef_intg_conf
    except Exception as e:
        print(f"Error filtering the TIF {tif_name} with confidence level {confidence}")
        raise e


def mask_adef_hn_by_forest(
    tif_forest14,
    tif_forest18,
    tif_forest24,
    tif_adef_roi,
    tif_forest14_match,
    tif_forest18_match,
    tif_forest24_match,
    tif_out,
    confidence_integ=1,
    chunks="auto",
    lock_read=True,
    lock_write=None,
):
    """
    Masks deforestation alerts (ADEF_HN) using forest cover data from 2014 and 2018.
    This function filters deforestation alerts based on forest cover data from 2014 and 2018,
    applies masks to the alerts, and generates a new raster file with the masked alerts.
    Args:
        tif_forest14 (str): Path to the forest cover raster file for 2014.
        tif_forest18 (str): Path to the forest cover raster file for 2018.
        tif_adef_hn (str): Path to the deforestation alerts raster file.
        tif_forest14_match (str): Path to save the adjusted forest cover raster file for 2014.
        tif_forest18_match (str): Path to save the adjusted forest cover raster file for 2018.
        tif_out (str): Path to save the output masked raster file.
        chunks (str, int, dict, or bool, optional): Chunk size for reading the raster files. Defaults to "auto".
            - If "auto", the chunk size is determined automatically based on the file size and system memory.
            - If an integer, it specifies the size of chunks in pixels.
            - If a dictionary, it allows specifying chunk sizes for each dimension (e.g., {"x": 512, "y": 512}).
            - If False, chunking is disabled, and the entire file is read into memory.
    """
    # Create the names of the TIF files
    tif_forest14_name = os.path.basename(tif_forest14).split(".")[:-1][0]
    tif_forest18_name = os.path.basename(tif_forest18).split(".")[:-1][0]
    tif_forest24_name = os.path.basename(tif_forest24).split(".")[:-1][0]
    # tif_adef_hn_name = os.path.basename(tif_adef_hn).split(".")[:-1][0]
    tif_out_name = os.path.basename(tif_out).split(".")[:-1][0]

    # Validate if the input TIF file and reference file exist
    if not os.path.exists(tif_forest14):
        raise FileNotFoundError(
            f"The forest TIF file does not exist: {tif_forest14_name}"
        )

    if not os.path.exists(tif_forest18):
        raise FileNotFoundError(
            f"The forest TIF file does not exist: {tif_forest18_name}"
        )

    if not os.path.exists(tif_forest24):
        raise FileNotFoundError(
            f"The forest TIF file does not exist: {tif_forest24_name}"
        )

    # if not os.path.exists(tif_adef_hn):
    #     raise FileNotFoundError(
    #         f"The ADEF_HN TIF file does not exist: {tif_adef_hn_name}"
    #     )

    try:
        # Filtrar las alertas de los años 2014, 2018, 2024
        adef_hn = filter_adef_intg_conf(
            tif_adef_roi,
            confidence=confidence_integ,
            chunks=chunks,
            lock_read=lock_read,
            lock_write=lock_write,
        )
        print("Starting the masking of the alerts with forest...")

        # Mascaras de bosque antes del 2018
        print("...Applying forest masks")
        zero_day = pd.Timestamp("2014-12-31")
        tif_in_days = adef_hn % 10000

        # Min date of the alerts
        print("...getting the min, max dates of the tif")
        min_day = tif_in_days.min(skipna=True)
        max_day = tif_in_days.max(skipna=True)
        min_date = zero_day + timedelta(days=min_day.compute().item())
        max_date = zero_day + timedelta(days=max_day.compute().item())
        range_dates = pd.date_range(min_date, max_date, freq="D")
        print(f"...the TIF contains alerts from {min_date} to {max_date}")

        # Dates of filtering
        days_to_18 = (pd.Timestamp("2018-01-01") - zero_day).days
        days_to_24 = (pd.Timestamp("2024-01-01") - zero_day).days

        # Ranges
        range_14_lt18 = pd.date_range(
            pd.Timestamp("2014-01-01"), pd.Timestamp("2017-12-31"), freq="D"
        )
        range_18_lt24 = pd.date_range(
            pd.Timestamp("2018-01-01"), pd.Timestamp("2023-12-31"), freq="D"
        )
        range_24 = pd.date_range(
            pd.Timestamp("2024-01-01"), pd.Timestamp(datetime.today()), freq="D"
        )

        # Apply mask 2014 - 2017 if there is data before 2018
        if any(date in range_14_lt18 for date in range_dates):
            print("...masking alerts before 2018")
            try:
                mask_adefhn_lt18 = adef_hn.sel(band=1) % 10000 < days_to_18
                adef_hn_lt18 = adef_hn.where(mask_adefhn_lt18, 0)
                adjust_tif(
                    tif_forest14,
                    tif_adef_roi,
                    tif_forest14_match,
                    chunks=chunks,
                )
                adef_hn_masked_lt18 = mask_by_tif(
                    tif_forest14_match,
                    adef_hn_lt18,
                    chunks=chunks,
                    lock_read=lock_read,
                    lock_write=lock_write,
                )
                print("...Processed alerts before 2018")
            except ValueError as e:
                print("Error applying the masking for alerts before 2018")
                raise e
        else:
            adef_hn_masked_lt18 = False
            print("...Alerts before 2018 were not found")

        # Mascaras de bosque 2018 - 2024
        if any(date in range_18_lt24 for date in range_dates):
            print(f"...masking from {min_date.year} to 2023")
            mask_adefhn_gte18 = (adef_hn.sel(band=1) % 10000 >= days_to_18) & (
                adef_hn.sel(band=1) % 10000 < days_to_24
            )
            adef_hn_gte18 = adef_hn.where(mask_adefhn_gte18, 0)
            adjust_tif(
                tif_forest18,
                tif_adef_roi,
                tif_forest18_match,
                chunks=chunks,
            )
            adef_hn_masked_gte18 = mask_by_tif(
                tif_forest18_match,
                adef_hn_gte18,
                chunks=chunks,
                lock_read=lock_read,
                lock_write=lock_write,
            )
            print(f"...alerts from {min_date.year} were processed")
        else:
            adef_hn_masked_gte18 = False
            print("...Alerts from 2018 onwards were not found")

        if any(date in range_24 for date in range_dates):
            # Mascaras de bosque 2024 - presente
            print("...masking from 2024 onwards")
            mask_adefhn_gte24 = adef_hn.sel(band=1) % 10000 >= days_to_24
            adef_hn_gte24 = adef_hn.where(mask_adefhn_gte24, 0)
            adjust_tif(
                tif_forest24,
                tif_adef_roi,
                tif_forest24_match,
                chunks=chunks,
            )
            adef_hn_masked_gte24 = mask_by_tif(
                tif_forest24_match,
                adef_hn_gte24,
                chunks=chunks,
                lock_read=lock_read,
                lock_write=lock_write,
            )
            print("...alerts from 2024 were processed")
        else:
            adef_hn_masked_gte24 = False
            print("...Alerts from 2024 onwards were not found")
        try:
            tif_adef_hn_masked = (
                adef_hn_masked_lt18 + adef_hn_masked_gte18 + adef_hn_masked_gte24
            )
            dir_out = os.path.dirname(tif_out)
            os.makedirs(dir_out, exist_ok=True)
            tif_adef_hn_masked.rio.to_raster(
                tif_out,
                tiled=True,
                compress="DEFLATE",
                lock=lock_write or threading.Lock(),
            )
            print(f"Masked TIF saved as {tif_out_name}")
            return
        except Exception as e:
            print(f"Error saving the masked TIF {tif_out_name}")
            raise e

    except Exception as e:
        print(f"Error en el enmascaramiento por bosque: {e}")
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
        f"{polygonize_path} "
        f"{tif} "
        f"{out_vector} "
        f"{layer_name} 'value' "
        f"-of {driver} "
        f"-overwrite "
        f"-mask {tif} "
    )
    try:
        os.system(command)
    except:
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


def dw_tif(url, tif_out, timeout=10):
    """
    Downloads a TIF file from a given URL and saves it to the specified output path.

    Args:
        url (str): The URL from which to download the TIF file.
        tif_out (str): The path where the downloaded TIF file will be saved.
        timeout (int, optional): The timeout in seconds for the download request. Defaults to 10.

    Raises:
        Exception: If an HTTP error occurs during the download.
        Exception: If a connection error occurs while attempting to reach the server.
        Exception: For any other errors that occur during the download process.

    Returns:
        None
    """

    # Crear el nombre del tif
    tif_out_name = os.path.basename(tif_out)
    dir_base = os.path.dirname(tif_out)
    os.makedirs(dir_base, exist_ok=True)
    print(f"Descargando el TIF de Alertas desde {url}...")
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        with open(tif_out, "wb") as file:
            file.write(response.content)
        print(f"Archivo descargado y guardado en {tif_out_name}")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Error connecting to the server: {conn_err}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    return None


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
    wfs = WebFeatureService(wfs_url, version=version)

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


def get_gdal_polygonize_path():
    """
    Determines the path to the `gdal_polygonize` utility based on the operating system.

    Raises:
        FileNotFoundError: If `gdal_polygonize` is not found on the system.

    Returns:
        str: The full path to the `gdal_polygonize` utility.
    """
    system = platform.system().lower()

    if system == "linux":
        path = shutil.which("gdal_polygonize.py")
        if path:
            return path  # binary found in PATH

    elif system == "windows":
        # Look for gdal_polygonize.py in Conda or Python scripts folder
        conda_prefix = os.environ.get("CONDA_PREFIX", "")
        candidates = [
            os.path.join(conda_prefix, "Scripts", "gdal_polygonize.py"),
            os.path.join(sys.prefix, "Scripts", "gdal_polygonize.py"),
        ]
        for path in candidates:
            if os.path.isfile(path):
                return path

    raise FileNotFoundError(
        "gdal_polygonize not found. Ensure GDAL is installed and available."
        "For conda run `conda install -c conda-forge gdal` for Windows users."
        "For Linux users with uv, run `uv pip install --find-links https://girder.github.io/large_image_wheels gdal pyproj`."
        "otherwise, install GDAL from your package manager (e.g., apt, yum, dnf) or from source."
    )


def get_gdalwarp_path():
    """
    Determines the path to the `gdalwarp` utility based on the operating system.

    Raises:
        FileNotFoundError: If `gdalwarp` is not found on the system.

    Returns:
        str: The full path to the `gdalwarp` utility.
    """
    system = platform.system().lower()

    # Linux: usually in PATH
    if system == "linux":
        path = shutil.which("gdalwarp")
        if path:
            return path

    # Windows: look inside Conda environment
    elif system == "windows":
        conda_prefix = os.environ.get("CONDA_PREFIX", "")
        candidates = [
            os.path.join(conda_prefix, "Library", "bin", "gdalwarp.exe"),
            os.path.join(conda_prefix, "Scripts", "gdalwarp.exe"),
            shutil.which("gdalwarp"),  # just in case it's in PATH
        ]
        for path in candidates:
            if path and os.path.isfile(path):
                return path

    raise FileNotFoundError(
        "gdalwarp not found. Ensure GDAL is installed and available."
        "For conda run `conda install -c conda-forge gdal` for Windows users."
        "For Linux users with uv, run `uv pip install --find-links https://girder.github.io/large_image_wheels gdal pyproj`."
        "otherwise, install GDAL from your package manager (e.g., apt, yum, dnf) or from source."
    )


def validate_setting_tif(tif, chunks="auto", lock_read=True):
    """
    Validates and loads a TIF file or xarray.DataArray, returning the object and its name.

    Args:
        tif (str, pathlib.Path, or xr.DataArray): Path to the TIF file or an xarray DataArray.

    Raises:
        FileNotFoundError: If the TIF file path does not exist.
        ValueError: If the DataArray does not have a name.

    Returns:
        tuple: (tif object, tif name)
    """
    if isinstance(tif, (str, pathlib.Path)):
        tif_name = os.path.basename(tif).split(".")[:-1][0]
        if not os.path.exists(tif):
            raise FileNotFoundError(f"The input TIF file does not exist: {tif_name}")
        tif_data = rxr.open_rasterio(tif, chunks=chunks, lock=lock_read)
        tif_data.name = tif_name
        return tif_data, tif_name
    elif isinstance(tif, xr.DataArray):
        if tif.name is not None:
            tif_name = tif.name
            tif_data = tif
            return tif_data, tif_name
        else:
            raise ValueError(
                "The TIF must have a name. Assign one using `data.name = name`."
            )


def default_vector():
    """
    Returns the default vector layer (departments of Honduras) as a GeoDataFrame.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame of Honduras departments.
    """
    ## Preparar los datos auxiliares
    # Crear la conexion al servicio WFS
    url_icf_wfs = "https://geoserver.icf.gob.hn/icfpub/wfs"

    # Obtener el GeoDataFrame de los departamentos de Honduras
    lyr_dep = "icfpub:limite_departamentos_gp"
    gdf_dep = get_wfs_layer(
        url_icf_wfs,
        lyr_dep,
        version="1.1.0",
    )
    return gdf_dep


def divide_intg_for_forest(
    tif_intg,
    periods,
    chunks="auto",
    lock_read=True,
    lock_write=None,
    out_folder=None,
):

    # Validate and create array data
    tif_data, tif_name = validate_setting_tif(
        tif_intg, chunks=chunks, lock_read=lock_read
    )

    n_periods = len(periods)
    zero_day = pd.Timestamp("2014-12-31")
    dic_tifs = {}
    print(f"Dividing the TIF {tif_name} into {n_periods} periods...")
    try:
        # Iterating over the pairs of periods
        for i in tqdm(range(n_periods), desc="Processing periods"):
            period = periods[i]
            start = period[0]
            end = period[1]
            print(f"...Processing the period {start} to {end}...")
            start_day = (pd.Timestamp(start) - zero_day).days
            end_day = (pd.Timestamp(end) - zero_day).days
            tif_in_days = tif_data % 10000
            mask = (tif_in_days >= start_day) & (tif_in_days <= end_day)
            tif_intg_period = tif_data.where(mask, 0)
            name = f"{tif_name}_{start}_{end}"
            tif_intg_period.name = name
            dic_tifs[name] = tif_intg_period
            if out_folder is not None:
                out_tif_name = os.path.join(out_folder, f"{name}.tif")
                tif_intg_period.rio.to_raster(
                    out_tif_name,
                    tiled=True,
                    compress="DEFLATE",
                    lock=lock_write or threading.Lock(),
                )
        return dic_tifs
    except Exception as e:
        print(f"Error dividing the TIF {tif_name} into periods")
        raise e


def mask_by_forest(
    tif_to_mask,
    forest_masks,
    tif_out=None,
    chunks="auto",
    lock_read=True,
    lock_write=None,
):
    """
    Masks a raster file (TIF) for a specific period using the appropriate forest mask.

    Args:
        tif_to_mask (str or xr.DataArray): Path to the raster file to be masked or an xarray DataArray.
        forest_masks (list): List of paths to forest mask raster files (e.g., for years 2014, 2018, 2024).
        tif_out (str, optional): Path to save the output masked raster file. If None, the masked raster is returned as an xarray DataArray.
        chunks (str, int, dict, or bool, optional): Chunk size for reading the raster files. Defaults to "auto".
        lock_read (bool, optional): Whether to use a lock when reading files. Defaults to True.
        lock_write (threading.Lock or None, optional): Lock to use when writing files. Defaults to None.

    Raises:
        ValueError: If the start date is after the end date.
        ValueError: If the date range does not match any valid forest mask period.

    Returns:
        xr.DataArray or None: The masked raster as an xarray DataArray if tif_out is None, otherwise None.
    """
    # Validate and create array data
    tif_data, tif_to_mask_name = validate_setting_tif(
        tif_to_mask, chunks=chunks, lock_read=lock_read
    )

    # Extract the dates from the TIF
    dates = tif_to_mask_name.split("_")
    start_date = pd.Timestamp(dates[-2])
    end_date = pd.Timestamp(dates[-1])

    if start_date > end_date:
        raise ValueError(
            f"The start date {start_date} is greater than the end date {end_date}"
        )

    # Validate the period to select the forest masks
    if end_date < pd.Timestamp("2018-01-01"):
        forest_mask = [f for f in forest_masks if "14" in str(f)]
    elif end_date < pd.Timestamp("2024-01-01"):
        forest_mask = [f for f in forest_masks if "18" in str(f)]
    elif end_date >= pd.Timestamp("2024-01-01"):
        forest_mask = [f for f in forest_masks if "24" in str(f)]
    else:
        raise ValueError(
            f"Pass a valid date range for the forest masks: {start_date} to {end_date}, valid ranges dates are: "
            "2014-01-01 to 2017-12-31, 2018-01-01 to 2023-12-31, 2024-01-01 to present"
        )

    # Validate and create the forest mask
    # Adjust the forest mask to the TIF
    folder_forest = os.path.dirname(forest_mask[0])
    forest_name = os.path.basename(forest_mask[0]).split(".")[0]
    forest_adjusted = os.path.join(folder_forest, f"{forest_name}_match.tif")
    adjust_tif(
        forest_mask[0],
        tif_data,
        forest_adjusted,
        chunks=chunks,
    )
    tif_masked = mask_by_tif(
        forest_adjusted,
        tif_data,
        tif_out=tif_out,
        chunks=chunks,
        lock_read=lock_read,
        lock_write=lock_write,
    )
    return tif_masked


def clip_tif_gdal(
    tif, roi_clip, tif_out, extend=True, chunks="auto", lock_read=True, **vector_kwargs
):
    """
    Clips a raster file (TIF) using GDAL's gdalwarp utility and a vector file or GeoDataFrame as the cutline.

    Args:
        tif (str or pathlib.Path): Path to the input TIF file to be clipped.
        roi_clip (str, pathlib.Path, or gpd.GeoDataFrame): Path to the vector file or a GeoDataFrame used as the cutline.
        tif_out (str): Path to save the output clipped TIF file.
        extend (bool, optional): If True, extends the bounds of the output raster to include the cutline. Defaults to True.
        chunks (str, int, dict, or bool, optional): Chunk size for reading the raster files. Defaults to "auto".
        lock_read (bool, optional): Whether to use a lock when reading files. Defaults to True.
        **kwargs: Additional keyword arguments for reading the vector file.

    Raises:
        TypeError: If the vector is not a file path or a GeoDataFrame.

    Returns:
        None
    """
    # # Validate the input TIF file and create
    tif_data, tif_name = validate_setting_tif(tif, chunks=chunks, lock_read=lock_read)
    xres, yres = tif_data.rio.resolution()

    # Validate the input vector file
    if isinstance(roi_clip, (str, pathlib.Path)) and os.path.exists(roi_clip):
        data_clip = gpd.read_file(roi_clip, **vector_kwargs)
    if isinstance(roi_clip, gpd.GeoDataFrame):
        data_clip = roi_clip
    else:
        raise TypeError("The vector must be a file path or a GeoDataFrame.")

    gdal_warp_path = get_gdalwarp_path()
    if data_clip.crs != "EPSG:4326":
        data_clip = data_clip.to_crs("EPSG:4326")
    ext = data_clip.total_bounds
    print(f"Using {gdal_warp_path} to clip the TIF")
    cmd = (
        f"{gdal_warp_path} "
        f"-overwrite "
        f"-t_srs EPSG:4326 "
        f"-te {ext[0]} {ext[1]} {ext[2]} {ext[3]} "
        f"-tr {xres} {yres} "
        f"-srcnodata 0 "
        f"-dstnodata 0 "
        f"-co TILED=YES "
        f"-co COMPRESS=DEFLATE "
        f"-wo NUM_THREADS=ALL_CPUS "
        f"-multi "
        f"-ot uint16 "
        f"{tif} "
        f"{tif_out}"
    )
    try:
        os.system(cmd)
    except:
        print(f"Error clipping {tif_name} with {roi_clip}")
        raise
    print(f"Clipped {tif_name} saved as {tif_out}")
    return None


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
