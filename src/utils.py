import os
import threading
import pathlib
from datetime import timedelta
from datetime import datetime
import rioxarray as rxr
import xarray as xr
import pandas as pd
import geopandas as gpd


def calculate_decompose_date(gdf, gridcode, adef_src, year=None):
    """
    Calculate and decompose dates based on a grid code.
    This function takes a GeoDataFrame and a grid code column, calculates the date
    from the grid code, and decomposes it into year, month, day of the week, and
    ISO calendar week.

    :param gdf: GeoDataFrame containing the data.
    :param gridcode: Column name in the GeoDataFrame that contains the gridcode.
    :param adef_src: Source of the data, either "GLAD" or "INTEGRATED".
    :param year: Year to use for date calculation (required for "GLAD").

    :return: GeoDataFrame with additional columns for year, month, day of the week, and ISO calendar week. Raises an exception if an error occurs.
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
            start_of_year = datetime(year, 1, 1)
            gdf["fecha"] = gdf[gridcode].apply(
                lambda x: start_of_year + timedelta(days=int(x) - 1)
            )
            gdf["anio"] = year
        elif adef_src == "INTEGRATED" and year is None:
            zero_day = pd.Timestamp("2014-12-31")
            gdf["fecha"] = gdf[gridcode].apply(
                lambda x: zero_day + timedelta(days=int(x % 10000))
            )
            gdf["anio"] = gdf["fecha"].apply(lambda x: x.year)
        else:
            raise ValueError(
                "Invalid parameters: For 'GLAD', both 'adef_src' and 'year' must be provided. For 'INTEGRATED', only 'adef_src' is required."
            )
    except Exception as e:
        print(f"Error al calcular la fecha: {e}")
        raise

    try:
        gdf["mes"] = gdf["fecha"].apply(lambda x: months_of_year[x.month - 1])
        gdf["dia"] = gdf["fecha"].apply(lambda x: days_of_week[x.weekday()])
        gdf["semana"] = gdf["fecha"].apply(lambda x: x.isocalendar()[1])
        return gdf
    except Exception as e:
        print(f"Error al descomponer la fecha: {e}")
        raise


def check_tif_attr(tif_no_adjust, tif_reference, tif_matched, chunks="auto"):
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
    # Create the names of the TIF files
    tif_no_adjust_name = os.path.basename(tif_no_adjust).split(".")[:-1][0]
    tif_reference_name = os.path.basename(tif_reference).split(".")[:-1][0]
    tif_out_name = os.path.basename(tif_matched).split(".")[:-1][0]

    # Validate if the input TIF file and reference file exist
    if not os.path.exists(tif_no_adjust):
        raise FileNotFoundError(
            f"The input raster file does not exist: {tif_no_adjust_name}"
        )
    if not os.path.exists(tif_reference):
        raise FileNotFoundError(
            f"The reference raster file does not exist: {tif_reference_name}"
        )

    # Start the comparison process
    print(
        f"...comparing the raster {tif_no_adjust_name} based on properties of {tif_reference_name}"
    )
    if os.path.exists(tif_matched):
        try:
            tif_adjusted = rxr.open_rasterio(
                tif_matched,
                chunks=chunks,
                lock=True,
            )
            tif_test = rxr.open_rasterio(
                tif_reference,
                chunks=chunks,
                lock=True,
            )
            crs_test = tif_adjusted.rio.crs == tif_test.rio.crs
            transform_test = tif_adjusted.rio.transform() == tif_test.rio.transform()
            bounds_test = tif_adjusted.rio.bounds() == tif_test.rio.bounds()
            resolution_test = tif_adjusted.rio.resolution() == tif_test.rio.resolution()
            if crs_test and transform_test and bounds_test and resolution_test:
                print(
                    f"...the raster {tif_no_adjust_name} is already adjusted as {tif_out_name}. No action will be taken."
                )
                return None

            print(f"The raster {tif_no_adjust_name} does not match...")
            result = {
                "tif_path": tif_no_adjust,
                "crs_test": crs_test,
                "transform_test": transform_test,
                "bounds_test": bounds_test,
                "resolution_test": resolution_test,
            }
            return result
        except Exception as e:
            print(f"Error comparing the raster {tif_no_adjust_name}")
            raise e
    else:
        print(
            f"...the {tif_out_name} does not exist, proceeding to adjust the raster with {tif_reference_name}"
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
    # Create the names of the TIF files
    tif_no_adjust_name = os.path.basename(tif_no_adjust).split(".")[:-1][0]
    tif_reference_name = os.path.basename(tif_reference).split(".")[:-1][0]
    tif_out_name = os.path.basename(tif_out).split(".")[:-1][0]

    # Validate if the input TIF file and reference file exist
    if not os.path.exists(tif_no_adjust):
        raise FileNotFoundError(
            f"The input raster file does not exist: {tif_no_adjust_name}"
        )
    if not os.path.exists(tif_reference):
        raise FileNotFoundError(
            f"The reference raster file does not exist: {tif_reference_name}"
        )

    # Check the attributes of the rasters file
    result = check_tif_attr(tif_no_adjust, tif_reference, tif_out, chunks=chunks)
    if result is None:
        return

    # Start the adjustment process
    print(f"...Adjusting the raster {tif_no_adjust_name} with {tif_reference_name}")
    try:
        # tif_no_adj = rxr.open_rasterio(tif_no_adjust, chunks=chunks, lock=True)
        tif_ref = rxr.open_rasterio(
            tif_reference, chunks=chunks, lock=True, masked=True
        )
        xmin, ymin, xmax, ymax = tif_ref.rio.bounds()
        gdal_adjust = (
            f"gdalwarp "
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
        print(f"Raster ajustado y guardado en {tif_out_name}")
        return
    except Exception as e:
        print(f"No se pudo ajustar {tif_no_adjust_name} con {tif_reference_name}")
        raise e


def mask_by_tif(
    tif_mask,
    tif_to_mask,
    tif_out=None,
    chunks="auto",
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
    # Create the data of the TIF files or load them if they are already opened
    # Mask data
    if isinstance(tif_mask, (str, pathlib.Path)):
        tif_mask_name = os.path.basename(tif_mask).split(".")[:-1][0]
        if not os.path.exists(tif_mask):
            raise FileNotFoundError(
                f"The mask TIF file does not exist: {tif_mask_name}"
            )
        mask_data = rxr.open_rasterio(tif_mask, chunks=chunks, lock=True)
    elif isinstance(tif_mask, xr.DataArray):
        tif_mask_name = "Mask TIF"
        mask_data = tif_mask
    else:
        raise TypeError("The mask must be a string or an xarray DataArray.")

    # Data to be masked
    if isinstance(tif_to_mask, (str, pathlib.Path)):
        tif_to_mask_name = os.path.basename(tif_to_mask).split(".")[:-1][0]
        if not os.path.exists(tif_to_mask):
            raise FileNotFoundError(
                f"The TIF file to be masked does not exist: {tif_to_mask_name}"
            )
        tif_data = rxr.open_rasterio(tif_to_mask, chunks=chunks, lock=True)
    elif isinstance(tif_to_mask, xr.DataArray):
        tif_to_mask_name = "TIF to mask"
        tif_data = tif_to_mask
    else:
        raise TypeError("The TIF to mask must be a string or an xarray DataArray.")

    # Start the masking process
    print(f"Starting the masking of {tif_to_mask_name} with {tif_mask_name}...")
    try:
        mask = mask_data == 1
        tif_data = tif_data.where(mask, 0)
        if tif_out is not None:
            tif_data.rio.to_raster(
                tif_out, tiled=True, compress="DEFLATE", lock=threading.Lock()
            )
            tif_out_name = os.path.basename(tif_out).split(".")[:-1][0]
            print(f"Masked TIF saved as {tif_out_name}")
            return
        return tif_data
    except Exception as e:
        print(f"Error masking the TIF {tif_to_mask_name} with {tif_mask_name}")
        raise e


def filter_adef_intg_conf(tif, confidence=1, tif_out=None, chunks="auto"):
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
    if isinstance(tif, (str, pathlib.Path)):
        # Create the names of the TIF files
        tif_name = os.path.basename(tif).split(".")[:-1][0]

        # Validate if the input TIF file exists
        if not os.path.exists(tif):
            raise FileNotFoundError(f"The input TIF file does not exist: {tif_name}")
        tif_data = rxr.open_rasterio(
            tif,
            chunks=chunks,
            lock=True,
        )
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
                lock=threading.Lock(),
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
    tif_adef_hn,
    tif_forest14_match,
    tif_forest18_match,
    tif_forest24_match,
    tif_out,
    confidence_integ=1,
    chunks="auto",
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
    tif_adef_hn_name = os.path.basename(tif_adef_hn).split(".")[:-1][0]
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

    if not os.path.exists(tif_adef_hn):
        raise FileNotFoundError(
            f"The ADEF_HN TIF file does not exist: {tif_adef_hn_name}"
        )

    try:
        # Filtrar las alertas de los años 2014, 2018, 2024
        adef_hn = filter_adef_intg_conf(
            tif_adef_hn, confidence=confidence_integ, chunks=chunks
        )
        print("Starting the masking of the alerts with forest...")

        # Mascaras de bosque antes del 2018
        print("...Applying forest masks")
        zero_day = pd.Timestamp("2014-12-31")
        tif_in_days = adef_hn % 10000

        # Min date of the alerts
        min_day = tif_in_days.min().values.item()
        min_date = zero_day + timedelta(days=min_day)
        max_day = tif_in_days.max().values.item()
        max_date = zero_day + timedelta(days=max_day)
        range_dates = pd.date_range(min_date, max_date, freq="D")
        print(f"...the TIF contains alerts from {min_date}")

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
                adjust_tif(tif_forest14, tif_adef_hn, tif_forest14_match)
                adef_hn_masked_lt18 = mask_by_tif(
                    tif_forest14_match, adef_hn_lt18, chunks=chunks
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
            adjust_tif(tif_forest18, tif_adef_hn, tif_forest18_match)
            adef_hn_masked_gte18 = mask_by_tif(
                tif_forest18_match, adef_hn_gte18, chunks=chunks
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
            adjust_tif(tif_forest24, tif_adef_hn, tif_forest24_match)
            adef_hn_masked_gte24 = mask_by_tif(
                tif_forest24_match, adef_hn_gte24, chunks=chunks
            )
            print("...alerts from 2024 were processed")
        else:
            adef_hn_masked_gte24 = False
            print("...Alerts from 2024 onwards were not found")
        try:
            tif_adef_hn_masked = (
                adef_hn_masked_lt18 + adef_hn_masked_gte18 + adef_hn_masked_gte24
            )
            if os.path.exists(tif_out):
                os.remove(tif_out)
            tif_adef_hn_masked.rio.to_raster(
                tif_out, tiled=True, compress="DEFLATE", lock=threading.Lock()
            )
            print(f"Masked TIF saved as {tif_out_name}")
            return
        except Exception as e:
            print(f"Error saving the masked TIF {tif_out_name}")
            raise e

    except Exception as e:
        print(f"Error en el enmascaramiento del {tif_adef_hn_name}")
        raise e


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
    driver = out_file.split(".")[-1]
    out_vector = os.path.join(out_folder, out_file)
    command = (
        f"gdal_polygonize.py "
        f"{tif} "
        f"{out_vector} "
        f"{layer_name} 'value' "
        f"-of {driver} "
        f"-overwrite "
    )
    os.system(command)
    print(f"Vector file saved as {out_file} in {out_folder}")
