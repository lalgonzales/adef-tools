"""Rasters functions for ADEF tools."""

import os
import sys
import subprocess
import pathlib
import platform
import shutil
import requests
import pandas as pd
import rioxarray as rxr
import xarray as xr
import time


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


def dw_tif(url, tif_out, timeout=10, retries=3, delay=3):
    """
    Downloads a TIF file from a given URL and saves it to the specified output path, with retries.

    Args:
        url (str): The URL from which to download the TIF file.
        tif_out (str): The path where the downloaded TIF file will be saved.
        timeout (int, optional): The timeout in seconds for the download request. Defaults to 10.
        retries (int, optional): Number of retry attempts if download fails. Defaults to 3.
        delay (int, optional): Seconds to wait between retries. Defaults to 3.

    Raises:
        Exception: If all retries fail.

    Returns:
        None
    """
    tif_out_name = os.path.basename(tif_out)
    dir_base = os.path.dirname(tif_out)
    os.makedirs(dir_base, exist_ok=True)

    attempt = 0
    while attempt < retries:
        print(
            f"Descargando el TIF de Alertas desde {url}... (intento {attempt+1}/{retries})"
        )
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            with open(tif_out, "wb") as file:
                file.write(response.content)
            print(f"Archivo descargado y guardado en {tif_out_name}")
            return
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
        ) as err:
            print(f"Error de conexión o HTTP: {err}")
        except Exception as e:
            print(f"Ocurrió un error: {e}")
        attempt += 1
        if attempt < retries:
            print(f"Reintentando en {delay} segundos...")
            time.sleep(delay)
    raise Exception(f"No se pudo descargar el archivo después de {retries} intentos.")


def validate_setting_tif(tif, **rxr_kwargs):
    """
    Validates and loads a TIF file or xarray.DataArray, returning the object and its name.

    Args:
        tif (str, pathlib.Path, or xr.DataArray): Path to the TIF file or an xarray DataArray.
        **rxr_kwargs: Extra keyword arguments for rioxarray.open_rasterio.

    Raises:
        FileNotFoundError: If the TIF file path does not exist.
        ValueError: If the DataArray does not have a name.

    Returns:
        tuple: (tif object, tif name)
    """
    # Validate input type and load the TIF file or DataArray
    if isinstance(tif, (str, pathlib.Path)):
        tif_name = os.path.basename(tif).split(".")[:-1][0]
        if not os.path.exists(tif):
            raise FileNotFoundError(f"The input TIF file does not exist: {tif_name}")
        tif_data = rxr.open_rasterio(tif, **rxr_kwargs)
        tif_data.name = tif_name
    elif isinstance(tif, xr.DataArray):
        if tif.name is not None:
            tif_name = tif.name
        else:
            raise ValueError(
                "The TIF must have a name. Assign one using `data.name = name`."
            )
        tif_data = tif
    else:
        raise TypeError(
            "The input must be a string, pathlib.Path, or xarray.DataArray."
        )
    return tif_data, tif_name


def clip_tif_ext_gdal(tif, roi_clip, tif_out, rxr_kwargs=None, gpd_kwargs=None):
    """
    Clips a raster file (TIF) to the extent of a vector file using GDAL's gdalwarp.

    Args:
        tif (str or xarray.DataArray): Path to the input TIF file to be clipped or an xarray DataArray.
        roi_clip (str or gpd.GeoDataFrame): Path to the vector file used for clipping or a GeoDataFrame.
        tif_out (str): Path to save the output clipped TIF file.
        rxr_kwargs (dict, optional): Extra keyword arguments for rioxarray.open_rasterio.
        gpd_kwargs (dict, optional): Extra keyword arguments for geopandas.read_file.

    Raises:
        TypeError: If the vector is not a file path or a GeoDataFrame.

    Returns:
        str: Path to the output clipped TIF file.
    """
    import geopandas as gpd

    rxr_kwargs = rxr_kwargs or {}
    gpd_kwargs = gpd_kwargs or {}

    tif_data, tif_name = validate_setting_tif(tif, **rxr_kwargs)
    xres, yres = tif_data.rio.resolution()

    # Validate the input vector file
    if isinstance(roi_clip, (str, pathlib.Path)) and os.path.exists(roi_clip):
        data_clip = gpd.read_file(roi_clip, **gpd_kwargs)
    elif isinstance(roi_clip, gpd.GeoDataFrame):
        data_clip = roi_clip
    else:
        raise TypeError("The vector must be a file path or a GeoDataFrame.")

    gdal_warp_path = get_gdalwarp_path()
    if data_clip.crs != "EPSG:4326":
        data_clip = data_clip.to_crs("EPSG:4326")
    ext = data_clip.total_bounds
    print(f"Using {gdal_warp_path} to clip the TIF")
    command = (
        [gdal_warp_path]
        + ["-overwrite"]
        + ["-t_srs", "EPSG:4326"]
        + ["-te", str(ext[0]), str(ext[1]), str(ext[2]), str(ext[3])]
        + ["-tr", str(xres), str(yres)]
        + ["-srcnodata", "0"]
        + ["-dstnodata", "0"]
        + ["-co", "TILED=YES"]
        + ["-co", "COMPRESS=DEFLATE"]
        + ["-wo", "NUM_THREADS=ALL_CPUS"]
        + ["-multi"]
        + ["-ot", "uint16"]
        + [tif]
        + [tif_out]
    )
    try:
        subprocess.run(command, check=True)
        print(f"Clipped {tif_name} saved as {tif_out}")
        return tif_out
    except subprocess.CalledProcessError as e:
        print(f"Error running gdalwarp to clip {tif_name}: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error while clipping {tif_name}: {e}")
        raise


def clip_tif_ext_rxr(tif, vector, out_tif=None, **rxr_kwargs):
    """
    Clips a raster file (TIF) to the extent of a vector file.

    Args:
        tif (str or xarray.DataArray): Path to the input TIF file to be clipped or an xarray DataArray.
        vector (str or gpd.GeoDataFrame): Path to the vector file used for clipping or a GeoDataFrame.
        out_tif (str, optional): Path to save the output clipped TIF file. If None, the clipped TIF is returned as an xarray DataArray.
        **rxr_kwargs: Extra keyword arguments for rioxarray.open_rasterio and rio.to_raster.

    Returns:
        xr.DataArray or None: The clipped raster as an xarray DataArray if out_tif is None, otherwise None.
    """
    import threading
    import geopandas as gpd

    tif, tif_name = validate_setting_tif(tif, **rxr_kwargs)

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
                lock=rxr_kwargs.get("lock") or threading.Lock(),
            )
            print(f"Clipped TIF saved as {out_tif_name}")
            return out_tif

        # Return the clipped TIF as an xarray DataArray
        print(f"Clipped TIF {tif_name} with {vector_name} completed.")
        return tif_clipped
    except Exception as e:
        print(f"Error clipping {tif_name} with {vector_name}")
        raise e


def check_tif_attr(tif_adjusted, tif_reference, **rxr_kwargs):
    """
    Checks the attributes of a raster file against a reference raster file and determines if adjustments are needed.

    Args:
        tif_adjusted (str, pathlib.Path, or xr.DataArray): The raster file to check.
        tif_reference (str, pathlib.Path, or xr.DataArray): The reference raster file.
        **rxr_kwargs: Extra keyword arguments for rioxarray.open_rasterio.

    Raises:
        FileNotFoundError: If the raster file to be checked does not exist.
        FileNotFoundError: If the reference raster file does not exist.

    Returns:
        bool: True if the raster matches the reference, False if adjustments are needed.
    """
    # Validate input types
    tif_adj, tif_adj_name = validate_setting_tif(tif_adjusted, **rxr_kwargs)

    tif_ref, tif_ref_name = validate_setting_tif(tif_reference, **rxr_kwargs)

    print(
        f"...comparing the raster {tif_adj_name} based on properties of {tif_ref_name}"
    )

    # Check the attributes of the raster files
    try:
        crs_test = tif_adj.rio.crs == tif_ref.rio.crs
        transform_test = tif_adj.rio.transform() == tif_ref.rio.transform()
        bounds_test = tif_adj.rio.bounds() == tif_ref.rio.bounds()
        resolution_test = tif_adj.rio.resolution() == tif_ref.rio.resolution()
        if crs_test and transform_test and bounds_test and resolution_test:
            print(
                f"...the raster {tif_adj_name} matches the reference {tif_ref_name}. No action will be taken."
            )
            return True

        # If any of the attributes do not match, return the attributes of the adjusted raster
        print(
            f"...the raster {tif_adj_name} does not match the reference {tif_ref_name}. Adjustments will be made."
        )
        return False
    except Exception as e:
        print(f"Error comparing the raster {tif_adj_name} with {tif_ref_name}")
        raise e


def adjust_tif(tif_no_adjust, tif_reference, tif_out=None, **rxr_kwargs):
    """
    Adjusts a raster file to match the spatial attributes of a reference raster file.

    Args:
        tif_no_adjust (str): Path to the raster file that needs to be adjusted.
        tif_reference (str): Path to the reference raster file.
        tif_out (str): Path to the output raster file that will match the reference.
        **rxr_kwargs: Extra keyword arguments for rioxarray.open_rasterio.

    Returns:
        None
    """
    # Validating and creating the array data
    tif_to_adjust, tif_to_adjust_name = validate_setting_tif(
        tif_no_adjust, **rxr_kwargs
    )
    tif_ref, tif_ref_name = validate_setting_tif(tif_reference, **rxr_kwargs)

    # Start the adjustment process
    print(f"...Adjusting the raster {tif_to_adjust_name} with {tif_ref_name}")

    if tif_out is not None:
        try:
            xmin, ymin, xmax, ymax = tif_ref.rio.bounds()
            gdalwarp_path = get_gdalwarp_path()
            gdal_adjust = (
                [gdalwarp_path]
                + ["-t_srs", str(tif_ref.rio.crs)]
                + ["-overwrite"]
                + ["-te", str(xmin), str(ymin), str(xmax), str(ymax)]
                + [
                    "-tr",
                    str(tif_ref.rio.resolution()[0]),
                    str(tif_ref.rio.resolution()[1]),
                ]
                + ["-r", "bilinear"]
                + ["-multi"]
                + ["-wo", "NUM_THREADS=ALL_CPUS"]
                + ["-srcnodata", "0"]
                + ["-dstnodata", "0"]
                + ["-co", "COMPRESS=DEFLATE"]
                + ["-co", "TILED=YES"]
                + [tif_no_adjust, tif_out]
            )
            subprocess.run(gdal_adjust, check=True)
            tif_out_name = os.path.basename(tif_out).split(".")[:-1][0]
            print(f"Adjusted raster saved as {tif_out_name}")
            return tif_out
        except subprocess.CalledProcessError as e:
            print(
                f"Could not adjust {tif_to_adjust_name} with {tif_ref_name} using gdalwarp."
            )
            print(f"Error: {e}")
            print("Trying to adjust using rioxarray...")

        # If gdalwarp fails, try using rioxarray
        try:
            tif_adjusted = tif_to_adjust.rio.reproject_match(tif_ref)
            tif_adjusted.rio.to_raster(
                tif_out,
                tiled=True,
                compress="DEFLATE",
                lock=rxr_kwargs.get("lock"),
            )
            tif_out_name = os.path.basename(tif_out).split(".")[:-1][0]
            print(f"Adjusted raster saved as {tif_out_name}")
            return
        except Exception as e:
            print(
                f"Error adjusting {tif_to_adjust_name} with {tif_ref_name} using rioxarray: {e}"
            )
            raise

    # If tif_out is None, we will return the adjusted raster
    print(
        f"...adjusting the raster {tif_to_adjust_name} to match {tif_ref_name} using rioxarray..."
    )
    tif_to_adj_clip = tif_to_adjust.rio.clip_box(
        *tif_ref.rio.bounds(),
        crs=tif_ref.rio.crs,
    )
    tif_adjusted = tif_to_adj_clip.rio.reproject_match(tif_ref)
    return tif_adjusted


def mask_by_tif(
    tif_mask,
    tif_to_mask,
    tif_out=None,
    **rxr_kwargs,
):
    """
    Masks a raster file using another raster file as a mask.

    Args:
        tif_mask (str, path object or xarray.DataArray): Path to the mask raster file or an xarray DataArray.
        tif_to_mask (str, path object or xarray.DataArray): Path to the raster file to be masked or an xarray DataArray.
        tif_out (str, optional): Path to save the output masked raster file. If None, the masked raster is returned as an xarray DataArray.
        **rxr_kwargs: Extra keyword arguments for rioxarray.open_rasterio.

    Returns:
        xr.DataArray or None: The masked raster as an xarray DataArray if tif_out is None, otherwise None.
    """
    # Validating and creating the array data
    tif_data, tif_to_mask_name = validate_setting_tif(tif_to_mask, **rxr_kwargs)
    mask_data, tif_mask_name = validate_setting_tif(tif_mask, **rxr_kwargs)

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
                lock=rxr_kwargs.get("lock"),
            )
            tif_out_name = os.path.basename(tif_out).split(".")[:-1][0]
            print(f"Masked TIF saved as {tif_out_name}")
            return tif_out
        return tif_data
    except Exception as e:
        print(f"Error masking the TIF {tif_to_mask_name} with {tif_mask_name}")
        raise e


def divide_intg_for_forest(
    tif_intg,
    periods,
    out_folder=None,
    **rxr_kwargs,
):
    """
    Divide an integrated alerts raster into multiple rasters by periods.

    Args:
        tif_intg (str): Path to the integrated alerts raster.
        periods (list of tuples): List of (start, end) date tuples.
        out_folder (str, optional): Output folder to save the period rasters.
        **rxr_kwargs: Extra keyword arguments for rioxarray.open_rasterio and rio.to_raster.

    Returns:
        dict: Dictionary of {period_name: xarray.DataArray}
    """
    import threading
    from tqdm import tqdm
    import pandas as pd

    # Validate and create array data
    tif_data, tif_name = validate_setting_tif(tif_intg, **rxr_kwargs)

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
            dic_tifs[f"{start}_{end}"] = {"name": name, "data": tif_intg_period}
            if out_folder is not None:
                out_tif_name = os.path.join(out_folder, f"{name}.tif")
                tif_intg_period.rio.to_raster(
                    out_tif_name,
                    tiled=True,
                    compress="DEFLATE",
                    lock=rxr_kwargs.get("lock") or threading.Lock(),
                )
        return dic_tifs
    except Exception as e:
        print(f"Error dividing the TIF {tif_name} into periods")
        raise e


def set_forest_data(out_data):
    """
    Downloads forest mask TIF files from GitLab if they do not already exist in the output directory.

    Args:
        out_data (str): Directory where the forest mask TIF files will be saved.
    """
    import time

    out_data = str(out_data)

    os.makedirs(out_data, exist_ok=True)
    present = time.strftime("%Y-%m-%d", time.localtime(time.time()))
    tifs_gitlab = {
        "2014-01-01_2017-12-31": {
            "url": "https://git.icf.gob.hn/alopez/adef-integ-tools/-/raw/main/data/bosque14_lzw.tif",
            "name": "bosque14_lzw",
        },
        "2018-01-01_2023-12-31": {
            "url": "https://git.icf.gob.hn/alopez/adef-integ-tools/-/raw/main/data/bosque18_lzw.tif",
            "name": "bosque18_lzw",
        },
        f"2024-01-01_{present}": {
            "url": "https://git.icf.gob.hn/alopez/adef-integ-tools/-/raw/main/data/bosque24_lzw.tif",
            "name": "bosque24_lzw",
        },
    }

    tif_local = {}
    for period, info in tifs_gitlab.items():
        tif_path = os.path.join(out_data, f"{info["name"]}.tif")
        tif_matched = os.path.join(out_data, f"{info["name"]}_matched.tif")
        if not os.path.exists(tif_path):
            print(f"Downloading forest mask {info["name"]} for the period {period}...")
            dw_tif(info["url"], tif_path)
        else:
            print(f"Forest mask {info["name"]} already exists. Skipping download.")
        tif_local[period] = {
            "name": info["name"],
            "forest_tif": tif_path,
            "matched_tif": tif_matched,
        }

    return tif_local


def mask_adefintg_forest(
    tif_path,
    path_forest_data,
    out_file=None,
    **rxr_kwargs,
):
    """
    Masks an integrated alerts raster (ADEF) by forest periods using forest mask rasters.

    Args:
        tif_path (str or xarray.DataArray): Path to the integrated alerts raster or an xarray DataArray.
        path_forest_data (str): Directory where forest mask TIF files are stored or will be downloaded.
        out_file (str, optional): Path to save the final masked raster. If None, returns the masked raster as an xarray DataArray.
        **rxr_kwargs: Extra keyword arguments for rioxarray.open_rasterio and rio.to_raster.

    Returns:
        str or xarray.DataArray: Path to the saved masked raster if out_file is provided, otherwise the masked raster as an xarray DataArray.
    """
    import time

    periods = [
        ("2014-01-01", "2017-12-31"),
        ("2018-01-01", "2023-12-31"),
        ("2024-01-01", time.strftime("%Y-%m-%d", time.localtime(time.time()))),
    ]

    tif_periods = divide_intg_for_forest(
        tif_intg=tif_path,
        periods=periods,
        **rxr_kwargs,
    )

    forest_dict = set_forest_data(path_forest_data)

    adef_masked = []
    for period_key, info in tif_periods.items():
        tif_period = info["data"]
        # Buscar la máscara de bosque cuyo 'period' coincida con period_key
        forest_path = None
        forest_matched = None
        for perdio_match, mask_info in forest_dict.items():
            if perdio_match == period_key:
                forest_path = mask_info.get("forest_tif") or mask_info.get("path")
                forest_matched = mask_info.get("matched_tif")
                break
        if forest_path is None:
            print(f"No forest mask found for period {period_key}. Skipping.")
            continue

        if not os.path.exists(forest_matched):
            f_match = adjust_tif(
                tif_no_adjust=forest_path,
                tif_reference=tif_period,
                tif_out=forest_matched,
                **rxr_kwargs,
            )
        else:
            test_attr = check_tif_attr(
                tif_adjusted=forest_matched, tif_reference=tif_period, **rxr_kwargs
            )
            if not test_attr:
                f_match = adjust_tif(
                    tif_no_adjust=forest_path,
                    tif_reference=tif_period,
                    tif_out=forest_matched,
                    **rxr_kwargs,
                )
            else:
                f_match = forest_matched

        adef_period_masked = mask_by_tif(
            tif_mask=f_match,
            tif_to_mask=tif_period,
            **rxr_kwargs,
        )
        adef_masked.append(adef_period_masked)

    # Combine the masked periods into a single xarray object
    if not adef_masked:
        return None
    adef_masked_joined = adef_masked[0]
    for masked in adef_masked[1:]:
        adef_masked_joined = adef_masked_joined + masked

    if out_file:
        print(f"Saving masked raster to {out_file}")
        adef_masked_joined.rio.to_raster(
            out_file, tiled=True, compress="DEFLATE", lock=rxr_kwargs.get("lock")
        )
        print("Masked raster saved successfully.")
        return out_file

    adef_masked_joined.name = "adef_masked_forest"
    return adef_masked_joined


def tif_adef_to_phid(tif_phid, tif_adef_masked, out_file=None, **rxr_kwargs):
    """_summary_

    Args:
        tif_phid (_type_): _description_
        tif_adef_masked (_type_): _description_
        tif_phid_match (_type_): _description_
        tif_out (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Check if tif_phid exists, if not, download it
    if not os.path.exists(tif_phid):
        tif_phid_git = (
            "https://git.icf.gob.hn/alopez/adef-integ-tools/-/raw/main/data/phid_hn.tif"
        )
        dw_tif(tif_phid_git, tif_phid)

    # Create the matched TIF path
    tif_phid_match = os.path.join(
        os.path.dirname(tif_phid),
        f"{os.path.basename(tif_phid).split('.')[0]}_matched.tif",
    )

    tif_adef_masked, tif_adef_masked_name = validate_setting_tif(
        tif_adef_masked, **rxr_kwargs
    )

    print(f"Agregando areas de Proteccion Hidrica a {tif_adef_masked_name}")

    if not os.path.exists(tif_phid_match):
        tif_phid_matched = adjust_tif(
            tif_phid, tif_adef_masked, tif_phid_match, **rxr_kwargs
        )
    else:
        test_attr = check_tif_attr(
            tif_adjusted=tif_phid_match, tif_reference=tif_adef_masked, **rxr_kwargs
        )
        if not test_attr:
            tif_phid_matched = adjust_tif(
                tif_no_adjust=tif_phid,
                tif_reference=tif_adef_masked,
                tif_out=tif_phid_match,
                **rxr_kwargs,
            )
        else:
            tif_phid_matched = tif_phid_match

        # Crear el tif de adef con phid

    try:
        print("...iniciando la creación del adef combinado con phid...")
        tif_phid_matched, _ = validate_setting_tif(tif_phid_matched, **rxr_kwargs)
        tif_phid_matched = tif_phid_matched.astype("int32")
        tif_phid_to_sum = tif_phid_matched * 100000

        tif_phid_within_adef = tif_phid_to_sum.where(tif_adef_masked > 0, 0)

        adef_with_phid = tif_adef_masked + tif_phid_within_adef

    except Exception as e:
        print(f"Error al crear el adef combinado: {e}")
        raise

    if out_file is not None:
        # Guardar el adef combinado
        out_file_name = os.path.basename(out_file).split(".")[:-1][0]
        adef_with_phid.rio.to_raster(
            out_file, tiled=True, compress="DEFLATE", lock=rxr_kwargs.get("lock")
        )
        print(
            f"TIF de adef combinado con phid; '{out_file_name}' creado y exportado correctamente"
        )
        return out_file
    return adef_with_phid


def filter_adef_intg_conf(tif, confidence=1, out_file=None, **rxr_kwargs):
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
    tif_data, tif_name = validate_setting_tif(tif, **rxr_kwargs)

    # Start the filtering process
    print(
        f"Filtering the TIF {tif_name} with confidence level {confidence} and greater..."
    )
    try:
        mask = tif_data // 10000 >= confidence
        adef_intg_conf = tif_data.where(mask)
        adef_intg_conf.name = tif_name
        if out_file is not None:
            tif_out_name = os.path.basename(out_file).split(".")[:-1][0]
            dir_out = os.path.dirname(out_file)
            os.makedirs(dir_out, exist_ok=True)
            adef_intg_conf.rio.to_raster(
                out_file,
                tiled=True,
                compress="DEFLATE",
                lock=rxr_kwargs.get("lock"),
            )
            print(f"Filtered TIF saved as {tif_out_name}")
            return out_file
        print(f"Filtered TIF {tif_name} with confidence level {confidence} completed.")
        return adef_intg_conf
    except Exception as e:
        print(f"Error filtering the TIF {tif_name} with confidence level {confidence}")
        raise e


def filter_adef_intg_time(tif, filter_time, out_file=None, **rxr_kwargs):
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
        tif_data, tif_name = validate_setting_tif(tif, **rxr_kwargs)

        # Define the zero day and calculate days from the TIF data
        zero_day = pd.Timestamp("2014-12-31")
        tif_in_days = tif_data % 10000

        # Determine the filter type and apply the corresponding logic
        filter_type = filter_time[0]
        if filter_type == "Last":
            filter_quantity = filter_time[1]
            filter_units = filter_time[2]

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
            filter_start = pd.Timestamp(filter_time[1])
            filter_end = pd.Timestamp(filter_time[2])
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
        if out_file is not None:
            try:
                out_file_name = os.path.basename(out_file).split(".")[:-1][0]
                print(f"Saving the filtered TIF as {out_file_name}...")
                tif_filtered.rio.to_raster(
                    out_file,
                    tiled=True,
                    compress="DEFLATE",
                    lock=rxr_kwargs.get("lock"),
                )
                print(f"Filtered TIF saved as {out_file_name}")
                return out_file
            except Exception as e:
                print(f"Error saving the filtered TIF: {e}")
                raise
        return tif_filtered
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
