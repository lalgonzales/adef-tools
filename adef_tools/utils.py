"""Utils for ADEF tools."""

import os
import sys
import shutil
import platform
from pathlib import Path
import pandas as pd
import geopandas as gpd
import rioxarray as rxr
import xarray as xr
import yaml
from sqlalchemy import create_engine


def validate_setting_vector(vector, **gpd_kwargs):
    """
    Validate and load a vector file or GeoDataFrame, returning the object and its name.

    Args:
        vector (str, pathlib.Path, or gpd.GeoDataFrame): Path to the vector file or a GeoDataFrame.
        **gpd_kwargs: Extra keyword arguments for geopandas.read_file/read_parquet.

    Raises:
        FileNotFoundError: If the vector file path does not exist.
        TypeError: If the input is not a valid type.
        ValueError: If the GeoDataFrame does not have a CRS.

    Returns:
        tuple: (GeoDataFrame, vector name)
    """
    if isinstance(vector, (str, Path)):
        if not os.path.exists(vector):
            raise FileNotFoundError(f"The input vector file does not exist: {vector}")
        ext = os.path.splitext(vector)[1].lower()
        if ext == ".parquet":
            gdf = gpd.read_parquet(vector, **gpd_kwargs)
        else:
            gdf = gpd.read_file(vector, **gpd_kwargs)
        vector_name = os.path.splitext(os.path.basename(vector))[0]
    elif isinstance(vector, gpd.GeoDataFrame):
        gdf = vector
        # Use .name attribute if present, otherwise assign a generic name
        vector_name = getattr(gdf, "name", "vector")
    else:
        raise TypeError("The input must be a string, pathlib.Path, or GeoDataFrame.")
    if gdf.crs is None:
        raise ValueError("The GeoDataFrame must have a CRS defined.")
    return gdf, vector_name


def validate_setting_tif(tif, **rxr_kwargs):
    """
    Validate and load a TIF file or xarray.DataArray, returning the object and its name.

    Args:
        tif (str, pathlib.Path, or xr.DataArray): Path to the TIF file or an xarray DataArray.
        **rxr_kwargs: Extra keyword arguments for rioxarray.open_rasterio.

    Raises:
        FileNotFoundError: If the TIF file path does not exist.
        TypeError: If the input is not a valid type.
        ValueError: If the DataArray does not have a name.

    Returns:
        tuple: (tif object, tif name)
    """
    if isinstance(tif, (str, Path)):
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


def get_gdal_rasteize_path():
    """
    Determines the path to the `gdal_rasterize` utility based on the operating system.

    Raises:
        FileNotFoundError: If `gdal_rasterize` is not found on the system.

    Returns:
        str: The full path to the `gdal_rasterize` utility.
    """
    system = platform.system().lower()

    if system == "linux":
        path = shutil.which("gdal_rasterize")
        if path:
            return path  # binary found in PATH

    elif system == "windows":
        # Look for gdal_polygonize.py in Conda or Python scripts folder
        conda_prefix = os.environ.get("CONDA_PREFIX", "")
        candidates = [
            os.path.join(conda_prefix, "Scripts", "gdal_rasterize.py"),
            os.path.join(sys.prefix, "Scripts", "gdal_rasterize.py"),
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


def get_safe_lock(name="rio", client=None):
    """
    Returns a Dask distributed lock if a Dask client is available, otherwise returns a local threading lock.

    Args:
        name (str): Name of the lock.
        client (Client, optional): Dask client instance. If not provided, tries to detect an active client.

    Returns:
        Lock: A Dask distributed Lock or a threading.Lock.
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
                "Invalid parameters: 'year' is required for 'GLAD'. For 'INTEGRATED', only 'adef_src' is needed."
            )

        # Decompose date using vectorized methods
        gdf["mes"] = gdf["fecha"].dt.month.map(lambda m: months_of_year[m - 1])
        gdf["dia"] = gdf["fecha"].dt.weekday.map(lambda d: days_of_week[d])
        gdf["semana"] = gdf["fecha"].dt.isocalendar().week

        return gdf

    except Exception as e:
        print(f"Error processing dates: {e}")
        raise


def load_yaml(yaml_file=None) -> dict:
    """
    Load configuration from a YAML file and expand environment variables (e.g., $DB_MAIN_USER).

    Args:
        yaml_file (str or Path, optional): Path to the YAML config file.

    Returns:
        dict: Parsed YAML configuration as a dictionary.
    """
    try:
        with open(yaml_file, "r", encoding="UTF-8") as file:
            content = file.read()

        # Replace $VAR with its value from os.environ
        import re

        content = re.sub(
            r"\$(\w+)", lambda m: os.getenv(m.group(1), f"${m.group(1)}"), content
        )

        data = yaml.safe_load(content)
        return data
    except (FileNotFoundError, yaml.YAMLError, OSError) as e:
        print(f"[load_yaml] Error loading YAML file '{yaml_file}': {e}")
        raise


def build_engine(db_config: dict, **kwargs):
    """
    Create a SQLAlchemy engine from the provided configuration.

    Args:
        db_config (dict): Database connection parameters. Must include keys:
            'user', 'password', 'host', 'port', 'database'.
        **kwargs: Additional keyword arguments for SQLAlchemy's create_engine.

    Returns:
        sqlalchemy.engine.Engine or None: SQLAlchemy engine instance, or None if creation fails.
    """
    try:
        df_user = db_config.get("user")
        df_pass = db_config.get("password")
        df_host = db_config.get("host")
        df_port = db_config.get("port")
        df_db = db_config.get("database")
        if not all([df_user, df_pass, df_host, df_port, df_db]):
            raise ValueError("Database configuration is incomplete.")
        engine = create_engine(
            f"postgresql://{df_user}:{df_pass}@{df_host}:{df_port}/{df_db}",
            **kwargs,
        )
        return engine
    except (OSError, ValueError, TypeError, AttributeError, ImportError) as e:
        print(f"[build_engine] Error creating engine: {e}")
        return None
