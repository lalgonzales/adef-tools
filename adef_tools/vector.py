"""Vector functions for ADEF tools."""

import os
import subprocess
import time
import warnings
from pathlib import Path
from tqdm import tqdm
import geopandas as gpd
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.orm import sessionmaker
from shapely.errors import ShapelyDeprecationWarning
from adef_tools.utils import (
    validate_setting_tif,
    validate_setting_vector,
    get_gdal_rasteize_path,
)

# Ignore GeoPandas warnings related to CRS
warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS.")

# Optional: Ignore Shapely warnings if they occur
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)


def load_table_as_gdf_or_df(sql, engine, is_geo=True, retries=3):
    """
    Load the result of a SQL query from a database as a GeoDataFrame or DataFrame, with retry logic.

    Args:
        sql (str): SQL query to execute. Must be a complete SELECT statement.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for the database.
        is_geo (bool, optional): If True, load as GeoDataFrame. If False, as DataFrame. Defaults to True.
        retries (int, optional): Number of retry attempts on failure. Defaults to 3.

    Returns:
        geopandas.GeoDataFrame or pandas.DataFrame: Loaded query result as GeoDataFrame or DataFrame.
    """
    if engine is None:
        raise ValueError("A valid SQLAlchemy engine must be provided.")
    if not sql:
        raise ValueError("A valid SQL query string must be provided.")
    Session = sessionmaker(bind=engine)
    for attempt in range(retries):
        session = Session()
        connection = session.connection()
        try:
            if is_geo:
                gdf = gpd.read_postgis(sql=sql, con=connection)
                gdf.geometry = gdf.geometry.make_valid()
            else:
                df = pd.read_sql(sql=sql, con=connection)
                gdf = gpd.GeoDataFrame(df, geometry=None)
            session.close()
            return gdf
        except (SQLAlchemyError, OperationalError) as e:
            session.rollback()
            print(
                f"[load_table_as_gdf_or_df] Attempt {attempt + 1} of {retries} failed: {e}"
            )
            time.sleep(5)
        finally:
            session.close()
    print(
        f"[load_table_as_gdf_or_df] Could not load after {retries} attempts. Query: {sql}"
    )
    return None


def vector_to_tif_gdal(
    vector, tif_reference, out_file, rxr_kwargs=None, gpd_kwargs=None
):
    """
    Rasterize a vector file using GDAL, matching a reference raster's extent and resolution.

    Args:
        vector (str, Path, or GeoDataFrame): Input vector file or GeoDataFrame.
        tif_reference (str, Path, or xarray.DataArray): Reference raster for extent/resolution.
        out_file (str or Path): Output raster file path.
        rxr_kwargs (dict, optional): Extra kwargs for rioxarray.open_rasterio.
        gpd_kwargs (dict, optional): Extra kwargs for geopandas.read_file/to_file.

    Returns:
        str: Path to the output raster file.
    """
    # Set default arguments if not provided
    out_folder = os.path.dirname(out_file)
    out_tmp_vector = os.path.join(out_folder, "tmp_vector.gpkg")

    # Validate the inputs
    gdf, vector_name = validate_setting_vector(vector, **(gpd_kwargs or {}))
    tif_data, _ = validate_setting_tif(tif_reference, **(rxr_kwargs or {}))
    print(f"Converting {vector_name} to raster...")

    # Temporary file for the vector
    if isinstance(vector, (str, Path)):
        out_tmp_vector = vector
    else:
        gdf.to_file(out_tmp_vector, **(gpd_kwargs or {}))

    # Create auxiliary attributes for rasterization
    xres, yrex = tif_data.rio.resolution()[0], tif_data.rio.resolution()[1]
    xmin, ymin, xmax, ymax = tif_data.rio.bounds()

    # Get the GDAL rasterize path
    rasterize_path = get_gdal_rasteize_path()
    print(f"Using {rasterize_path} to convert the vector to raster")

    command = (
        [rasterize_path]
        + ["-at"]
        + ["-a", "id"]
        + ["-te", str(xmin), str(ymin), str(xmax), str(ymax)]
        + ["-tr", str(xres), str(-yrex)]
        + ["-of", "GTiff"]
        + ["-a_nodata", "0"]
        + ["-co", "COMPRESS=DEFLATE"]
        + ["-co", "TILED=YES"]
        + [out_tmp_vector]
        + [out_file]
    )

    try:
        subprocess.run(command, check=True)
        print(f"Raster file saved as {out_file}")
        return out_file
    except subprocess.CalledProcessError as e:
        print(f"Error running gdal_rasterize for {vector_name}: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error while running gdal_rasterize for {vector_name}: {e}")
        raise
    finally:
        # Clean up temporary vector file
        if os.path.exists(out_tmp_vector) and not isinstance(vector, (str, Path)):
            os.remove(out_tmp_vector)


def calculate_area_ha(gdf_gcs, field_area_name):
    """
    Calculate the area in hectares for each geometry in a GeoDataFrame. Geometries must be in
    EPSG:4326. The area is calculated based on the UTM zone where the centroid of each geometry is
    located. For geometries in UTM zone 16, EPSG:32616 is used; for zone 17, EPSG:32617 is used.

    Args:
        gdf_gcs (geopandas.GeoDataFrame): GeoDataFrame with geometries in EPSG:4326 CRS.
        field_area_name (str): Name of the column to store the calculated area in hectares.

    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with an additional column containing the area in
        hectares.
    """
    # Validate if the GeoDataFrame is in 'EPSG:4326'
    if gdf_gcs.crs.to_epsg() != 4326:
        print(
            f"The expected CRS is 'EPSG:4326' but the GeoDataFrame has 'EPSG:{gdf_gcs.crs.to_epsg()}'."
        )
        return

    # Start the area calculation process
    try:
        gdf_columns = [col for col in gdf_gcs.columns if col != field_area_name]
        gdf_gcs = gdf_gcs.copy()
        gdf_gcs["centroid_x"] = gdf_gcs.geometry.centroid.x
        gdf_gcs.loc[gdf_gcs["centroid_x"] < -84, "geom_32616"] = (
            gdf_gcs.geometry.to_crs("EPSG:32616")
        )
        gdf_gcs.loc[gdf_gcs["centroid_x"] >= -84, "geom_32617"] = (
            gdf_gcs.geometry.to_crs("EPSG:32617")
        )
        gdf_gcs.loc[pd.notna(gdf_gcs["geom_32616"]), field_area_name] = (
            gdf_gcs["geom_32616"].area / 10000
        )
        gdf_gcs.loc[pd.notna(gdf_gcs["geom_32617"]), field_area_name] = (
            gdf_gcs["geom_32617"].area / 10000
        )
        gdf_gcs = gdf_gcs[gdf_columns + [field_area_name]]
        return gdf_gcs
    except Exception as e:
        print("An error occurred while calculating the area in hectares.")
        raise e


def add_lim_attributes(gdf_base, dict_data_rem_lim, keys_lim, index_field=None):
    """
    Add attributes from boundary layers to alerts in the given GeoDataFrame. Intersects the input
    GeoDataFrame with boundary layers specified in the dictionary and adds the attributes from the
    boundary layers to the alerts. If an alert does not intersect with any boundary layer, performs a
    spatial join within a specified distance.

    Args:
        gdf_base (geopandas.GeoDataFrame): Alerts GeoDataFrame.
        dict_data_rem_lim (dict): Dictionary containing boundary layers as GeoDataFrames.
        keys_lim (list): List of keys to identify which boundary layers to use from the dictionary.
        index_field (str, optional): Field to use as index for merging.

    Returns:
        geopandas.GeoDataFrame: GeoDataFrame with added boundary attributes.
    """
    print("Adding attributes from boundary layers to alerts...")
    data_lim = {
        key: dict_data_rem_lim[key] for key in keys_lim if key in dict_data_rem_lim
    }
    try:
        data_intersect = gdf_base.copy()
        data_intersect["centroid"] = data_intersect.geometry.to_crs(
            "EPSG:3857"
        ).representative_point()
        data_intersect = data_intersect[["centroid", index_field]].copy()
        data_intersect.set_geometry("centroid", inplace=True)
        data_intersect = data_intersect.to_crs(gdf_base.crs)
    except Exception as e:
        print(f"Error creating centroids for boundary assignment: {e}")
        raise
    try:
        for key, gdf in tqdm(data_lim.items(), desc="Adding attributes from layers"):
            data_intersect = data_intersect.sjoin(
                gdf, how="left", predicate="intersects"
            )
            data_intersect.drop(columns=["index_right"], inplace=True)
            data_joined = data_intersect[data_intersect.iloc[:, -1].notna()].copy()
            data_no_joined = data_intersect[data_intersect.iloc[:, -1].isna()].copy()
            if data_no_joined.shape[0] > 0:
                geom_column_gdf = gdf.geometry.name
                columns_to_drop = [col for col in gdf.columns if col != geom_column_gdf]
                data_no_joined.drop(columns=columns_to_drop, inplace=True)
                data_union = data_no_joined.sjoin(
                    gdf, how="left", predicate="dwithin", distance=0.013
                )  # 0.013 degrees, ~1,500 meters.
                data_union.drop(columns=["index_right"], inplace=True)
                data_intersect = pd.concat([data_joined, data_union])
                print(f"Attributes from layer {key} added to alerts.")
            else:
                data_intersect = data_joined.copy()
                print(f"Attributes from layer {key} added to alerts.")
        data_intersect.drop(columns=["centroid"], inplace=True)
        gdf_new_data = gdf_base.merge(data_intersect, on=index_field, how="left")
        print("Attributes from boundary layers added to alerts.")
    except Exception as e:
        print(f"Error adding attributes from boundary layers: {e}")
        raise
    return gdf_new_data


def add_rem_attributes(gdf_base, dict_data_rem_lim, keys_rem_pma, keys_rem_pa):
    """
    Add attributes from multiple GeoDataFrames to a base GeoDataFrame by performing spatial joins.

    Args:
        gdf_base (geopandas.GeoDataFrame): Base GeoDataFrame to which attributes will be added.
        dict_data_rem_lim (dict): Dictionary containing GeoDataFrames to be joined with the base.
        keys_rem_pma (list): List of keys for PMA areas.
        keys_rem_pa (list): List of keys for PA areas.

    Returns:
        geopandas.GeoDataFrame: Updated GeoDataFrame with added attributes.
    """
    print("Adding attributes from REM layers to alerts...")
    keys_rem = keys_rem_pma + keys_rem_pa
    dic_data_rem = {
        key: dict_data_rem_lim[key] for key in keys_rem if key in dict_data_rem_lim
    }
    try:
        for key, gdf_rem in tqdm(dic_data_rem.items(), desc="Processing REM layers"):
            if "index_left" in gdf_base.columns or "index_right" in gdf_base.columns:
                gdf_base.drop(
                    columns=["index_left", "index_right"], errors="ignore", inplace=True
                )
            gdf_base = gdf_base.sjoin(gdf_rem, how="left", predicate="intersects")
            print(f"Attributes from layer {key} added to alerts.")
        print("Attributes from REM layers added to alerts.")
    except Exception as e:
        print(f"Error adding attributes from REM layers: {e}")
        raise
    return gdf_base


def save_to_file(vector, out_file, **gpd_kwargs):
    """
    Save a vector (GeoDataFrame or file path) to the specified file format.

    Args:
        vector (str, Path, or GeoDataFrame): Vector data to save.
        out_file (str or Path): Output file path.
        **gpd_kwargs: Extra keyword arguments for GeoPandas save functions.

    Returns:
        str: Path to the saved file.
    """
    gdf, _ = validate_setting_vector(vector, **gpd_kwargs)
    try:
        if out_file.endswith(".parquet"):
            gdf.to_parquet(out_file, **gpd_kwargs)
        elif out_file.endswith(".feather"):
            gdf.to_feather(out_file, **gpd_kwargs)
        else:
            gdf.to_file(out_file, **gpd_kwargs)
        print(f"File saved successfully as {out_file}")
        return out_file
    except Exception as e:
        print(f"Error saving file {out_file}: {e}")
        raise e
