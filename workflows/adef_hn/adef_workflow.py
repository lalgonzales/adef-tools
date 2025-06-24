"""
Workflow for processing ADEF data.

This module contains the main workflow functions for the ADEF (Deforestation
Alerts) process, including:

- prepare_adefgfw: Prepares and processes raster and vector data for ADEF alerts,
  applying steps such as download, clipping, masking, date filtering, confidence
  filtering, hydric protection, and vectorization. Each step is controlled by the
  configuration.
- table_updates_workflow: Updates the ADEF database tables, including identifying
  new alerts, updating main and historic tables, and saving debug outputs if
  enabled.

All functions are designed for clarity, maintainability, and integration with the
ADEF workflow configuration system.
"""

import os
import time
from pathlib import Path
from db_funcs import ADEFUPDATE
from adef_tools.adef_fn import ADEFINTG
from adef_tools.utils import get_safe_lock
from adef_tools.utils_adef import default_vector
from adef_tools.vector import validate_setting_vector


# Sync dev with production


def prepare_adefgfw(config):  # pylint: disable=too-many-locals
    """
    Prepare and process raster and vector data for ADEF alerts.

    This function executes the main preprocessing steps for ADEF alerts, including:
    - Downloading the raster file if required.
    - Clipping the raster to the area of interest.
    - Masking non-forest areas.
    - Filtering by date range and confidence level.
    - Adding hydric protection information.
    - Vectorizing the raster output.

    Each step is controlled by the configuration object.

    Args:
        config (MainConfig): Configuration object with all workflow parameters.

    Returns:
        Path or None: Path to the final vectorized output, or None if not vectorized.
    """
    print("Iniciando la preparación de ADEF GFW...")
    lock = get_safe_lock("rio-read", config.client) if config.use_lock else None
    steps = config.steps

    adef = ADEFINTG()
    tif_adef_path = config.results_dir / config.tif_adef_name
    current_raster = tif_adef_path

    if "download" in steps:
        adef.download(tif_adef_path)

    if "clip" in steps:
        gdf = (
            validate_setting_vector(config.vector_clip)
            if config.vector_clip
            else default_vector()
        )
        clipped_path = config.results_dir / config.tif_clip_name
        tif_clipped = adef.clip_to_ext(
            gdf,
            out_file=clipped_path,
            tif_path=current_raster,
            rxr_kwargs={"lock": lock, "chunks": 1024},
        )
        current_raster = tif_clipped
    elif (config.results_dir / config.tif_clip_name).exists():
        current_raster = config.results_dir / config.tif_clip_name

    if "mask" in steps:
        forest_masked = adef.mask_forests(
            path_forest_data=config.data_dir,
            tif_path=current_raster,
            out_file=None,
            rxr_kwargs={"lock": lock, "chunks": 1024},
        )
        current_raster = forest_masked
    elif (config.results_dir / config.tif_masked_name).exists():
        current_raster = config.results_dir / config.tif_masked_name

    if "time" in steps:
        tif_filtered_date = adef.filter_by_date_range(
            tif_path=current_raster,
            filter_time=config.date_range,
            out_file=None,
            rxr_kwargs={"lock": lock, "chunks": 1024},
        )
        current_raster = tif_filtered_date
    elif (config.results_dir / config.tif_time_name).exists():
        current_raster = config.results_dir / config.tif_time_name

    if "confidence" in steps and config.confidence != 1:
        tif_filtered = adef.filter_by_confidence(
            tif_path=current_raster,
            confidence_level=config.confidence,
            out_file=None,
            # out_file=config.results_dir / config.tif_confidence_name,
            rxr_kwargs={"lock": lock, "chunks": 1024},
        )
        current_raster = tif_filtered
    elif (config.results_dir / config.tif_confidence_name).exists():
        current_raster = config.results_dir / config.tif_confidence_name

    if "phid" in steps:

        phid_path_rel = Path(os.getenv("PHID_PATH"))
        phid_path = config.base_dir / phid_path_rel or config.data_dir / "phid_hn.tif"

        # Remove previous output to guarantee fresh run
        phid_out_path = config.results_dir / config.tif_with_phid_name
        phid_out_path.unlink(missing_ok=True)

        adef_with_phid = adef.add_phid(
            phid_path=phid_path,
            tif_path=current_raster,
            out_file=phid_out_path,
            rxr_kwargs={"lock": lock, "chunks": 1024},
        )
        current_raster = adef_with_phid
    elif (config.results_dir / config.tif_with_phid_name).exists():
        current_raster = config.results_dir / config.tif_with_phid_name

    if "vectorize" in steps:
        vector_out_rel = config.vector_gfw
        vector_out = config.base_dir / vector_out_rel
        # Remove previous vector output to guarantee fresh run
        vector_out.unlink(missing_ok=True)
        vectorized = adef.to_vector(
            tif_path=current_raster,
            out_folder=config.results_dir,
            out_file=vector_out,
            rxr_kwargs={"lock": lock, "chunks": 1024},
            gpd_kwargs=None,
        )
        return {
            "confidence_used": config.confidence if "confidence" in steps else None,
            "date_range_used": config.date_range if "time" in steps else None,
            "vector_path": vectorized,
            "vector_num_geoms": None,
        }
    return {
        "confidence_used": config.confidence if "confidence" in steps else None,
        "date_range_used": config.date_range if "time" in steps else None,
        "vector_path": None,
        "vector_num_geoms": None,
    }


def table_updates_workflow(
    config,
):  # pylint: disable=too-many-locals, too-many-statements
    """
    Update ADEF database tables and handle new/updated alerts.

    This function identifies new alerts, updates the main and historic ADEF tables,
    and saves debug outputs if enabled. It performs the following steps:
    - Loads and filters new vector data.
    - Identifies unique geometries for update.
    - Creates and saves the update table.
    - Prepares and inserts new alerts into the main table.
    - Updates the historic table with new codes and geometries.

    Args:
        config (MainConfig): Configuration object with all workflow parameters.
    """
    if config.use_lock:
        get_safe_lock("dgpd", config.client)

    # Define base directory
    base_dir = config.base_dir
    db_mapping_file = base_dir / "adef_hn" / "db_mapping.yaml"

    updater = ADEFUPDATE(db_mapping_file=db_mapping_file)

    # Out gpkg for debugging
    if config.debug:
        gpgk_rel = os.getenv("OUT_GPKG")
        out_gpkg = base_dir / gpgk_rel or config.results_dir / "debug.gpkg"
        nowstrfmt = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        print(f"Debug mode is ON. Output will be saved to {out_gpkg}")

    # Load the vector GFW path from environment variable or use default
    vector_gfw_relpath = Path(os.getenv("VECTOR_GFW"))
    vector_gfw_path = (
        base_dir / vector_gfw_relpath or config.results_dir / "vector.parquet"
    )
    vector_gfw, _ = validate_setting_vector(vector_gfw_path)

    # New data to be added
    data_new = updater.discard_data_in_db(
        data_gfw=vector_gfw,
    )

    if config.debug:
        print("Saving new data to be processed...")
        layer_new = "data_new"
        data_new["time_wrflw"] = nowstrfmt
        data_new.to_file(out_gpkg, layer=layer_new, mode="a")
        data_new.drop(columns=["time_wrflw"], inplace=True, errors="ignore")

    # Get the unique geometry
    unique_geoms = updater.geom_interes(data_new)
    if config.debug:
        print("Saving unique geometries...")
        layer_unique = "unique_geoms"
        unique_geoms["time_wrflw"] = nowstrfmt
        unique_geoms.to_file(out_gpkg, layer=layer_unique, mode="a")
        unique_geoms.drop(columns=["time_wrflw"], inplace=True, errors="ignore")

    # Create the table updates
    table_updates, ids_to_reemp = updater.case_update(
        unique_geoms,
        data_new,
    )
    if config.debug:
        print("Saving table updates...")
        layer_updates = "table_updates"
        table_updates["time_wrflw"] = nowstrfmt
        columns_to_keep = [col for col in table_updates.columns if col != "geom_adefpg"]
        table_updates[columns_to_keep].to_file(out_gpkg, layer=layer_updates, mode="a")
        table_updates.drop(columns=["time_wrflw"], inplace=True, errors="ignore")

    # Prepare the new alerts
    data_for_update = updater.set_new_alerts(table_updates=table_updates)
    if data_for_update is not None:
        data_with_attrib = updater.add_attributes_to_new_alerts(data_for_update)
        updater.update_main_db_table(data_with_attrib, ids_to_reemp)
        if config.debug:
            print("Saving data with attributes...")
            data_with_attrib["time_wrflw"] = nowstrfmt
            data_with_attrib.to_file(out_gpkg, layer="data_with_attrib", mode="a")
        print("ADEF table updated successfully.")
    else:
        print("No new data to update in the ADEF table.")

    # Prepare data to historic
    gdf_not_in_hist, cod_values = updater.set_new_alert_to_hist(vector_gfw)

    # Updating the historic table
    n_geom_inicial = len(vector_gfw) if hasattr(vector_gfw, "__len__") else None
    n_geom_new = len(data_new) if hasattr(data_new, "__len__") else None
    n_insert_pg = (
        len(data_for_update)
        if data_for_update is not None and hasattr(data_for_update, "__len__")
        else 0
    )
    n_insert_pg_hist = (
        len(gdf_not_in_hist)
        if gdf_not_in_hist is not None and hasattr(gdf_not_in_hist, "__len__")
        else 0
    )

    if gdf_not_in_hist is not None:
        updater.update_hist_db_table(gdf_not_in_hist, cod_values)
        if config.debug:
            print("Saving new data to historic...")
            gdf_not_in_hist["time_wrflw"] = nowstrfmt
            gdf_not_in_hist.to_file(out_gpkg, layer="gdf_not_in_hist", mode="a")
        print("Historic table updated successfully.")
    elif cod_values is not None:
        import geopandas as gpd

        gdf_not_in_hist = gpd.GeoDataFrame()
        updater.update_hist_db_table(gdf_not_in_hist, cod_values)
    else:
        print("No new data to update in the historic table.")

    # (Resumen de parámetros y resultados ahora solo se imprime en main.py)
    return {
        "n_geom_inicial": n_geom_inicial,
        "n_geom_new": n_geom_new,
        "n_insert_pg": n_insert_pg,
        "n_insert_pg_hist": n_insert_pg_hist,
    }
