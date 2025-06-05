import time
from tqdm import tqdm
from adef_tools.adef_fn import ADEFINTG
from adef_tools.raster import get_safe_lock
from adef_tools.utils_adef import default_vector
from adef_tools.vector import validate_setting_vector


def run_adef_workflow(config):
    steps = config.steps

    lock = get_safe_lock("rio-read", config.client) if config.use_lock else None
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

    if "mask" in steps:
        if (config.results_dir / config.tif_clip_name).exists():
            current_raster = config.results_dir / config.tif_clip_name
        forest_masked = adef.mask_forests(
            path_forest_data=config.data_dir,
            tif_path=current_raster,
            out_file=None,
            rxr_kwargs={"lock": lock, "chunks": 1024},
        )
        current_raster = forest_masked

    if "confidence" in steps and config.confidence != 1:
        tif_filtered = adef.filter_by_confidence(
            tif_path=current_raster,
            confidence_level=config.confidence,
            out_file=None,
            rxr_kwargs={"lock": lock, "chunks": 1024},
        )
        current_raster = tif_filtered

    if "time" in steps:
        tif_filtered_date = adef.filter_by_date_range(
            tif_path=current_raster,
            filter_time=config.date_range,
            out_file=None,
            rxr_kwargs={"lock": lock, "chunks": 1024},
        )
        current_raster = tif_filtered_date

    if "phid" in steps:
        phid_path = config.data_dir / "phid_hn.tif"
        adef.add_phid(
            phid_path=phid_path,
            tif_path=current_raster,
            out_file=config.results_dir / config.tif_with_phid_name,
            rxr_kwargs={"lock": lock, "chunks": 1024},
        )

    if "tabla_actualizaciones" in steps:
        # Aquí iría tu lógica para crear la tabla de actualizaciones
        pass

    if "actualiza_db" in steps:
        # Aquí iría tu lógica para actualizar la base de datos
        pass
