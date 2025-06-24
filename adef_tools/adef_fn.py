"""Alerts processing functions."""

from adef_tools import raster


class ADEFINTG:

    def __init__(self, url=None):
        self.url = url or (
            "https://data-api.globalforestwatch.org/dataset/gfw_integrated_alerts/"
            "latest/download/geotiff?grid=10/100000&tile_id=20N_090W&pixel_meaning="
            "date_conf&x-api-key=2d60cd88-8348-4c0f-a6d5-bd9adb585a8c"
        )
        self.tif_dir = None
        self.tif_file = None
        self.tif_path = None

    def download(self, out_file):
        """Download the ADEFINTG file."""

        raster.dw_tif(self.url, out_file)
        return out_file

    def clip_to_ext(
        self, ext_vector, out_file=None, tif_path=None, rxr_kwargs=None, gpd_kwargs=None
    ):
        """
        Clip the raster file to the provided vector.

        Args:
            ext_vector: Vector (GeoDataFrame, path, etc.) to clip with.
            out_file: Output file path. If None, returns xarray/dask object.
            tif_path: Optional. Path or URL to the raster to clip. If None, uses self.tif_path or self.url.
            rxr_kwargs: dict for rioxarray.open_rasterio and rio.to_raster.
            gpd_kwargs: dict for geopandas.read_file.

        Returns:
            If out_file is set: path to the clipped raster.
            If out_file is None: xarray/dask object.
        """
        rxr_kwargs = rxr_kwargs or {}
        gpd_kwargs = gpd_kwargs or {}

        # Prioridad: tif_path > self.tif_path > self.url
        tif_source = tif_path or self.tif_path or self.url
        if tif_source is None:
            raise RuntimeError(
                "No raster source provided. Set tif_path, self.tif_path, or self.url."
            )

        try:
            adef_clipped = raster.clip_tif_ext_gdal(
                tif_source,
                ext_vector,
                out_file,
                rxr_kwargs=rxr_kwargs,
                gpd_kwargs=gpd_kwargs,
            )
            return adef_clipped
        except RuntimeError as e:
            print(f"Clipping with gdal failed: {e}")

        try:
            adef_clipped = raster.clip_tif_ext_rxr(
                tif_source, ext_vector, out_file, **rxr_kwargs
            )
            return adef_clipped
        except RuntimeError as e:
            print(f"Clipping with rioxarray failed: {e}")
            raise

    def mask_forests(
        self, path_forest_data, tif_path=None, out_file=None, rxr_kwargs=None
    ):
        """Mask the raster by forest cover."""
        tif_masked = raster.mask_adefintg_forest(
            tif_path=tif_path,
            path_forest_data=path_forest_data,
            out_file=out_file,
            **rxr_kwargs,
        )
        return tif_masked

    def filter_by_confidence(
        self, tif_path=None, confidence_level=1, out_file=None, rxr_kwargs=None
    ):
        """
        Filtra el raster por nivel de confianza (1-4).

        Args:
            tif_path: Ruta al raster a filtrar. Si None, usa self.tif_path.
            confidence_level: Nivel de confianza (1-4).
            out_file: Ruta de salida opcional.
            rxr_kwargs: kwargs para rioxarray.

        Returns:
            Raster filtrado.
        """
        rxr_kwargs = rxr_kwargs or {}
        if not (1 <= confidence_level <= 4):
            raise ValueError("confidence_level debe estar entre 1 y 4")
        tif_filtered = raster.filter_adef_intg_conf(
            tif=tif_path,
            confidence=confidence_level,
            out_file=out_file,
            **rxr_kwargs,
        )
        return tif_filtered

    def filter_by_date_range(
        self, tif_path, filter_time, out_file=None, rxr_kwargs=None
    ):
        """
        Filtra el raster por rango de fechas o por la última fecha.
        """
        filtered = raster.filter_adef_intg_time(
            tif=tif_path,
            filter_time=filter_time,
            out_file=out_file,
            **rxr_kwargs,
        )
        return filtered

    def add_phid(self, phid_path, tif_path=None, out_file=None, rxr_kwargs=None):
        """Add PHID to the ADEFINTG raster."""
        #
        adef_with_phid = raster.tif_adef_to_phid(
            tif_phid=phid_path,
            tif_adef_masked=tif_path,
            out_file=out_file,
            **rxr_kwargs,
        )
        return adef_with_phid

    def to_vector(
        self, tif_path, out_folder, out_file=None, rxr_kwargs=None, gpd_kwargs=None
    ):
        """_summary_"""
        if out_file is None:
            vectorized = raster.tif_to_vector_gdal(
                tif=tif_path,
                out_folder=out_folder,
                rxr_kwargs=rxr_kwargs,
                gpd_kwargs=gpd_kwargs,
            )
            return vectorized

        vectorized = raster.tif_to_vector_gdal(
            tif=tif_path,
            out_folder=out_folder,
            out_file=out_file,
            rxr_kwargs=rxr_kwargs,
            gpd_kwargs=gpd_kwargs,
        )
        return vectorized
