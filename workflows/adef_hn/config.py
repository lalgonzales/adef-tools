from pathlib import Path


class MainConfig:
    def __init__(
        self,
        debug=False,
        base_dir=None,
        data_dir=None,
        results_dir=None,
        vector_clip=None,
        use_lock=True,
        distributed_lock=False,
        steps=None,
        confidence=1,
        date_range=None,
        tif_adef_name="adef_intg.tif",
        tif_clip_name="adef_intg_clipped.tif",
        tif_masked_name="adef_intg_forest_masked.tif",
        tif_with_phid_name="adef_intg_with_phid.tif",
        tif_time_name="adef_intg_time.tif",
        tif_confidence_name="adef_intg_confidence.tif",
        vector_gfw=None,
    ):
        """_summary_

        Args:
            debug (bool, optional): _description_. Defaults to False.
            base_dir (_type_, optional): _description_. Defaults to None.
            data_dir (_type_, optional): _description_. Defaults to None.
            results_dir (_type_, optional): _description_. Defaults to None.
            vector_clip (_type_, optional): _description_. Defaults to None.
            use_lock (bool, optional): _description_. Defaults to True.
            distributed_lock (bool, optional): _description_. Defaults to False.
            steps (_type_, optional): _description_. Defaults to None.
            confidence (int, optional): _description_. Defaults to 1.
            date_range (_type_, optional): _description_. Defaults to None.
            tif_adef_name (str, optional): _description_. Defaults to "adef_intg.tif".
            tif_clip_name (str, optional): _description_. Defaults to "adef_intg_clipped.tif".
            tif_masked_name (str, optional): _description_. Defaults to "adef_intg_forest_masked.tif".
            tif_with_phid_name (str, optional): _description_. Defaults to "adef_intg_with_phid.tif".
            tif_time_name (str, optional): _description_. Defaults to "adef_intg_time.tif".
            tif_confidence_name (str, optional): _description_. Defaults to "adef_intg_confidence.tif".
        """
        self.base_dir = (
            Path(base_dir) if base_dir else Path(__file__).resolve().parent.parent
        )
        self.data_dir = Path(data_dir) if data_dir else self.base_dir / "data"
        self.results_dir = (
            Path(results_dir) if results_dir else self.base_dir / "results"
        )
        self.vector_clip = vector_clip
        self.use_lock = use_lock
        if distributed_lock:
            from dask.distributed import Client, LocalCluster

            cluster = LocalCluster()
            self.client = Client(cluster)
        else:
            self.client = None
        self.steps = steps or ["download", "clip", "mask", "confidence", "phid"]
        self.confidence = confidence
        self.date_range = date_range
        self.debug = debug
        self.tif_adef_name = tif_adef_name
        self.tif_clip_name = tif_clip_name
        self.tif_masked_name = tif_masked_name
        self.tif_with_phid_name = tif_with_phid_name
        self.tif_time_name = tif_time_name
        self.tif_confidence_name = tif_confidence_name
        self.vector_gfw = vector_gfw

    @classmethod
    def from_dict(cls, d):
        # Extraer y convertir debug si existe
        return cls(**d)
