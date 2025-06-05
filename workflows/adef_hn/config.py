from pathlib import Path


class MainConfig:
    def __init__(
        self,
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
    ):
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
            from dask.distributed import LocalCluster

            cluster = LocalCluster()
            self.client = cluster.get_client()
        else:
            self.client = None
        self.steps = steps or ["download", "clip", "mask", "confidence", "phid"]
        self.confidence = confidence
        self.date_range = date_range
        self.tif_adef_name = tif_adef_name
        self.tif_clip_name = tif_clip_name
        self.tif_masked_name = tif_masked_name
        self.tif_with_phid_name = tif_with_phid_name

    @classmethod
    def from_dict(cls, d):
        return cls(**d)
