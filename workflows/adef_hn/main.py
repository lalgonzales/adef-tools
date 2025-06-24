"""Main entry point for the ADEF workflow."""

import yaml
from pathlib import Path
import os
from dotenv import load_dotenv

from adef_workflow import prepare_adefgfw, table_updates_workflow
from workflows.adef_hn.config import MainConfig

load_dotenv()
config_path = Path(__file__).parent / "config.yaml"


def main(cfg):
    """
    Main entry point for the ADEF workflow. Runs the preparation and update workflows, printing timing information for each step and the total duration.

    Args:
        cfg (MainConfig): Configuration object for the workflow.
    """
    import time

    def format_hms(seconds):
        h, rem = divmod(seconds, 3600)
        m, s = divmod(rem, 60)
        return f"{int(h)}h {int(m)}m {s:.2f}s"

    t0 = time.time()
    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t0))
    print(f"ADEF workflow started. Start time: {start_time}")

    # Step 1: Preparation
    t_prepare0 = time.time()
    # prepare_info = prepare_adefgfw(cfg)
    t_prepare1 = time.time()
    elapsed_prepare = t_prepare1 - t_prepare0
    print(f"Step 1 (prepare_adefgfw) finished. Duration: {format_hms(elapsed_prepare)}")

    # Step 2: Table updates
    t_update0 = time.time()
    update_info = table_updates_workflow(cfg)
    t_update1 = time.time()
    elapsed_update = t_update1 - t_update0
    print(
        f"Step 2 (table_updates_workflow) finished. Duration: {format_hms(elapsed_update)}"
    )

    # Total duration
    t1 = time.time()
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t1))
    elapsed = t1 - t0
    print(f"ADEF workflow finished. End time: {end_time}")
    print(f"Total duration: {format_hms(elapsed)}")

    # Print summary of parameters and results
    print("\n--- Resumen de parámetros y resultados ---")
    print(f"Inicio del workflow: {start_time}")
    print(f"Duración preparación: {format_hms(elapsed_prepare)}")
    print(f"Duración updates: {format_hms(elapsed_update)}")
    # print(f"Confidence usado: {prepare_info.get('confidence_used')}")
    # print(f"Período utilizado: {prepare_info.get('date_range_used')}")
    # print(f"Vector generado: {prepare_info.get('vector_path')}")
    print(f"Geometrías iniciales: {update_info.get('n_geom_inicial')}")
    print(f"Nuevas geometrías: {update_info.get('n_geom_new')}")
    print(f"Insertados en PG: {update_info.get('n_insert_pg')}")
    print(f"Insertados en PG_HIST: {update_info.get('n_insert_pg_hist')}")
    print("-----------------------------------------\n")


if __name__ == "__main__":
    try:
        with open(config_path, encoding="utf-8") as f:
            config_dict = yaml.safe_load(f)
            config_dict["debug"] = os.getenv("DEBUG", "False").lower() == "true"
            config_dict["vector_gfw"] = os.getenv("VECTOR_GFW")
        config = MainConfig.from_dict(config_dict)
    except FileNotFoundError:
        config = MainConfig()
    main(config)
