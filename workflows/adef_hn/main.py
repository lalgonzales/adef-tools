"""Main entry point for the ADEF workflow."""

import yaml
from pathlib import Path
from adef_workflow import run_adef_workflow
from workflows.adef_hn.config import MainConfig

config_path = Path(__file__).parent / "config.yaml"

try:
    with open(config_path, encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)
    config = MainConfig.from_dict(config_dict)
except FileNotFoundError:
    config = MainConfig()


def main():
    run_adef_workflow(config)


if __name__ == "__main__":
    main()
