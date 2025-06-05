from adef_workflow import run_adef_workflow
from config import MainConfig


def main():
    config = MainConfig()
    run_adef_workflow(config)


if __name__ == "__main__":
    main()
