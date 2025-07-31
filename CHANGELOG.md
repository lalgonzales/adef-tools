## v0.6.4 (2025-07-31)

### Fix

- build: ensure gdal and pre-commit integration for CI stability

  - Add system dependencies for gdal to fix build issues in CI.

  - Integrate and test pre-commit component in the pipeline.

  - Ensure compatibility for Python 3.13 and Alpine images.

  See merge request lalgonzales/adef-tools!28

## v0.6.3 (2025-07-19)

### Fix

- allow gdal>=3.10.0 for conda-forge compatibility

  See merge request alopez/adef-tools!25

## v0.6.2 (2025-07-10)

### Fix

- db_funcs: use find_dotenv to load .env from project root...

  fix(db_funcs): use find_dotenv to load .env from project root automatically

  This ensures environment variables are loaded from the project root regardless

  of execution directory, improving portability and reproducibility

  See merge request alopez/adef-tools!23

- db_funcs: use find_dotenv to load .env from project root automatically

  fix(db_funcs): use find_dotenv to load .env from project root automatically

  This ensures environment variables are loaded from the project root regardless

  of execution directory, improving portability and reproducibility

  See merge request alopez/adef-tools!23

## v0.6.1 (2025-06-24)

### Fix

- dependencies: remove pyproj from project dependencies

- dependencies: update dask-geopandas version

  - dask-geopandas constraint to >=0.5.0

  - add bump uv-pre-commit to v0.7.13

  See merge request alopez/adef-tools!17

### Refactor

- dependencies: remove unused build dependencies from pyproject.toml

## v0.6.0 (2025-06-24)

### Feat

- workflow: extend ADEF workflow with spatial joins, priority assignment and DB sync

  feat: extend ADEF workflow with spatial joins, alert priority logic and db write operations

  ### Changes included

  - Add `ADEFUPDATE` class with methods for:

  - Loading and merging current and historical alerts from PostgreSQL

  - Discarding duplicates and computing new alert geometries

  - Classifying updates (new, replace, overlap)

  - Assigning attributes from LIM, REM layers and POA fields

  - Inserting updated alerts into database

  - Add GDAL-based raster-to-vector and vector-to-raster tools

  - Modularize utilities and config handling (`validate_setting_tif`, `get_gdalwarp_path`, etc.)

  - Improve logic and structure in `main.py`, `adef_workflow.py`

  See merge request alopez/adef-tools!16

- extend ADEF workflow with spatial joins, alert priority logic and db write operations

  feat: extend ADEF workflow with spatial joins, alert priority logic and db write operations

  ### Changes included

  - Add `ADEFUPDATE` class with methods for:

  - Loading and merging current and historical alerts from PostgreSQL

  - Discarding duplicates and computing new alert geometries

  - Classifying updates (new, replace, overlap)

  - Assigning attributes from LIM, REM layers and POA fields

  - Inserting updated alerts into database

  - Add GDAL-based raster-to-vector and vector-to-raster tools

  - Modularize utilities and config handling (`validate_setting_tif`, `get_gdalwarp_path`, etc.)

  - Improve logic and structure in `main.py`, `adef_workflow.py`

  See merge request alopez/adef-tools!16

## v0.5.0 (2025-06-13)

### Feat

- adef_hn: first step for workflows to process Integrated Alerts

  - add main workflow script (adef_workflow.py)

  - implement configuration class (config.py)

  - add main entry point (main.py)

  See merge request alopez/adef-tools!13

- build: migrate to cz-changeup for commitizen

  - migrate from cz_conventional_commits to cz-changeup

  - update pyproject.toml, requirements.txt, and uv.lock files

- add ADEFINTG class and refactor functions

  - adef_fn: add ADEFINTG class with methods for download, clip, mask, filter, and phid

  Implemented the ADEFINTG class in adef_fn.py, including methods for downloading alerts data,

  clipping rasters, masking by forest cover, filtering by confidence and date range, and adding PHID.

  This provides a high-level interface for common alert raster processing workflows.

  - utils: modularize utils by moving raster and vector functions into dedicated modules

  - Moved raster-related functions from utils_adef.py to raster.py

  - Moved vector-related functions from utils_adef.py to vector.py

  - Adapted internal logic and imports to reflect the modular split

  - Reduced utils_adef.py to shared functions and WFS utilities only

  - workflows: move data files to workflows/data and remove unused notebook

  Data files bosque*_lzw.tif and phid_hn.tif were moved from data/ to workflows/data/

  to better organize workflow-specific resources. The old notebook adef_intg.ipynb was

  removed as it is no longer needed.

  See merge request alopez/adef-tools!12

- cli: add vector input, improve modularity and CI for ADEF integration

- update .gitignore to include additional file patterns and directories

- add utility functions for raster data processing and date calculations

- refactor and organize code structure in adef_intg notebook

### Fix

- filter_adef_intg_conf: Ensure output directory exists before writing raste

  See merge request alopez/adef-integ-tools!9

- force wcwidth==0.2.13

  cz bump forced the lib to 0.2.23 breaking the installation of rasterio

  See merge request alopez/adef-integ-tools!7

- update function call to handle additional parameter in run_adef_process

- update function call to handle additional parameter in run_adef_process

### Refactor

- raster: clean up imports and improve error handling

  ### Refactor

  - raster: clean up imports and improve error handling

  - 【Imports】: Removed unnecessary imports like `threading` and `tqdm`.

  - 【Error Handling】: Improved error handling in `dw_tif` to catch specific

  request exceptions and raise a `RuntimeError` after retries.

  - 【Code Optimization】: Aligned bounding box to raster grid in

  `clip_tif_ext_gdal` to ensure proper clipping.

  ### Perf

  - adef_hn: integrate config file loading

  - load configurations from a yaml file to allow for customizable workflow settings

  - handle cases where the configuration file is missing by using default values

  See merge request alopez/adef-tools!14

- utils: run gdal_polygonize.py with python

  - Execute gdal_polygonize.py using python for compatibility.

  - Add exception handling for subprocess errors.

- utils_adef.py: use subprocess.run instead of os.system for GDAL...

  refactor(utils_adef.py): use subprocess.run instead of os.system for GDAL commands and fix TIF attribute checks

  - Replace os.system with subprocess.run for executing GDAL-related commands, improving error handling and security.

  - Fix TIF attribute comparisons to use tif_reference instead of tif_to_adjusd.

  - Add better exception handling for WFS connection and GDAL operations.

  - Minor code cleanups and improved robustness in raster/vector processing utilities.

  See merge request alopez/adef-integ-tools!11

- utils_adef.py: use subprocess.run instead of os.system for GDAL commands and fix TIF attribute checks

  refactor(utils_adef.py): use subprocess.run instead of os.system for GDAL commands and fix TIF attribute checks

  - Replace os.system with subprocess.run for executing GDAL-related commands, improving error handling and security.

  - Fix TIF attribute comparisons to use tif_reference instead of tif_to_adjusd.

  - Add better exception handling for WFS connection and GDAL operations.

  - Minor code cleanups and improved robustness in raster/vector processing utilities.

  See merge request alopez/adef-integ-tools!11

- remove unused imports from adef_intg_fn.py

  See merge request alopez/adef-integ-tools!6

- cli: derive base_dir from out_folder instead of __file__

  This change replaces the hardcoded relative path with a path

  based on the resolved output folder, improving flexibility

  and making the tool more robust in different environments.

  See merge request alopez/adef-integ-tools!5

## v0.4.0 (2025-06-05)

### Feat

- adef_hn: first step for workflows to process Integrated Alerts

  - add main workflow script (adef_workflow.py)

  - implement configuration class (config.py)

  - add main entry point (main.py)

  See merge request alopez/adef-tools!13

## v0.3.0 (2025-06-05)

### Feat

- build: migrate to cz-changeup for commitizen

  - migrate from cz_conventional_commits to cz-changeup

  - update pyproject.toml, requirements.txt, and uv.lock files

- add ADEFINTG class and refactor functions

  - adef_fn: add ADEFINTG class with methods for download, clip, mask, filter, and phid

  Implemented the ADEFINTG class in adef_fn.py, including methods for downloading alerts data,

  clipping rasters, masking by forest cover, filtering by confidence and date range, and adding PHID.

  This provides a high-level interface for common alert raster processing workflows.

  - utils: modularize utils by moving raster and vector functions into dedicated modules

  - Moved raster-related functions from utils_adef.py to raster.py

  - Moved vector-related functions from utils_adef.py to vector.py

  - Adapted internal logic and imports to reflect the modular split

  - Reduced utils_adef.py to shared functions and WFS utilities only

  - workflows: move data files to workflows/data and remove unused notebook

  Data files bosque*_lzw.tif and phid_hn.tif were moved from data/ to workflows/data/

  to better organize workflow-specific resources. The old notebook adef_intg.ipynb was

  removed as it is no longer needed.

  See merge request alopez/adef-tools!12

### Fix

- filter_adef_intg_conf: Ensure output directory exists before writing raste

  See merge request alopez/adef-integ-tools!9

- force wcwidth==0.2.13

  cz bump forced the lib to 0.2.23 breaking the installation of rasterio

  See merge request alopez/adef-integ-tools!7

- update function call to handle additional parameter in run_adef_process

- update function call to handle additional parameter in run_adef_process

### Refactor

- utils: run gdal_polygonize.py with python

  - Execute gdal_polygonize.py using python for compatibility.

  - Add exception handling for subprocess errors.

- utils_adef.py: use subprocess.run instead of os.system for GDAL...

  refactor(utils_adef.py): use subprocess.run instead of os.system for GDAL commands and fix TIF attribute checks

  - Replace os.system with subprocess.run for executing GDAL-related commands, improving error handling and security.

  - Fix TIF attribute comparisons to use tif_reference instead of tif_to_adjusd.

  - Add better exception handling for WFS connection and GDAL operations.

  - Minor code cleanups and improved robustness in raster/vector processing utilities.

  See merge request alopez/adef-integ-tools!11

- utils_adef.py: use subprocess.run instead of os.system for GDAL commands and fix TIF attribute checks

  refactor(utils_adef.py): use subprocess.run instead of os.system for GDAL commands and fix TIF attribute checks

  - Replace os.system with subprocess.run for executing GDAL-related commands, improving error handling and security.

  - Fix TIF attribute comparisons to use tif_reference instead of tif_to_adjusd.

  - Add better exception handling for WFS connection and GDAL operations.

  - Minor code cleanups and improved robustness in raster/vector processing utilities.

  See merge request alopez/adef-integ-tools!11

- remove unused imports from adef_intg_fn.py

  See merge request alopez/adef-integ-tools!6

- cli: derive base_dir from out_folder instead of __file__

  This change replaces the hardcoded relative path with a path

  based on the resolved output folder, improving flexibility

  and making the tool more robust in different environments.

  See merge request alopez/adef-integ-tools!5

## v0.2.6 (2025-05-19)

### Refactor

- **utils**: run gdal_polygonize.py with python

## v0.2.5 (2025-05-19)

### Refactor

- **utils_adef.py**: use subprocess.run instead of os.system for GDAL...
- **utils_adef.py**: use subprocess.run instead of os.system for GDAL commands and fix TIF attribute checks

## v0.2.4 (2025-05-18)

### Fix

- **filter_adef_intg_conf**: Ensure output directory exists before writing raste
- force wcwidth==0.2.13

## v0.2.3 (2025-05-17)

### Fix

- force wcwidth==0.2.13

## v0.2.2 (2025-05-17)

### Refactor

- remove unused imports from adef_intg_fn.py
- **cli**: derive base_dir from out_folder instead of __file__

## v0.2.1 (2025-05-16)

### Fix

- update function call to handle additional parameter in run_adef_process

## v0.2.0 (2025-05-16)

### Feat

- **cli**: add vector input, improve modularity and CI for ADEF integration

### Fix

- update function call to handle additional parameter in run_adef_process

## v0.1.1 (2025-05-12)

## v0.1.0 (2025-05-08)

### BREAKING CHANGE

- Ensure to review the updated dependencies and configurations.

### Feat

- update .gitignore to include additional file patterns and directories
- add utility functions for raster data processing and date calculations
- refactor and organize code structure in adef_intg notebook
- add new raster data files for analysis
- update dependencies and improve project structure
