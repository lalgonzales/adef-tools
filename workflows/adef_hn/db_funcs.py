"""
Table update utilities for ADEF.

This module provides helper functions and the ADEFUPDATE class for managing
ADEF (Deforestation Alerts) table updates. It includes:

- Helper functions for workflow-specific attribute processing and aggregation.
- The ADEFUPDATE class, which loads database engines, prepares and merges main
  tables, performs spatial operations, and updates both main and historical
  tables.

All docstrings and comments are in English and formatted for Pylint compliance.
"""

import os
import time
from functools import cached_property
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.orm import sessionmaker
import pandas as pd
import geopandas as gpd
import dask_geopandas as dgpd
import dask.dataframe as dd
import numpy as np
from dotenv import load_dotenv, find_dotenv
from memory_profiler import profile
from adef_tools import vector
from adef_tools.utils import calculate_decompose_date, load_yaml, build_engine

# Load environment variables from .env file
load_dotenv(find_dotenv(), override=True)


# --- Workflow-specific helper functions ---
def poa_cases(gdf):
    """
    Add a column 'is_poa_vig' to indicate if the POA is valid for each row.

    Args:
        gdf (gpd.GeoDataFrame): Input GeoDataFrame with POA date columns.

    Returns:
        gpd.GeoDataFrame: The same GeoDataFrame with the 'is_poa_vig' column added.
    """
    try:
        date_cols = ["fecha", "fech_apro_poa", "fech_venci_poa", "fech_prorr_poa"]
        for col in date_cols:
            if col in gdf.columns:
                gdf[col] = pd.to_datetime(gdf[col], errors="coerce")
        gdf["is_poa_vig"] = gdf.apply(
            lambda x: (
                "Si"
                if (
                    x.get("fech_apro_poa", pd.NaT) is not pd.NaT
                    and x.get("fecha", pd.NaT) >= x.get("fech_apro_poa", pd.NaT)
                    and (
                        (
                            x.get("fech_venci_poa", pd.NaT) is not pd.NaT
                            and x.get("fecha", pd.NaT)
                            <= x.get("fech_venci_poa", pd.NaT)
                        )
                        or (
                            x.get("fech_prorr_poa", pd.NaT) is not pd.NaT
                            and x.get("fecha", pd.NaT)
                            <= x.get("fech_prorr_poa", pd.NaT)
                        )
                    )
                )
                else "No" if pd.notnull(x.get("fech_apro_poa", None)) else ""
            ),
            axis=1,
        )
        return gdf
    except Exception as e:
        print(f"[poa_cases] Error processing POA cases: {e}")
        raise


def aggregate_data(gdf, dict_data_rem_lim):
    """
    Aggregate the input GeoDataFrame by 'noAlerta', combining attributes from related tables.
    For columns present in related tables, concatenate unique, sorted values as comma-separated strings.
    For other columns, take the first value per group.

    Args:
        gdf (gpd.GeoDataFrame): Input GeoDataFrame to aggregate.
        dict_data_rem_lim (dict): Dictionary of related GeoDataFrames for attribute aggregation.

    Returns:
        gpd.GeoDataFrame: Aggregated GeoDataFrame grouped by 'noAlerta'.
    """

    def concat_sorted_unique(series):
        # Remove NaN, convert to string, remove duplicates, sort, and join
        return ", ".join(sorted(set(series.dropna().astype(str))))

    try:
        columns = []
        for _, gdf_in_dic in dict_data_rem_lim.items():
            geom_col = gdf_in_dic.geometry.name
            for col in gdf_in_dic.columns:
                if col != geom_col:
                    columns.append(col)
        agg_dict = {
            col: "first"
            for col in gdf.columns
            if col not in columns and col != "noAlerta"
        }
        agg_dict.update({col: concat_sorted_unique for col in columns})
        df_agg = gdf.groupby("noAlerta").agg(agg_dict).reset_index()
        gdf_agg = gpd.GeoDataFrame(df_agg, geometry=gdf.geometry.name, crs=gdf.crs)
        return gdf_agg
    except Exception as e:
        print(f"[aggregate_data] Error aggregating data: {e}")
        raise


def adef_priority(gdf, keys_tables_rem_pma, keys_tables_rem_pa, dict_rem_lim):
    """
    Assign a priority level to each alert based on PMA/PA columns and hydric protection.

    Args:
        gdf (gpd.GeoDataFrame): Input GeoDataFrame with columns for PMA, PA, and 'prot_hid'.
        keys_tables_rem_pma (list): List of PMA table keys.
        keys_tables_rem_pa (list): List of PA table keys.
        dict_rem_lim (dict): Dictionary of related GeoDataFrames for attribute lookup.

    Returns:
        gpd.GeoDataFrame: The same GeoDataFrame with a new 'prioridad' column.
    """
    try:
        columns_pma = [
            col
            for key in keys_tables_rem_pma
            for col in dict_rem_lim[key].columns
            if col != dict_rem_lim[key].geometry.name
        ]
        columns_pa = [
            col
            for key in keys_tables_rem_pa
            for col in dict_rem_lim[key].columns
            if col != dict_rem_lim[key].geometry.name
        ]
        condition_pma = (
            gdf[columns_pma]
            .apply(
                lambda x: x.notna()
                & (~x.astype(str).eq(""))
                & (~x.eq(pd.Timestamp(0))),
                axis=1,
            )
            .any(axis=1)
        )
        condition_pa = (
            gdf[columns_pa]
            .apply(
                lambda x: x.notna()
                & (~x.astype(str).eq(""))
                & (~x.eq(pd.Timestamp(0))),
                axis=1,
            )
            .any(axis=1)
        )
        gdf["prioridad"] = np.where(
            condition_pma | (gdf["prot_hid"] == "Si"),
            "Muy Alta",
            np.where(condition_pa, "Alta", "Media"),
        )
        return gdf
    except Exception as e:
        print(f"[adef_priority] Error assigning priority: {e}")
        raise


def add_ubication(gdf):
    """
    Add a 'ubicacion' column to the GeoDataFrame with the representative point coordinates (lat, lon) as a string.

    Args:
        gdf (gpd.GeoDataFrame): Input GeoDataFrame.

    Returns:
        gpd.GeoDataFrame: The same GeoDataFrame with the 'ubicacion' column added.
    """
    try:
        gdf["ubicacion"] = gdf.geometry.representative_point().apply(
            lambda geom: f"{geom.y}, {geom.x}"
        )
        return gdf
    except Exception as e:
        print(f"[add_ubication] Error adding ubication: {e}")
        raise


class ADEFUPDATE:
    """
    Class to handle ADEF table updates.

    This class manages the update process for the ADEF (Deforestation Alerts)
    tables, including loading database engines and table mappings from a YAML
    configuration file, preparing and merging main tables, performing spatial
    operations, and updating both the main and historical tables. It provides
    methods for:

    - Loading and configuring database engines and table mappings.
    - Loading main and historical alert tables as GeoDataFrames.
    - Preparing merged DataFrames for analysis and assignment status.
    - Performing spatial joins and overlays to identify new, updated, or
      intersecting geometries.
    - Assigning new IDs and codes to alerts, and updating records in the
      database.
    - Adding required attributes to new alerts before insertion.
    - Executing SQL scripts for advanced or batch operations.

    All methods are documented for clarity and maintainability. Heavy properties
    are cached for performance.
    """

    def __init__(self, db_mapping_file=None):
        """
        Initialize the ADEFUPDATE class.

        Args:
            yaml_file (str or Path, optional): Path to the YAML config file.
                If None, uses 'db_mapping.yaml' in the same directory as this script.
            auto_load (bool, optional): If True, automatically loads DB engines and main tables.
        """
        self.db_engines = {}
        self.db_config = {}
        self._load_db_engines(db_mapping_file)

    # --- Properties ---
    @property
    def adefpg(self):
        """
        GeoDataFrame of the main deforestation alerts table (adefpg).

        Returns:
            gpd.GeoDataFrame: Main alerts table as a GeoDataFrame.
        """
        engine_main = self.get_engine("db_main_readonly")
        schema_main = self.db_config["db_main"]["schema"]
        table_adefpg = self.get_table_name("db_main", "adefpg")
        sql = f"SELECT * FROM {schema_main}.{table_adefpg}"
        return vector.load_table_as_gdf_or_df(
            sql=sql,
            engine=engine_main,
        )

    @property
    def adefpg_hist(self):
        """
        GeoDataFrame of the historical deforestation alerts table (adefpg_hist).

        Returns:
            gpd.GeoDataFrame: Historical alerts table as a GeoDataFrame.
        """
        engine_main = self.get_engine("db_main_readonly")
        schema_main = self.db_config["db_main"]["schema"]
        table_adefpg_hist = self.get_table_name("db_main", "adefpg_hist")
        sql = f"SELECT * FROM {schema_main}.{table_adefpg_hist}"
        return vector.load_table_as_gdf_or_df(
            sql=sql,
            engine=engine_main,
        )

    @property
    def adefpg_asig(self):
        """
        DataFrame of the assigned deforestation alerts table (adef_asig).

        Returns:
            pd.DataFrame: Assigned alerts table as a DataFrame (not GeoDataFrame).
        """
        engine_app = self.get_engine("db_app")
        schema_app = self.db_config["db_app"]["schema"]
        table_adefpg_asig = self.get_table_name("db_app", "adef_asig")
        sql = f"SELECT * FROM {schema_app}.{table_adefpg_asig}"
        return vector.load_table_as_gdf_or_df(
            sql=sql,
            engine=engine_app,
            is_geo=False,
        )

    @property
    def adefpg_merged(self):
        """
        GeoDataFrame merging main alerts and assignment status.

        Returns:
            gpd.GeoDataFrame: Merged alerts and assignment status.
        """
        engine_main = self.get_engine("db_main_readonly")
        schema_main = self.db_config["db_main"]["schema"]
        table_adefpg = self.get_table_name("db_main", "adefpg")
        sql = f"""
            SELECT "geom", "id", "cod", "area_ha", "anio", "fecha"
            FROM {schema_main}.{table_adefpg}
        """

        adefpg = vector.load_table_as_gdf_or_df(
            sql=sql,
            engine=engine_main,
        )
        adefpg_asig = self.adefpg_asig.rename(columns={"id_alerta_deforestacion": "id"})
        adefpg_asig["assigned"] = "Si"
        adefpg_asig = adefpg_asig[["id", "assigned"]].copy()
        adefpg_merged = pd.merge(adefpg, adefpg_asig, on="id", how="left")
        return adefpg_merged

    @property
    def adefpg_hist_less_cols(self):
        """
        GeoDataFrame of the historical deforestation alerts table (adefpg_hist) with fewer columns.

        Returns:
            gpd.GeoDataFrame: Historical alerts table with selected columns as a GeoDataFrame.
        """
        engine_main = self.get_engine("db_main")
        schema_main = self.db_config["db_main"]["schema"]
        table_adefpg_hist = self.get_table_name("db_main", "adefpg_hist")

        sql = f"""
            SELECT "geom", "id", "cod", "cod_old"
            FROM {schema_main}.{table_adefpg_hist}
        """
        return vector.load_table_as_gdf_or_df(
            sql=sql,
            engine=engine_main,
            is_geo=True,
        )

    @property
    def _keys_tables_lim(self):
        """
        List of logical keys for 'lim' tables from the config.

        Returns:
            list: Keys for 'lim' tables.
        """
        return self.db_config.get("db_main", {}).get("keys_tables_lim", [])

    @property
    def _keys_tables_rem_pma(self):
        """
        List of logical keys for PMA tables from the config.

        Returns:
            list: Keys for PMA tables.
        """
        return self.db_config.get("db_main", {}).get("keys_tables_rem_pma", [])

    @property
    def _keys_tables_rem_pa(self):
        """
        List of logical keys for PA tables from the config.

        Returns:
            list: Keys for PA tables.
        """
        return self.db_config.get("db_main", {}).get("keys_tables_rem_pa", [])

    @cached_property
    def dict_rem_lim(self):
        """
        Dictionary of GeoDataFrames for all 'lim', PMA, and PA tables, with columns renamed and filtered.

        Returns:
            dict: Dictionary mapping table keys to GeoDataFrames.
        """
        dict_rem = {}
        keys_tables_lim = self._keys_tables_lim
        keys_tables_rem_pma = self._keys_tables_rem_pma
        keys_tables_rem_pa = self._keys_tables_rem_pa
        keys_tables = keys_tables_lim + keys_tables_rem_pma + keys_tables_rem_pa
        engine_main = self.get_engine("db_main")
        schema_main = self.db_config["db_main"]["schema"]
        for key in keys_tables:
            try:
                # Define the interesting columns based on renames
                renames = self.db_config["db_main"]["renames"].get(key, {})
                cols_to_keep = list(renames.keys())
                cols_to_keep.append("geom")

                # Get the data from the database
                table = self.get_table_name("db_main", key)

                # SQL query to load the table
                sql = f"""
                    SELECT {', '.join(cols_to_keep)}
                    FROM {schema_main}.{table}
                """

                # Filter Declared Protected Areas (PAs) if applicable
                if key == "ap":
                    # SQL query personalized for Declared PAs
                    sql = f"""
                        SELECT {', '.join(cols_to_keep)}
                        FROM {schema_main}.{table}
                        WHERE "estado" = 'DECLARADA'
                    """
                gdf = vector.load_table_as_gdf_or_df(sql=sql, engine=engine_main)

                # Check if the GeoDataFrame is valid
                if hasattr(gdf, "crs") and (
                    gdf.crs is None or gdf.crs.to_epsg() != 4326
                ):
                    gdf = gdf.to_crs("EPSG:4326")

                # Ensure the geometry column is set correctly
                gdf = gdf[cols_to_keep].copy()

                # Rename columns based on the configuration
                gdf.rename(columns=renames, inplace=True)
                dict_rem[key] = gdf
            except (KeyError, AttributeError, TypeError, ValueError) as e:
                print(f"[dict_rem_lim] Error processing table '{key}': {e}")
        return dict_rem

    def _load_db_engines(self, yaml_path=None) -> None:
        """
        Load all database engines and configuration from a YAML file using logical aliases.

        Args:
            yaml_path (str or Path, optional): Path to the YAML config file.
                If None, uses the instance's yaml_file attribute.

        Side Effects:
            Populates self.db_engines and self.db_config with loaded engines and config.
            Prints errors if any engine fails to load.
        """
        try:
            if yaml_path is None:
                path_module = os.path.dirname(os.path.abspath(__file__))
                yaml_path = os.path.join(path_module, "db_mapping.yaml")

            config = load_yaml(yaml_path)
            self.db_config = config
            for alias, db_conf in config.items():
                try:
                    engine = build_engine(
                        db_conf,
                        pool_pre_ping=True,
                        pool_size=20,
                        max_overflow=20,
                        pool_timeout=30,
                    )
                    if engine is not None:
                        self.db_engines[alias] = engine
                    else:
                        print(
                            f"[_load_db_engines] Engine for '{alias}' could not be created."
                        )
                except (
                    OSError,
                    ValueError,
                    TypeError,
                    AttributeError,
                    ImportError,
                ) as e:
                    print(f"[_load_db_engines] Error loading engine for '{alias}': {e}")
        except (OSError, ValueError, TypeError, AttributeError, ImportError) as e:
            print(f"[_load_db_engines] Error loading DB engines from YAML: {e}")

    def get_engine(self, alias: str):
        """
        Retrieve a database engine by its logical alias.

        Args:
            alias (str): Logical alias for the database.

        Returns:
            sqlalchemy.engine.Engine or None: The engine if found, else None.
        """
        try:
            engine = self.db_engines.get(alias)
            if engine is None:
                print(f"[get_engine] No engine found for alias '{alias}'.")
            return engine
        except (SQLAlchemyError, OperationalError, AttributeError, KeyError) as e:
            print(f"[get_engine] Error retrieving engine for alias '{alias}': {e}")
            return None

    def get_table_name(self, db_alias: str, table_alias: str) -> str:
        """
        Retrieve the actual table name from a database and table alias.

        Args:
            db_alias (str): Logical alias for the database (e.g., 'db_main').
            table_alias (str): Logical alias for the table (e.g., 'adefpg').

        Returns:
            str or None: The actual table name, or None if not found.
        """
        try:
            db_conf = self.db_config.get(db_alias, {})
            tables = db_conf.get("tables", {})
            table_name = tables.get(table_alias)
            if table_name is None:
                print(
                    f"[get_table_name] Table alias '{table_alias}' not found in database '{db_alias}'."
                )
            return table_name
        except (KeyError, AttributeError, TypeError, ValueError) as e:
            print(
                f"[get_table_name] Error retrieving table name for '{db_alias}.{table_alias}': {e}"
            )
            return None

    def get_ids_seq(self, seq_alias: str, no_geoms: int, retries: int = 3) -> list:
        """
        Retrieve a sequence of IDs from a specified table sequence in the database.

        Args:
            seq_alias (str): Logical alias for the sequence table in the config.
            no_geoms (int): Number of IDs to retrieve from the sequence.
            retries (int, optional): Number of retry attempts in case of failure (default is 3).

        Returns:
            list: List of IDs retrieved from the sequence.

        Raises:
            RuntimeError: If unable to retrieve IDs after all retries.
        """
        engine_main = self.get_engine("db_main")
        schema_main = self.db_config["db_main"]["schema"]
        seq_main_table = self.get_table_name("db_main", seq_alias)

        query = text(
            f"""
            SELECT nextval('{schema_main}.{seq_main_table}')
            FROM generate_series(1, {no_geoms});
            """
        )

        Session = sessionmaker(bind=engine_main)

        for attempt in range(retries):
            session = Session()
            connection = session.connection()
            try:
                result = connection.execute(query)
                ids = [row[0] for row in result]
                print(f"...Obtained {len(ids)} values from sequence `{seq_alias}`.")
                return ids
            except (SQLAlchemyError, OperationalError) as e:
                session.rollback()
                print(f"Attempt {attempt + 1} of {retries} failed: {e}")
                time.sleep(5)
            finally:
                session.close()
        # If all attempts fail, stop the script
        raise RuntimeError(
            f"Could not obtain IDs from sequence `{seq_alias}` after {retries} attempts."
        )

    @profile
    def discard_data_in_db(
        self, data_gfw: gpd.GeoDataFrame, out_file=None, **gpd_kwargs
    ) -> gpd.GeoDataFrame:
        """
        Remove geometries from data_gfw that are already present in the database (adefpg_merged).

        Args:
            data_gfw (gpd.GeoDataFrame): New geometries to check against the database.

        Returns:
            gpd.GeoDataFrame: Only the geometries from data_gfw that are NOT already present in the database.
        """
        try:
            # Buffer to avoid topology errors
            data_gfw_buff = data_gfw.copy()
            data_gfw_buff.geometry = data_gfw_buff.buffer(-0.000000001)

            data_pg = self.adefpg_merged.copy()

            # Exclude gfw data that is already in pg
            data_gfw_in_pg = gpd.sjoin(
                data_gfw_buff, data_pg, how="inner", predicate="within"
            )
            data_gfw_new = data_gfw.loc[
                ~data_gfw.index.isin(data_gfw_in_pg.index)
            ].copy()
            data_gfw_new["gcode"] = data_gfw_new["value"] % 10000

            if out_file:
                data_gfw_new = vector.save_to_file(data_gfw_new, out_file, **gpd_kwargs)
            return data_gfw_new
        except (KeyError, AttributeError, TypeError, ValueError) as e:
            print(f"[discard_data_in_db] Error excluding existing geometries: {e}")
            return gpd.GeoDataFrame(columns=data_gfw.columns)

    @profile
    def geom_interes(
        self, data_gfw: gpd.GeoDataFrame, out_file=None, **gpd_kwargs
    ) -> gpd.GeoDataFrame:
        """
        Get the intersection geometries between the database and the new geometries.

        Args:
            data_gfw (gpd.GeoDataFrame): New geometries to intersect with the database.

        Returns:
            gpd.GeoDataFrame: Geometries resulting from the intersection.
        """
        try:
            data_gfw = data_gfw[[data_gfw.geometry.name]].copy()
            data_pg = self.adefpg_merged
            data_pg = data_pg[[data_pg.geometry.name, "id"]].copy()

            # Ensure the geometry column is set correctly
            if data_gfw.geometry.name != data_pg.geometry.name:
                data_gfw = data_gfw.rename(
                    columns={data_gfw.geometry.name: data_pg.geometry.name}
                )
                data_gfw.set_geometry(data_pg.geometry.name, inplace=True)

            geom_pg_interes = gpd.sjoin_nearest(
                data_gfw,
                data_pg,
                how="left",
                max_distance=0.000000001,
                distance_col="distance",
            )
            data_pg_ids = geom_pg_interes["id"].unique()
            data_pg_interes = data_pg.loc[data_pg["id"].isin(data_pg_ids)][
                [data_pg.geometry.name]
            ].copy()
            union_geom = pd.concat([data_gfw, data_pg_interes], ignore_index=True)
            print(
                "--------------------",
                "Total geometries resulting from the union of data_gfw and data_pg_interes:",
                len(union_geom),
                "--------------------",
                sep="\n",
            )

            # # If the length of the union geometry exceeds 500,000: logic in testing
            # if len(union_geom) > 500_000:
            #     final = self.geom_interes_dask(union_geom)
            #     return final

            # Dissolve and extract unique geometries
            union_geom_buff = union_geom.copy()
            union_geom_buff.geometry = union_geom_buff.buffer(0.00000001)

            geom_dissolved = union_geom_buff.dissolve()
            geom_exploded = geom_dissolved.explode(ignore_index=True)

            # Reset index
            unique_geom = geom_exploded.reset_index()
            unique_geom = unique_geom.rename(columns={"index": "noAlerta"})

            final = union_geom.sjoin(unique_geom)
            final = final.dissolve(by="noAlerta", as_index=True).reset_index()
            final = vector.calculate_area_ha(gdf_gcs=final, field_area_name="area_ha")

            final = final[final["area_ha"] > 0.5].copy()
            if "index_right" in final.columns:
                final.drop(columns=["index_right"], inplace=True)
            if out_file:
                final = vector.save_to_file(final, out_file, **gpd_kwargs)
            return final
        except (KeyError, AttributeError, TypeError, ValueError) as e:
            print(f"[geom_interes] Error computing intersection: {e}")
            raise

    def geom_interes_dask(self, gdf_interes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Dissolve and extract unique geometries using Dask for large datasets.

        Args:
            gdf_interes (gpd.GeoDataFrame): Input GeoDataFrame with geometries of interest.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame with dissolved and exploded unique geometries.

        Note:
            This function is intended for use in tests or when the number of geometries is very large.
        """
        # Get the geometry column name
        geom_col = gdf_interes.geometry.name
        union_geom_buff = dgpd.from_geopandas(gdf_interes, npartitions=200)

        if geom_col != "geometry":
            union_geom_buff = union_geom_buff.rename(columns={geom_col: "geometry"})
            union_geom_buff = union_geom_buff.set_geometry("geometry")

        try:
            union_geom_buff.geometry = union_geom_buff.buffer(0.00000001)

            geom_dissolved = union_geom_buff.dissolve()
            geom_exploded = geom_dissolved.explode(ignore_index=True)

            # Reset index
            unique_geom = geom_exploded.reset_index()
            unique_geom = unique_geom.rename(columns={"index": "noAlerta"})

            # unique_geom = unique_geom.spatial_shuffle(npartitions=200).persist()
            # unique_geom.spatial_partitions = dgpd.calculate_spatial_partitions(unique_geom)

            joined = unique_geom.sjoin(gdf_interes)
            joined = joined.to_dask_dataframe()
            final = dd.merge(
                gdf_interes,
                joined,
                how="left",
                left_on=gdf_interes.index,
                right_on=joined.index_right,
            )
            final = final.dissolve(by="noAlerta").reset_index()
            final = vector.calculate_area_ha(gdf_gcs=final, field_area_name="area_ha")

            final = final[final["area_ha"] > 0.5].copy()
            if "index_right" in final.columns:
                final.drop(columns=["index_right"], inplace=True)
            if final.geometry.name != geom_col:
                final = final.rename(columns={final.geometry.name: geom_col})
                final = final.set_geometry(geom_col)
            return final.compute() if hasattr(final, "compute") else final
        except (KeyError, AttributeError, TypeError, ValueError) as e:
            print(f"[geom_interes_dask] Error computing intersection with Dask: {e}")
            raise

    @profile
    def case_update(
        self,
        new_geoms: gpd.GeoDataFrame,
        data_gfw: gpd.GeoDataFrame,
        out_file=None,
        **gpd_kwargs,
    ):
        """
        Identify and classify update cases between new geometries and existing alerts.

        This method compares new alert geometries with the current alerts in the database,
        performs spatial and attribute joins, and applies decision logic to classify each
        case as 'Nueva', 'Mantener', 'Reemplazar_add', or 'Traslape_reemplazar'. It also
        computes relevant metrics and returns the update table and IDs to be replaced.

        Args:
            new_geoms (gpd.GeoDataFrame): GeoDataFrame with new alert geometries to analyze.
            data_gfw (gpd.GeoDataFrame): GeoDataFrame with original GFW alert data.
            out_file (str, optional): Path to save the resulting update table (default: None).
            **gpd_kwargs: Additional keyword arguments for saving the file.

        Returns:
            Tuple[gpd.GeoDataFrame, np.ndarray]:
                - GeoDataFrame with update classification and metrics.
                - Array of IDs to be replaced in the main table.
        """

        print("---Identifying update cases...")
        gdf_adefpg = self.adefpg_merged.copy()

        try:
            # Prepare PG alerts
            gdf_adefpg["geom_adefpg"] = gdf_adefpg.geometry
            gdf_adefpg.geometry = gdf_adefpg.geometry.to_crs("EPSG:3857")
            gdf_adefpg.geometry = gdf_adefpg.geometry.buffer(-0.1).to_crs("EPSG:4326")
            zero_day = pd.Timestamp("2014-12-31")
            gdf_adefpg["days_pg"] = gdf_adefpg["fecha"].apply(
                lambda x: (pd.Timestamp(x) - zero_day).days
            )

            # Join attributes for analysis
            gdf_update1 = new_geoms.sjoin(gdf_adefpg, how="left")
            gdf_update1.rename(
                columns={"area_ha_left": "alert_area", "area_ha_right": "adefpg_area"},
                inplace=True,
            )
            gdf_update1.drop(columns=["index_right"], inplace=True)
            print(
                f"{gdf_update1.shape[0]} records created in the join of prepared GLAD alerts with PG alerts"
            )
        except (KeyError, AttributeError, TypeError, ValueError) as e:
            print(f"Error joining attributes of combined alerts with PG alerts: {e}")
            raise

        # Join attributes with merged PG alerts to get date and hydric protection
        print("---Getting gcode and prot_hid for alerts to be incorporated...")
        try:
            gdf_adef_hn = data_gfw.copy()
            gdf_adef_hn.geometry = gdf_adef_hn.geometry.to_crs("EPSG:3857")
            gdf_adef_hn.geometry = gdf_adef_hn.geometry.buffer(-0.1).to_crs(
                data_gfw.crs
            )

            # Start the creation of the table with updates
            gdf_updates = gdf_update1.sjoin(gdf_adef_hn, how="left")

            # Fill na values
            gdf_updates["value"] = gdf_updates["value"].fillna(0)
            gdf_updates["days_pg"] = gdf_updates["days_pg"].fillna(0)

            # Get the gcode
            gdf_updates["gcode"] = gdf_updates["value"].apply(lambda x: int(x % 10000))
            gdf_updates["gcode"] = np.where(
                gdf_updates["gcode"] > gdf_updates["days_pg"],
                gdf_updates["gcode"],
                gdf_updates["days_pg"],
            ).astype(int)
            gdf_updates["gcode"] = gdf_updates.groupby("noAlerta")["gcode"].transform(
                "max"
            )

            # Get the prot_hid
            gdf_updates["prot_hid"] = gdf_updates["value"].apply(
                lambda x: "Si" if int(x) >= 100000 else "No"
            )
            gdf_updates["prot_hid"] = gdf_updates.groupby("noAlerta")[
                "prot_hid"
            ].transform(lambda x: "Si" if "Si" in x.values else "No")

            gdf_updates.drop(columns=["index_right"], inplace=True)
            gdf_updates.drop_duplicates(
                subset=["noAlerta", "id", "gcode"], inplace=True
            )

            # Create metrics
            gdf_updates["adefpg_area"] = gdf_updates.groupby("noAlerta")[
                "adefpg_area"
            ].transform("sum")
            gdf_updates["alert_count"] = gdf_updates.groupby("cod")[
                "noAlerta"
            ].transform("nunique")
            gdf_updates["cod_count"] = gdf_updates.groupby("noAlerta")["cod"].transform(
                "nunique"
            )
            gdf_updates["diferencia"] = (
                gdf_updates["alert_area"] - gdf_updates["adefpg_area"]
            )
            print(
                f"{len(gdf_updates)} records obtained from the join of DB alerts and downloaded alerts"
            )
        except (KeyError, AttributeError, TypeError, ValueError) as e:
            print(f"Error joining attributes: {e}")
            raise

        # Decision logic
        print("---Applying decision logic...")

        def case_logic(row):
            # Field calculation values remain in Spanish for business logic
            if pd.isna(row["cod"]):
                return "Nueva"
            if abs(row["diferencia"]) < 0.001:
                return "Mantener"
            if row["diferencia"] >= 0.001:
                return "Reemplazar_add"
            if row["diferencia"] <= -0.001:
                return "Traslape_reemplazar"
            return "No_analizada"

        try:
            gdf_updates["update"] = gdf_updates.apply(case_logic, axis=1)
            print("Decision logic applied")
        except (KeyError, AttributeError, TypeError, ValueError) as e:
            print(f"Error applying decision logic: {e}")
            raise

        try:
            ids_to_reemp = (
                gdf_updates[
                    (gdf_updates["update"] == "Reemplazar_add")
                    & (gdf_updates["assigned"] != "Si")
                ]
                .id.unique()
                .astype(int)
            )
            if out_file:
                gdf_updates = vector.save_to_file(gdf_updates, out_file, **gpd_kwargs)
            return gdf_updates, ids_to_reemp
        except (KeyError, AttributeError, TypeError, ValueError) as e:
            print(f"Error creando updates: {e}")
            raise

    @profile
    def set_new_alerts(self, table_updates: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Select and prepare new alerts to be assigned from the update table.

        Args:
            table_updates (gpd.GeoDataFrame): Table with update cases.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame with new alerts ready for attribute preparation and DB insertion.
        """
        geom_col = table_updates.geometry.name
        columns_to_select = [
            geom_col,
            "noAlerta",
            "gcode",
            "prot_hid",
            "update",
            "days_pg",
        ]
        table_updates = table_updates[columns_to_select]
        # Only keep new or replacement alerts
        new_alerts = table_updates[
            table_updates["update"].isin(["Nueva", "Reemplazar_add"])
        ].copy()
        if new_alerts.empty:
            print("No new alerts in update to update.")
            return None

        # Remove already assigned alerts if present
        adefpg_assigned = self.adefpg_merged
        adefpg_assigned = adefpg_assigned[adefpg_assigned["assigned"] == "Si"].copy()
        new_alerts_unassigned = gpd.overlay(
            new_alerts, adefpg_assigned, how="difference", keep_geom_type=False
        )

        # Calculate area in hectares and filter out small areas
        new_alerts_unassigned = vector.calculate_area_ha(
            new_alerts_unassigned, "area_ha"
        )
        new_alerts_unassigned = new_alerts_unassigned[
            new_alerts_unassigned["area_ha"] > 0.5
        ].copy()

        if new_alerts_unassigned.empty:
            print("No new alerts to assign. Discard by diff with assigned alerts.")
            return None
        print(f"New alerts to assign: {len(new_alerts_unassigned)} records.")
        return new_alerts_unassigned

    @profile
    def add_attributes_to_new_alerts(
        self, new_alerts: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Add all required attributes to new alerts before inserting into the main table.

        Args:
            new_alerts (gpd.GeoDataFrame): GeoDataFrame with new alerts ready for attribute preparation.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame with all attributes ready for DB insertion.
        """
        gdf_base = new_alerts.copy()
        dict_data_rem_lim = self.dict_rem_lim
        keys_tables_lim = self._keys_tables_lim
        keys_tables_pma = self._keys_tables_rem_pma
        keys_tables_pa = self._keys_tables_rem_pa

        # Add the dates fields
        data_with_attributes = calculate_decompose_date(gdf_base, "gcode", "INTEGRATED")

        # Add observation text based on the update type
        data_with_attributes["obs"] = np.where(
            data_with_attributes["update"] == "Nueva",
            f"Nueva alerta procesada el {pd.Timestamp.today().date()}",
            f"Alerta con aumento de área procesada el {pd.Timestamp.today().date()}",
        )
        data_with_attributes.drop(
            columns=["update", "days_pg"], inplace=True, errors="ignore"
        )

        # Add lim attributes
        data_with_lim_attri = vector.add_lim_attributes(
            data_with_attributes, dict_data_rem_lim, keys_tables_lim, "noAlerta"
        )

        # Add rem attributes
        data_with_lim_rem_attri = vector.add_rem_attributes(
            data_with_lim_attri,
            dict_data_rem_lim,
            keys_tables_pma,
            keys_tables_pa,
        )

        # Analyze POA attributes
        print("Analyzing POA attributes...")
        data_with_lim_rem_attri = poa_cases(data_with_lim_rem_attri)

        # Aggregate the rows
        print("Making the aggregation of the data")
        data_with_lim_rem_attri = aggregate_data(
            data_with_lim_rem_attri, dict_data_rem_lim
        )

        # Add priority
        data_with_lim_rem_attri = adef_priority(
            data_with_lim_rem_attri,
            keys_tables_pma,
            keys_tables_pa,
            dict_data_rem_lim,
        )

        # Add ubication
        data_with_lim_rem_attri = add_ubication(data_with_lim_rem_attri)

        # Final adjustments
        # Convert to multipolygon if necessary
        from shapely.geometry import MultiPolygon, Polygon

        data_with_lim_rem_attri.geometry = data_with_lim_rem_attri.geometry.apply(
            lambda geom: MultiPolygon([geom]) if isinstance(geom, Polygon) else geom
        )

        # Rename gcode to gridcode
        data_with_lim_rem_attri.rename(columns={"gcode": "gridcode"}, inplace=True)

        # Drop unnecessary columns
        columns_to_drop = ["index_right", "noAlerta"]
        data_with_lim_rem_attri.drop(
            columns=columns_to_drop, inplace=True, errors="ignore"
        )

        return data_with_lim_rem_attri

    def _get_ids_main(self):
        """
        Returns a set of IDs present in the main database table.
        """
        engine_main = self.get_engine("db_main_readonly")
        schema_main = self.db_config["db_main"]["schema"]
        table_adefpg = self.get_table_name("db_main", "adefpg")
        sql = f"SELECT id FROM {schema_main}.{table_adefpg}"
        return (
            vector.load_table_as_gdf_or_df(
                sql=sql,
                engine=engine_main,
                is_geo=False,
            )["id"]
            .unique()
            .tolist()
        )

    def _get_ids_in_hist(self):

        engine_main = self.get_engine("db_main_readonly")
        schema_main = self.db_config["db_main"]["schema"]
        table_adefpg_hist = self.get_table_name("db_main", "adefpg_hist")
        sql = f"SELECT cod_old FROM {schema_main}.{table_adefpg_hist}"
        df = vector.load_table_as_gdf_or_df(
            sql=sql,
            engine=engine_main,
            is_geo=False,
        )

        if not df.empty:
            ids = df["cod_old"].str.split("-").str[1].dropna()
            ids = ids.astype(int).tolist()
            return ids
        else:
            print("No historical IDs found in the database.")

    @profile
    def update_main_db_table(
        self, data_to_update: gpd.GeoDataFrame, ids_to_reemp: np.ndarray
    ) -> bool:
        """
        Update the main alerts table in the database by deleting and inserting records.

        Args:
            data_to_update (gpd.GeoDataFrame): DataFrame with new alerts ready for insertion.
            ids_to_reemp (np.ndarray): Array of IDs to be replaced in the main table.

        Returns:
            bool: True if the update was successful, False otherwise.
        """
        # Identify IDs in PG tables
        ids_in_main = self._get_ids_main()
        ids_in_hist = self._get_ids_in_hist()
        ids_in_pg = set(ids_in_main) | set(ids_in_hist)

        # Get the ids for the new alerts
        no_geoms = data_to_update.shape[0]
        ids_seq = self.get_ids_seq(
            seq_alias="adefpg_seq",
            no_geoms=no_geoms,
        )
        ids_final = list(set(ids_seq) - set(ids_in_pg))
        while len(ids_final) != no_geoms:
            diff = no_geoms - len(ids_final)
            ids_to_complete = self.get_ids_seq(seq_alias="adefpg_seq", no_geoms=diff)
            ids_final.extend(ids_to_complete)
            ids_final = list(set(ids_final) - set(ids_in_pg))

        # Assign IDs to the new geometries
        data_to_update["id"] = ids_final

        # Get the cods for the new alerts
        data_to_update["cod"] = data_to_update.apply(
            lambda x: f"ADEF-{str(x['id']).zfill(8)}-{x['anio']}", axis=1
        )

        # Set the engine and schema for db_main
        engine_main = self.get_engine("db_main")
        schema_main = self.db_config["db_main"]["schema"]
        adefpg_table = self.get_table_name("db_main", "adefpg")

        # Set the query for the ids to replace
        query_delete = text(
            f"DELETE FROM {schema_main}.{adefpg_table} WHERE id IN ({', '.join(map(str, ids_to_reemp))})"
        )

        # Create a session to execute the query
        Session = sessionmaker(bind=engine_main)

        for attempt in range(3):
            session = Session()
            connection = session.connection()
            try:
                # Delete the ids to replace
                if len(ids_to_reemp) > 0:
                    connection.execute(
                        query_delete, {"ids_to_reemp": tuple(ids_to_reemp)}
                    )
                    print(f"Deleted {len(ids_to_reemp)} records from {adefpg_table}.")
                else:
                    print("No records to delete from the main table.")

                # Verify the geom column name
                if data_to_update.geometry.name != self.adefpg.geometry.name:
                    data_to_update = data_to_update.rename(
                        columns={
                            data_to_update.geometry.name: self.adefpg.geometry.name
                        }
                    )
                    data_to_update.set_geometry(self.adefpg.geometry.name, inplace=True)

                # Insert the new geometries into the main table
                data_to_update.to_postgis(
                    adefpg_table,
                    connection,
                    schema_main,
                    if_exists="append",
                )

                # Confirm the changes
                connection.commit()
                print(
                    f"Inserted {data_to_update.shape[0]} new records into `adefpg` main table."
                )
                return True
            except (SQLAlchemyError, OperationalError) as e:
                session.rollback()
                print(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(5)
            finally:
                session.close()
        print("Failed to update the main database table after multiple attempts.")
        return False

    def adefpg_not_in_hist(self):
        """Feature in development: Identify records in adefpg that are not in adefpg_hist."""
        # Define the engine, schema, and table names
        engine_main = self.get_engine("db_main_readonly")
        schema_main = self.db_config["db_main"]["schema"]
        table_adefpg = self.get_table_name("db_main", "adefpg")
        table_adefpg_hist = self.get_table_name("db_main", "adefpg_hist")

        # Create the SQL query to find records in adefpg not in adefpg_hist
        sql = f"""
            SELECT *
            FROM {schema_main}.{table_adefpg} AS adefpg
            WHERE NOT EXISTS (
                SELECT 1
                FROM {schema_main}.{table_adefpg_hist} AS adefpg_hist
                WHERE adefpg.cod = adefpg_hist.cod_old
            )
        """
        # Load the data into a GeoDataFrame
        gdf_not_in_hist = vector.load_table_as_gdf_or_df(
            sql=sql,
            engine=engine_main,
        )
        return gdf_not_in_hist

    @profile
    def set_new_alert_to_hist(self, data_gfw) -> tuple:
        """
        Identify and prepare new alerts from the main table that are not yet in the historical table.

        Returns:
            tuple: (GeoDataFrame with joined new and old codes, string of value pairs for SQL insertion)
        """
        # Reload main tables to ensure up-to-date data
        print("Starting the process to identify new alerts for the history table.")
        adefpg_table = self.adefpg
        adefpg_hist_table = self.adefpg_hist_less_cols

        # Identify geometries in the main table not present in the history table
        gdf_not_in_hist = adefpg_table.loc[
            ~adefpg_table["cod"].isin(adefpg_hist_table["cod_old"])
        ].copy()
        if gdf_not_in_hist.empty:
            print("No new geometries to process.")
            return None, None
        gdf_not_in_hist.geometry = gdf_not_in_hist.buffer(-0.00000001)

        # Prepare history table for join
        adefpg_hist_table = adefpg_hist_table[
            [adefpg_hist_table.geometry.name, "cod_old", "cod"]
        ].copy()

        # Spatial join to get old codes
        gdf = gpd.sjoin(gdf_not_in_hist, adefpg_hist_table)
        gdf = gdf.loc[gdf.cod_left != gdf.cod_right].copy()
        gdf.rename(
            columns={
                "cod_left": "cod",
            },
            inplace=True,
        )
        gdf = gdf.drop_duplicates(subset=["cod", "cod_old"], ignore_index=True)

        # Map codes to history table
        df = gdf[["cod", "cod_old"]].copy()

        # Use a set to ensure uniqueness
        values_cod = ", ".join(
            [
                f"('{cod_old}', '{cod}')"
                for cod_old, cod in zip(df["cod_old"], df["cod"])
            ]
        )

        # Erase data already in the history table
        gdf_eraser = gpd.sjoin(adefpg_hist_table, gdf_not_in_hist)

        # Data GFW without the geometries already in the history table
        data_gfw_diff = gpd.sjoin(data_gfw, gdf_not_in_hist)
        data_gfw_diff = gpd.overlay(
            data_gfw_diff, gdf_eraser, how="difference", keep_geom_type=False
        )
        if data_gfw_diff.empty and len(values_cod) == 0:
            print("No new geometries to process. Discard by diff with history table.")
            return None, None
        if data_gfw_diff.empty:
            print(
                "No new geometries to process. Discard by diff with history table. There are new cods to update."
            )
            return None, values_cod

        # Update fields
        data_gfw_diff["cod_old"] = data_gfw_diff["cod"]
        data_gfw_diff = vector.calculate_area_ha(data_gfw_diff, "area_ha")
        data_gfw_diff = add_ubication(data_gfw_diff)
        data_gfw_diff["gridcode"] = data_gfw_diff["value"] % 10000
        data_gfw_diff = calculate_decompose_date(
            data_gfw_diff, "gridcode", "INTEGRATED"
        )

        # Remove unnecessary columns
        columns_to_drop = ["index_right", "value"]
        data_gfw_diff.drop(columns=columns_to_drop, inplace=True, errors="ignore")

        return data_gfw_diff, values_cod

    def _get_ids_hist(self):
        """
        Returns a set of IDs present in the historical alerts table.
        """
        engine_main = self.get_engine("db_main_readonly")
        schema_main = self.db_config["db_main"]["schema"]
        table_adefpg_hist = self.get_table_name("db_main", "adefpg_hist")
        sql = f"SELECT id FROM {schema_main}.{table_adefpg_hist}"
        return (
            vector.load_table_as_gdf_or_df(
                sql=sql,
                engine=engine_main,
                is_geo=False,
            )["id"]
            .unique()
            .tolist()
        )

    def _batched_update_cod_hist(
        self, connection, schema_main, adefpg_hist_table, cod_pairs, batch_size=500
    ):
        """
        Ejecuta el UPDATE de cod en la tabla histórica en batches para evitar consultas demasiado grandes.
        Args:
            connection: conexión SQLAlchemy activa.
            schema_main: nombre del schema.
            adefpg_hist_table: nombre de la tabla.
            cod_pairs: lista de tuplas (cod_old, cod).
            batch_size: tamaño del batch (default 500).
        """
        for i in range(0, len(cod_pairs), batch_size):
            batch = cod_pairs[i : i + batch_size]
            values_cod = ", ".join(
                [f"('{cod_old}', '{cod}')" for cod_old, cod in batch]
            )
            query = text(
                f"""
                UPDATE {schema_main}.{adefpg_hist_table}
                SET cod = df.cod
                FROM (VALUES {values_cod}) AS df(cod_old, cod)
                WHERE {schema_main}.{adefpg_hist_table}.cod_old = df.cod_old;
            """
            )
            connection.execute(query)

    @profile
    def update_hist_db_table(self, data_to_add, values_cod: str) -> bool:
        """
        Update the historical alerts table in the database by updating codes and inserting new records.

        Args:
            data_to_add (gpd.GeoDataFrame): DataFrame with new alerts ready for insertion.
            values_cod (str): String of value pairs for SQL insertion.

        Returns:
            bool: True if the update was successful, False otherwise.
        """
        print("Starting the update of the history table with new alerts.")
        # Identify IDs in PG tables
        ids_in_hist = self._get_ids_hist()

        # Set the engine and schema for db_main
        engine_main = self.get_engine("db_main")
        schema_main = self.db_config["db_main"]["schema"]
        adefpg_hist_table = self.get_table_name("db_main", "adefpg_hist")

        # Parse values_cod into list of tuples for batching
        import re

        cod_pairs = re.findall(r"\('([^']+)', '([^']+)'\)", values_cod)

        # Create a session to execute the queries
        Session = sessionmaker(bind=engine_main)
        for attempt in range(3):
            session = Session()
            connection = session.connection()
            try:
                # Update the cods in the history table in batches
                if cod_pairs:
                    self._batched_update_cod_hist(
                        connection,
                        schema_main,
                        adefpg_hist_table,
                        cod_pairs,
                        batch_size=1000,
                    )
                    print(f"Values cod updated in {len(cod_pairs)} pairs (batched).")
                else:
                    print("No records to update in the history table.")

                # Get the ids for the new alerts
                no_geoms = data_to_add.shape[0]
                if no_geoms > 0:
                    ids_seq = self.get_ids_seq(
                        seq_alias="adefpg_hist_seq",
                        no_geoms=no_geoms,
                    )
                    ids_final = list(set(ids_seq) - set(ids_in_hist))
                    while len(ids_final) != no_geoms:
                        diff = no_geoms - len(ids_final)
                        ids_to_complete = self.get_ids_seq(
                            seq_alias="adefpg_hist_seq", no_geoms=diff
                        )
                        ids_final.extend(ids_to_complete)
                        ids_final = list(set(ids_final) - set(ids_in_hist))

                    # Assign IDs to the new geometries
                    data_to_add["id"] = ids_final

                    # Verify the geom column name
                    if data_to_add.geometry.name != self.adefpg_hist.geometry.name:
                        data_to_add = data_to_add.rename(
                            columns={
                                data_to_add.geometry.name: self.adefpg_hist.geometry.name
                            }
                        )
                        data_to_add.set_geometry(
                            self.adefpg_hist.geometry.name, inplace=True
                        )

                    # Insert the new geometries into the history table
                    data_to_add.to_postgis(
                        adefpg_hist_table,
                        connection,
                        schema_main,
                        if_exists="append",
                        chunksize=1000,
                    )

                # Confirm the changes
                connection.commit()
                print(
                    f"Inserted {data_to_add.shape[0]} new records into `adefpg_hist` history table."
                )
                return True
            except (SQLAlchemyError, OperationalError) as e:
                session.rollback()
                print(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(5)
            finally:
                session.close()

    def execute_sql_file(self, sql_file_path, db_alias="db_main_readonly"):
        """
        Executes a complete SQL file on the configured engine, using the native cursor for maximum compatibility with complex scripts (DO $$...$$, multiple statements, etc).

        Args:
            sql_file_path (str or Path): Path to the .sql file to execute.
            db_alias (str): Logical alias of the engine (default 'db_main').

        Returns:
            bool: True if the execution was successful, False if error.
        """

        engine = self.get_engine(db_alias)
        if engine is None:
            print(f"[execute_sql_file] No engine found for alias '{db_alias}'.")
            return False
        sql_file_path = str(sql_file_path)
        if not os.path.isfile(sql_file_path):
            print(f"[execute_sql_file] SQL file not found: {sql_file_path}")
            return False
        try:
            with open(sql_file_path, "r", encoding="utf-8") as f:
                sql_script = f.read()
            # Use the native cursor for maximum compatibility
            with engine.begin() as conn:
                raw_conn = conn.connection
                if hasattr(raw_conn, "connection"):
                    # SQLAlchemy >=1.4: extract the psycopg2 connection
                    raw_conn = raw_conn.connection
                with raw_conn.cursor() as cur:
                    cur.execute(sql_script)
            print(f"[execute_sql_file] Successfully executed SQL file: {sql_file_path}")
            return True
        except SQLAlchemyError as e:
            print(f"[execute_sql_file] SQLAlchemy error: {e}")
            return False
        except OSError as e:
            print(f"[execute_sql_file] Error executing SQL file: {e}")
            return False
