"""Vector functions for ADEF tools."""

import os
import sys
import time
import pathlib
import geopandas as gpd
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.orm import sessionmaker


def load_table_as_gdf_or_df(
    table_name, engine, schema="public", is_geodata=True, retries=3
):
    """
    Load a table from a database as a GeoDataFrame or DataFrame.

    :param table_name: Name of the table to load.
    :param engine: SQLAlchemy engine to connect to the database.
    :param schema: Schema where the table is located, defaults to 'public'.
    :param is_geodata: Flag indicating if the table contains geospatial data, defaults to True.
    :param retries: Number of retry attempts in case of failure, defaults to 3.
    :return: Loaded table as a GeoDataFrame if is_geodata is True, otherwise as a DataFrame.
    :raises SQLAlchemyError: If there is an error with the SQLAlchemy operation.
    :raises OperationalError: If there is an operational error during the database connection.
    """
    Session = sessionmaker(bind=engine)

    for attempt in range(retries):
        session = Session()
        connection = session.connection()
        try:
            if is_geodata:
                gdf = gpd.read_postgis(
                    f"SELECT * FROM {schema}.{table_name}", connection
                )
                gdf.geometry = gdf.geometry.make_valid()
            else:
                gdf = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", connection)
                gdf = gpd.GeoDataFrame(
                    gdf,
                    geometry=None,
                )
            session.close()
            return gdf
        except (SQLAlchemyError, OperationalError) as e:
            session.rollback()
            print(f"Intento {attempt + 1} de {retries} fallido: {e}")
            time.sleep(5)  # Esperar antes de reintentar
        finally:
            session.close()
    print(f"No se pudo cargar {table_name} después de {retries} intentos.")


def validate_setting_vector(vector, **gpd_kwargs):
    """
    Validates and loads a vector file or GeoDataFrame, returning the object and its name.

    Args:
        vector (str, pathlib.Path, or gpd.GeoDataFrame): Path to the vector file or a GeoDataFrame.
        layer (str, optional): Layer name for multi-layer files (e.g., GPKG).

    Raises:
        FileNotFoundError: If the vector file path does not exist.
        ValueError: If the GeoDataFrame does not have a name or CRS.

    Returns:
        tuple: (GeoDataFrame, vector name)
    """
    if isinstance(vector, (str, pathlib.Path)):
        if not os.path.exists(vector):
            raise FileNotFoundError(f"The input vector file does not exist: {vector}")
        ext = os.path.splitext(vector)[1].lower()
        if ext == ".parquet":
            gdf = gpd.read_parquet(vector, **gpd_kwargs)
        else:
            gdf = gpd.read_file(vector, **gpd_kwargs)
        vector_name = os.path.splitext(os.path.basename(vector))[0]
    elif isinstance(vector, gpd.GeoDataFrame):
        gdf = vector
        # Usa el atributo .name si existe, si no, asigna un nombre genérico
        vector_name = "vector"
    else:
        raise TypeError("The input must be a string, pathlib.Path, or GeoDataFrame.")
    if gdf.crs is None:
        raise ValueError("The GeoDataFrame must have a CRS defined.")
    return gdf, vector_name
