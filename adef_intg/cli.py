"""This module provides a command line interface (CLI) for processing ADEF integrated alerts at the national level."""

import sys
from pathlib import Path
import click
from adef_intg.adef_intg_fn import run_adef_process
from adef_intg.utils_adef import get_safe_lock


@click.command()
@click.option(
    "--confidence",
    type=int,
    default=1,
    help="Nivel de confianza (1–4). Valor predeterminado: 1.",
)
@click.option(
    "--out-folder",
    type=click.Path(),
    default="./results",
    help="Carpeta de salida. Valor predeterminado: './results'.",
)
@click.option(
    "--out-file",
    default="adef_intg.gpkg",
    help="Nombre del archivo de salida. Valor predeterminado: 'adef_intg.gpkg'.",
)
@click.option(
    "--layer-name",
    default="alerts",
    help="Nombre de la capa dentro del archivo. Valor predeterminado: 'alerts'.",
)
@click.option(
    "--start-date", default=None, help="Fecha de inicio (YYYY-MM-DD). Opcional."
)
@click.option("--end-date", default=None, help="Fecha de fin (YYYY-MM-DD). Opcional.")
@click.option(
    "--use-dask",
    is_flag=True,
    default=False,
    help="Usar Dask para procesamiento distribuido. Por defecto: False.",
)
def cli(confidence, out_folder, out_file, layer_name, start_date, end_date, use_dask):
    """CLI para procesar alertas integradas ADEF a nivel nacional."""

    # Resolver la ruta absoluta de la carpeta de salida
    out_folder_resolved = Path(out_folder).resolve()

    # Imprimir los valores de las opciones
    click.echo("Se utilizarán los siguientes valores:")
    click.echo(f"  Nivel de confianza: {confidence}")
    click.echo(f"  Carpeta de salida: {out_folder_resolved}")
    click.echo(f"  Archivo de salida: {out_file}")
    click.echo(f"  Nombre de la capa: {layer_name}")
    click.echo(f"  Fecha de inicio: {start_date}")
    click.echo(f"  Fecha de fin: {end_date}")
    click.echo(f"  Usar Dask: {use_dask}")

    # Solicitar confirmación
    confirm = click.confirm("¿Desea continuar con estos valores?", default=True)
    if not confirm:
        click.echo("Operación cancelada.")
        sys.exit(0)  # Salir del programa si el usuario no confirma

    base_dir = Path(__file__).resolve().parent.parent

    # Inicializar client Dask si se indica
    client = None
    if use_dask:
        try:
            from dask.distributed import Client, default_client

            try:
                client = default_client()
                click.echo("Client Dask ya activo.")
            except ValueError:
                client = Client()
                click.echo("Client Dask local iniciado.")
        except ImportError:
            click.echo("Dask no está instalado. Continuando sin Dask.")

    lock_read = get_safe_lock("rio-read", client=client)
    lock_write = get_safe_lock("rio", client=client)

    run_adef_process(
        confidence,
        out_folder_resolved,
        out_file,
        layer_name,
        start_date,
        end_date,
        base_dir,
        lock_read,
        lock_write,
    )


if __name__ == "__main__":
    cli()
