"""This module is historic funcs retired from utils_adef.py"""

from datetime import datetime
import os
from pathlib import Path

try:
    from tkinter import Tk
    from tkinter.filedialog import askdirectory

    GUI_AVAILABLE = True
except ImportError:
    GUI_AVAILABLE = False

BASE_DIR = Path(__file__).resolve().parent.parent


def ask_confidence():
    while True:
        answer = input("🔢 Nivel de confianza mínimo (1–4): ").strip()
        if answer in {"1", "2", "3", "4"}:
            return int(answer)
        print("⚠️ Opción no válida. Escribe 1, 2, 3 o 4.")


def select_folder():
    default = BASE_DIR / "results"
    if GUI_AVAILABLE:
        try:
            root = Tk()
            root.withdraw()
            print(
                "📂 Se abrirá un explorador de archivos para seleccionar una carpeta..."
            )
            folder = askdirectory(
                title="Selecciona una carpeta para guardar los resultados"
            )
            root.destroy()
            if folder and os.path.isdir(folder):
                return folder
        except Exception as e:
            print(f"⚠️ Error con el explorador de archivos: {e}")
    print(f"⌨️ Usando carpeta predeterminada: {default}")
    os.makedirs(default, exist_ok=True)
    return str(default)


def ask_output_file():
    default_file = "adef_intg.gpkg"
    default_layer = f"adef_intg_{datetime.now().strftime('%Y%m%d')}"
    print(
        f"📁 Por defecto, se creará un archivo con el nombre '{default_file}' y capa '{default_layer}'."
    )

    file_name = input(
        "📁 Nombre del archivo (sin extensión, Enter para usar predeterminado): "
    ).strip()
    layer_name = input(
        "📄 Nombre de la capa (Enter para usar predeterminado): "
    ).strip()

    if not file_name:
        file_name = default_file
    else:
        file_name = f"{file_name}.gpkg"

    if not layer_name:
        layer_name = default_layer

    return file_name, layer_name


def dates_filter():
    print("🗓️ Puedes definir un rango de fechas opcional para filtrar resultados.")
    print("   Deja vacío ambos campos si no deseas aplicar filtro.\n")

    while True:
        start_raw = input(
            "📅 Fecha de inicio (YYYY-MM-DD) [Enter para omitir]: "
        ).strip()
        end_raw = input("📅 Fecha de fin    (YYYY-MM-DD) [Enter para omitir]: ").strip()

        if not start_raw and not end_raw:
            return None, None

        try:
            start_date = datetime.strptime(start_raw, "%Y-%m-%d")
            end_date = datetime.strptime(end_raw, "%Y-%m-%d")
            if start_date > end_date:
                raise ValueError("La fecha de inicio es posterior a la fecha de fin.")
            return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        except ValueError as e:
            print(f"⚠️ Error: {e}. Intenta nuevamente.")
