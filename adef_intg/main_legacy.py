"""This is the legacy version of the ADEF integration script."""

import sys
from pathlib import Path

from adef_intg import adef_intg_fn, interactive

if getattr(sys, "frozen", False):
    base_dir = Path.cwd()
else:
    base_dir = Path(__file__).resolve().parent.parent


def main():
    """
    Main function to execute the ADEF integration process.
    This function gathers user inputs, validates them, and runs the process.
    """
    confidence = interactive.ask_confidence()
    out_folder = interactive.select_folder()
    out_file, layer_name = interactive.ask_output_file()
    start_date, end_date = interactive.dates_filter()

    print("\n📊 Resumen de parámetros:")
    print(f"🔢 Confianza: {confidence}")
    print(f"📂 Carpeta: {out_folder}")
    print(f"📄 Archivo: {out_file} | Capa: {layer_name}")
    print(f"📅 Fechas: {start_date} - {end_date if end_date else '...' }")

    if input("✅ ¿Todo correcto? (s/n): ").strip().lower() != "s":
        print("❌ Cancelado por el usuario.")
        return
    try:
        adef_intg_fn.run_adef_process(
            confidence, out_folder, out_file, layer_name, start_date, end_date, base_dir
        )
        print("✅ Proceso completado con éxito.")
    except Exception as e:
        print("❌ Error durante el proceso.")
        print(f"Detalles del error: {e}")
        raise


if __name__ == "__main__":
    main()
