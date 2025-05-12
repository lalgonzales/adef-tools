# adef_intg.spec
from PyInstaller.utils.hooks import collect_submodules
from pathlib import Path

# Ruta base de tu script
src_path = Path(".") / "adef_intg"

# Entrypoint
entry_script = str(src_path / "cli.py")

# Hidden imports necesarios
hidden = collect_submodules("rasterio") + collect_submodules("fiona") + [
    "rioxarray",
    "pyproj",
    "shapely",
]

a = Analysis(
    [entry_script],
    pathex=[str(src_path)],
    hiddenimports=hidden,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
)

pyz = PYZ(a.pure, a.zipped_data, cipher=None)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="adef-intg",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="adef-intg",
)
