# Configuración y uso de adef-tools

---

## 1. Configurando el entorno de Python

### En Linux/Mac (recomendado: `uv`)

Instala `uv` siguiendo la documentación del sitio:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
Más opciones de instalación:
https://docs.astral.sh/uv/getting-started/installation/

Crea y activa el entorno virtual:
```bash
uv venv
source .venv/bin/activate
```

Instala las dependencias principales (incluyendo ruedas especiales para GDAL y pyproj):
```bash
uv pip install --find-links https://girder.github.io/large_image_wheels gdal pyproj
```

---

### En Windows (recomendado: Miniconda)

Instala Miniconda con winget:
```bash
winget install anaconda.miniconda3
```
Mas opciones de instalacion:
https://www.anaconda.com/docs/getting-started/miniconda/install

Crea y activa el entorno:
```bash
conda create -n adef-tools python=3.12
conda activate adef-tools
```

Instala las dependencias principales:
```bash
conda install -c conda-forge gdal libgdal-arrow-parquet libgdal pip
```

---

## 2. Instalando el paquete

Con el entorno ya activado, instala la herramienta desde PyPI:

```bash
pip install adef-tools
```

Verifica la instalación:
```bash
adef-tools --help
```

## 3. Usando el paquete

### Ejecución básica

```bash
adef-tools
```

### Opciones predeterminadas

Si no proporcionas opciones, se usarán los siguientes valores:
- `--vector`: Utilizá el límite HN por defecto.
- `--confidence`: 1
- `--out-folder`: `./results`
- `--out-file`: `adef_intg.gpkg`
- `--layer-name`: `alerts`
- `--start-date` y `--end-date`: No se aplicará filtrado por fechas.
- `--use-dask`:

### Ejemplo de uso personalizado

```bash
adef-tools --confidence 2 --out-folder ./custom_results --out-file custom_output.gpkg --layer-name custom_layer --start-date 2023-01-01 --end-date 2023-12-31
```

---

## Créditos
Desarrollado por *@lalgonzales | ICF/CIPF/UMF*

## Licencia
Este proyecto está bajo la Licencia MIT. Consulta el archivo [LICENSE](LICENSE) para más detalles.