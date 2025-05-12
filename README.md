# Configuraciones y uso del proyecto

## Instalación de `uv`

El proyecto funciona con `uv` para gestionar el entorno virtual y las dependencias. A continuación, se explica cómo instalarlo según tu sistema operativo:

### En Linux
Ejecuta el siguiente la siguiente linea en la terminal para instalar `uv`:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### En Windows
Abre PowerShell y ejecuta:
```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Verificar la instalación
Después de instalar `uv`, verifica que esté correctamente instalado ejecutando:
```bash
uv --version
```

Si el se muestra la versión de `uv`, la instalación fue exitosa.

---

## Inicializar el proyecto

### Primera ejecución
Para configurar el entorno virtual y asegurarte de que todas las dependencias estén instaladas, ejecuta:
```bash
uv run hello.py
```

Este instrucción instalará automáticamente las dependencias necesarias y verificará que todo esté listo para usar.

### Ejecutar el proyecto principal
El proyecto incluye una "Command Line Interfaz" (CLI) para procesar alertas integradas. Para ejecutarla, usa el siguiente corre esta linea en la terminal:
Una vez configurado el entorno, puedes ejecutar el proyecto principal con:
```bash
uv run adef_intg/cli.py
```

Si no proporcionas opciones, se usarán los siguientes valores predeterminados:
- `--confidence`: 1
- `--out-folder`: `./results`
- `--out-file`: `adef_intg.gpkg`
- `--layer-name`: `alerts`
- `--start-date` y `--end-date`: No se aplicará filtrado por fechas.

También puedes proporcionar tus propios valores. Por ejemplo:
```bash
uv run adef_intg/cli.py --confidence 2 --out-folder ./custom_results --out-file custom_output.gpkg --layer-name custom_layer --start-date 2023-01-01 --end-date 2023-12-31
```

## Uso rápido con alias o Makefile

Puedes agregar un alias en tu terminal para evitar escribir el path completo:

```bash
alias adef="uv run adef_intg/cli.py"
```
Luego, puedes ejecutar el proyecto con:
```bash
adef --confidence 2 --out-folder ./out --out-file salida.gpkg --layer-name alertas
```
O puedes usar un `Makefile`, se incluye un ejemplo en el proyecto. Para usarlo, asegúrate de tener `make` instalado y ejecuta:
```bash
make run ARGS="--confidence 2 --out-folder ./out --out-file salida.gpkg --layer-name alertas"
```

---

## Créditos
Desarrollado por *@lalgonzales | ICF/CIPF/UMF*


## Licencia
Este proyecto está bajo la Licencia MIT. Consulta el archivo [LICENSE](LICENSE) para más detalles.