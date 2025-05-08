# Configuraciónes iniciales
## Instalación de dependencias

Para instalar en linux
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
Para instalar en windows
```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

## Inicializar el proyecto
Ejecutar el siguiente script en la carpeta raiz del proyecto
```bash
uv run main.py
```

Para mas información sobre el uso de uv puedes consultar la [documentación](https://docs.astral.sh/uv/)


# Generacion del script en `.py`
Las pruebas y desarrollo del script se realizan en un notebook de jupyter, para generar el script en `.py` se ejecuta lo siguiente en el terminal
```bash
jupyter nbconvert --to script adef_intg.ipynb
```

Esta ejecución genera un archivo `adef_intg.py` en la misma carpeta donde se encuentra el notebook. que luego se puede ejecutar lo siguiente en el terminal
```bash
uv run adef_intg.py
```

Developed by *@lalgonzales | ICF/CIPF/UMF*

