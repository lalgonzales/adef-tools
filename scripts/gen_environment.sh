#!/bin/bash
# scripts/gen_environment.sh
# Genera un environment.yml válido desde requirements.txt generado por uv

set -euo pipefail

ENV_NAME="adef-env"
REQUIREMENTS_FILE="requirements.txt"
OUTPUT_FILE="environment.yml"

echo "name: ${ENV_NAME}" > "$OUTPUT_FILE"
echo "channels:" >> "$OUTPUT_FILE"
echo "  - conda-forge" >> "$OUTPUT_FILE"
echo "dependencies:" >> "$OUTPUT_FILE"
echo "  - python>=3.10" >> "$OUTPUT_FILE"
echo "  - mamba" >> "$OUTPUT_FILE"
echo "  - gdal" >> "$OUTPUT_FILE"
echo "  - libgdal" >> "$OUTPUT_FILE"
echo "  - libgdal-arrow-parquet" >> "$OUTPUT_FILE"
echo "  - pip" >> "$OUTPUT_FILE"
echo "  - pip:" >> "$OUTPUT_FILE"

# Procesamiento de requirements.txt
buffer=""
while IFS= read -r line || [[ -n "$line" ]]; do
  # Eliminar comentarios y espacios
  clean_line=$(echo "$line" | sed 's/#.*//' | xargs)
  [[ -z "$clean_line" ]] && continue

  # Continuación de línea \
  if [[ "$clean_line" == *"\\" ]]; then
    buffer+="${clean_line%\\} "
    continue
  else
    buffer+="$clean_line"
  fi

  # Eliminar --hash y condiciones
  dep=$(echo "$buffer" | sed 's/--hash=[^ ]*//g' | sed 's/;.*//g' | xargs)
  [[ -n "$dep" ]] && echo "    - $dep" >> "$OUTPUT_FILE"

  # Reiniciar buffer
  buffer=""
done < "$REQUIREMENTS_FILE"

echo "$OUTPUT_FILE generado correctamente desde $REQUIREMENTS_FILE"
