# Makefile para ejecutar el CLI con uv y pasar variables fácilmente

run:
	uv run adef_intg/cli.py $(ARGS)
