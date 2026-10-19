.PHONY: test demo run-api clean

test:
	python -m pytest -q

demo:
	python -m lakehouse_platform.cli --events 1000 --out ./lakehouse-data --fresh

run-api:
	LAKEHOUSE_ROOT=./lakehouse-data uvicorn lakehouse_platform.api:app --port 8000

clean:
	rm -rf lakehouse-data .pytest_cache
