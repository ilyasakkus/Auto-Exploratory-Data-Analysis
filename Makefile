.PHONY: install test lint demo clean

install:
	pip install -e ".[dev]"

test:
	pytest -v tests/

lint:
	flake8 spark_auto_eda tests

demo:
	python examples/basic_usage.py

clean:
	rm -rf build dist *.egg-info .pytest_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
