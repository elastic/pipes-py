PYTHON ?= python3

ifneq ($(findstring MSYS_NT,$(shell uname)),)
	ACTIVATE = $(VENV)/scripts/Activate.bat
else
	ACTIVATE = source $(VENV)/bin/activate
endif

all: lint test

prereq:
	$(PYTHON) -m pip install -r requirements.txt

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m black -q --check . || ($(PYTHON) -m black .; false)
	$(PYTHON) -m isort -q --check . || ($(PYTHON) -m isort .; false)

test:
	$(MAKE) test-venv

ruleset-check:
	$(PYTHON) scripts/check-ruleset-sync.py

.venv-test:
	$(PYTHON) -m venv $@

test-venv: VENV := .venv-test
test-venv: .venv-test
	$(ACTIVATE); $(MAKE) test-ci

test-ci:
	pip install -r requirements.txt .
	python -m unittest discover tests

pkg-build:
	$(PYTHON) -m build

pkg-install:
	$(PYTHON) -m pip install --force-reinstall dist/elastic_pipes-*.whl

pkg-test:

package: VENV := .venv-test
package: pkg-build
	rm -rf $(VENV)
	$(PYTHON) -m venv $(VENV)
	$(ACTIVATE); $(MAKE) pkg-install pkg-test
	rm -rf $(VENV)

clean:
	rm -rf build *.egg-info .venv-test
