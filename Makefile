VENV := .venv
PYTHON := $(VENV)/bin/python
PIP_COMPILE := $(VENV)/bin/pip-compile

.PHONY: run simulate simulate-faulty test lock

$(VENV)/.installed: pyproject.toml requirements.lock
	python3 -m venv --clear $(VENV)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.lock
	$(PYTHON) -m pip install -e . --no-deps
	touch $(VENV)/.installed

lock: pyproject.toml
	python3 -m pip install pip-tools
	pip-compile pyproject.toml --extra test -o requirements.lock

run: $(VENV)/.installed
	$(PYTHON) -m constellationops

simulate: $(VENV)/.installed
	$(PYTHON) -m constellationops.simulator --duration 15

simulate-faulty: $(VENV)/.installed
	$(PYTHON) -m constellationops.simulator \
		--seed 42 \
		--duration 20 \
		--rate 3 \
		--drop-prob 0.15 \
		--dup-prob 0.15 \
		--reorder-prob 0.15 \
		--reorder-delay-ms 500

test: $(VENV)/.installed
	$(PYTHON) -m pytest
