VENV := .venv
PYTHON := $(VENV)/bin/python

.PHONY: run simulate simulate-faulty test

$(VENV)/.installed: pyproject.toml
	python3 -m venv --clear $(VENV)
	$(PYTHON) -m pip install -e ".[test]"
	touch $(VENV)/.installed

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
