SHELL = /bin/bash
SERIES_TIEMPO_PIP ?= pip
SERIES_TIEMPO_PYTHON ?= python
VIRTUALENV = series-tiempo-ar-smn-scraping
CONDA_ENV = series-tiempo-ar-smn-scraping
ACTIVATE = /home/smn/miniconda3/bin/activate

.PHONY: all clean install_anaconda setup_anaconda


install_anaconda:
	wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
	bash Miniconda3-latest-Linux-x86_64.sh
	rm Miniconda3-latest-Linux-x86_64.sh
	export PATH=$$PATH:/home/smn/miniconda3/bin

setup_anaconda:
	conda create -n $(CONDA_ENV) python=3.6 --no-default-packages
	source $(ACTIVATE) $(CONDA_ENV); $(SERIES_TIEMPO_PIP) install -e .
