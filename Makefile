.PHONY: docs

env:
	- micromamba create -n ahah --file env.yml

conda:
	- conda create -n ahah --file env.yml

run:
	- dvc repro

routing:
	- python -m ahah.routing

docs:
	pdoc --html --force --output-dir docs ahah
