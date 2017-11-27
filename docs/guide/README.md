# Concourse Guide

This directory contains source files for the Concourse Guide which is published at [https://docs.cinchapi.com/concourse](https://docs.cinchapi.com/concourse).

## Usage
We use `mkdocs` to build the guide. Use the `./mkdocs` script to access `mkdocs` functionality.

## Configuration
Configuration is contained in the `mkdocs.yml` file. Read the mkdocs documentation at [http://mkdocs.readthedocs.io/en/0.10/](http://mkdocs.readthedocs.io/en/0.10/) for more information.

## Publishing
The guide is published using Github Pages via the `gh-pages` branch in this repo.

To publish changes:
```bash
./mkdocs gh-deploy
```
