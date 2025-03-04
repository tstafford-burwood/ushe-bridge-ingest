#!/bin/sh
coverage run -m pytest -v  /packages/asu_apgap_dataproc_package/ -n 2 -vv -k TestCompleterDataframeFactory
coverage report -m
