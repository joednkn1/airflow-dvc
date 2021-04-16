#!/bin/bash

pip install --requirement <(poetry export --dev --format requirements.txt)
pip install --no-deps .