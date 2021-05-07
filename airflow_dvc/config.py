import os
import toml
from typing import Optional
from semantic_version import SimpleSpec

from dataclasses import dataclass


@dataclass
class DVCConfig:
    dvc_version_constraint: SimpleSpec


_pyproject_config: Optional[DVCConfig] = None


def get_config() -> DVCConfig:
    global _pyproject_config
    if _pyproject_config is None:
        config = toml.load(os.path.join(os.path.dirname(__file__), "..", "pyproject.toml"))
        print(config)
        extras = config["tool"]["poetry"]["extras"]["dvc"]
        dvc_constraint = [cons for cons in extras if "dvc" in cons][0].replace("dvc", "")
        _pyproject_config = DVCConfig(
            dvc_version_constraint=SimpleSpec(dvc_constraint),
        )
    return _pyproject_config

