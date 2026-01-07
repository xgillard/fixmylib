"""script un peu crade qui corrige le path pour qu'on puisse executer le code
en utilisant plus de dépendances que ce qui est fourni de base par calibre."""

import site
from pathlib import Path


def init():
    current_dir = Path(__file__).parent.absolute()
    venv_path = current_dir / ".venv"
    site_packages = list(venv_path.glob("lib/python*/site-packages"))

    if site_packages:
        site.addsitedir(str(site_packages[0]))
