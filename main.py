"""
J'ai cassé ma bibliothèque calibre, mais j'avais un backup pas trop vieux.
Ce script sert à récupérer les métadonnées de tout le bazar pour re-créér une
bibliothèque qui soit ok.


Note importante:
Il faut lancer le script en utilisant calibre-debug plutot que python c'est
ce qui permet d'utiliser calibre comme une bibliothèque python.
"""

# on bidouille avec le path de python pour pouvoir combiner les libs qui sont
# rendues dispo via mon virtualenv managé avec uv et celles qui viennent de
# calibre elle meme. C'est un peu moche, mais c'est le tout premier truc à
# faire.
from foeffel import init

init()

import json  # noqa: E402
import shutil  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import re  # noqa: E402

import pandas as pd  # noqa: E402
from pathlib import (  # noqa: E402
    Path,
)
from Levenshtein import distance  # noqa: E402
from io import (  # noqa: E402
    StringIO,
)
from calibre.ebooks.metadata.opf import (  # noqa: E402
    OPF,
)
from calibre.ebooks.metadata.meta import (  # noqa: E402
    get_metadata,
    set_metadata,
)

from calibre.ebooks.metadata.book.base import (  # noqa: E402
    Metadata,
)


def load_database():
    """On charge la db calibre dans un dataframe.
    J'aurais aussi pu utiliser sqlite3 qui fournit le backend pour la vraie
    db de calibre, mais faire un export avec `calibredb list --all --for-machines`
    m'a semblé plus simple.
    """

    fname = "db.json"

    with open(fname, "r", encoding="utf8") as f:
        d = json.load(f)
        df = pd.DataFrame.from_records(d, index=["id"])
    return df


def closest_id(title: str, db: pd.DataFrame) -> int | None:
    """Recupere l'id de bouquin dans la db de calibre qui est le plus proche
    du titre du fichier lui meme."""

    cp = db.copy()
    cp["dist"] = cp["title"].apply(lambda x: distance(x.lower(), title.lower()))
    cp = cp[cp["dist"] <= 5]
    res = cp.nsmallest(n=1, columns="dist").index.values
    if len(res) == 0:
        return None
    else:
        return res[0]


def parse_opf(meta_opf):
    """Lit un fragment opf et le déserialise en un objet metadata de calibre"""
    stream = StringIO(meta_opf)
    return OPF(stream).to_book_metadata()


def get_meta_opf(id: int) -> Metadata:
    """Recupere les métadonnes d'un livre tel qu'il est connu dans la db calibre
    (sur base de l'identifiant donné par la db calibre)."""
    result = subprocess.run(
        [
            "calibredb",
            "show_metadata",
            "--with-library",
            "/home/xgillard/Calibre",
            "--as-opf",
            str(id),
        ],
        capture_output=True,
        text=True,
        encoding="utf8",
    )
    return parse_opf(result.stdout)


def get_format(path: Path) -> str:
    """Renvoie le format selon le schéma accepté par calibre debug"""

    return path.suffixes[-1][1:].lower().strip()


def get_meta_native(path: Path) -> Metadata:
    """Recupere les metadonnées de facon fiable"""

    format = get_format(path)

    with path.open("rb") as f:
        return get_metadata(  # type: ignore
            f,
            stream_type=format,
            force_read_metadata=True,
        )


def set_meta_native(path: Path, mi: Metadata):
    """Sette les meta données."""

    format = get_format(path)

    with path.open("rb+") as f:
        return set_metadata(
            f,
            mi,
            stream_type=format,
        )


def fix_meta(path: Path, db: pd.DataFrame):
    """Fixe les métadonnées du livre dont on passe le path en utilisant les
    infos qui viennent de la db."""
    mi = get_meta_native(path)
    id_ = closest_id(mi.title, db)
    if id_ is None:
        return mi

    opf = get_meta_opf(id_)

    mi.smart_update(opf)
    set_meta_native(path, mi)
    return mi


def fix_filename(path: Path, root: Path, mi: Metadata):
    """Copie le fichier dans un nouveau directory en lui donnant un nom
    canonique qui dépend de ses métadonnées."""
    suffix = path.suffix
    # category
    category = min(mi.tags) if mi.tags else "unsorted"
    dst = root / category

    # authors
    authors = "unk-auth"
    if (
        mi.authors is not None
        and isinstance(mi.authors, list)
        and not isinstance(mi.authors, str)
    ):
        authors = ",".join(mi.authors)
        authors = authors.replace("|", ",")
        authors = authors.replace("-", "_")
        authors = re.sub(r"['\"-]", " ", authors)
        authors = authors.split(",")
        authors = [a.strip() for a in authors]
        authors = f"{authors[0]}_et_al" if len(authors) > 1 else authors[0]
    else:
        authors = mi.authors if mi.authors.strip() else "unk-auth"
    authors = authors.split()[-1]

    # publisher
    publisher = mi.publisher if mi.publisher else "unk-pub"
    publisher = publisher.split()[0]
    # pubdate
    pubyr = mi.pubdate.year if mi.pubdate else "unk-date"
    pubinfo = f"{publisher}-{pubyr}"

    # structured fname
    struct_name = f"{mi.title}-{authors}-{pubinfo}{suffix}"

    if mi.series:
        dst = dst / mi.series / f"{mi.series_index:05d}-{struct_name}"
    else:
        dst = dst / struct_name

    dst = dst.absolute()
    path = path.absolute()

    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(path.absolute(), dst.absolute())


def fix_library(inpath: Path, outpath: Path):
    """Fix tous les fichiers qui se trouvent dans la bibliothèque en entrée
    et stocke les fichiers corrigés dans le dossier de sortie."""
    db = load_database()

    for fn in inpath.glob("**/*"):
        if fn.suffix in [".epub", ".pdf", ".mobi", ".azw3"]:
            mi = fix_meta(Path(fn), db)
            fix_filename(Path(fn), outpath, mi)


if __name__ == "__main__":
    inpath = sys.argv[1]
    outpath = sys.argv[2]

    fix_library(Path(inpath), Path(outpath))
