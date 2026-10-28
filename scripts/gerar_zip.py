"""
Gera dist/spark-query-studio.zip com o projeto completo (todos os arquivos versionados no git).

    python scripts/gerar_zip.py           # gera ou atualiza o zip
    python scripts/gerar_zip.py --check   # só confere se o zip está atualizado (usado no CI)

O zip só é regravado quando o conteúdo do projeto muda, e é determinístico (datas fixas, ordem fixa).
Assim ele só aparece como alterado no git quando o projeto muda de verdade.
"""
from __future__ import annotations

import argparse
import io
import os
import subprocess
import sys
import zipfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
NOME = "spark-query-studio"
DESTINO = RAIZ / "dist" / f"{NOME}.zip"
DATA_FIXA = (2026, 1, 1, 0, 0, 0)
IGNORAR_PREFIXOS = ("dist/", ".github/")
IGNORAR_NOMES = {".gitignore"}


def arquivos_do_projeto() -> list[str]:
    """Arquivos versionados no git (ou, sem git, os arquivos da pasta menos os ignoráveis)."""
    try:
        saida = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=RAIZ, capture_output=True, check=True).stdout
        nomes = [n for n in saida.decode("utf-8").split("\0") if n]
    except (OSError, subprocess.CalledProcessError):
        nomes = []
        for pasta, subpastas, arquivos in os.walk(RAIZ):
            subpastas[:] = [d for d in subpastas if not d.startswith(".") and d not in ("__pycache__", ".venv", "dist")]
            for a in arquivos:
                nomes.append(Path(pasta, a).relative_to(RAIZ).as_posix())
    return sorted(n for n in nomes
                  if not n.startswith(IGNORAR_PREFIXOS) and Path(n).name not in IGNORAR_NOMES
                  and "__pycache__" not in n and (RAIZ / n).is_file())


def conteudo_esperado() -> dict[str, tuple[bytes, int]]:
    """{nome no zip: (conteúdo, permissão)} do projeto atual."""
    esperado = {}
    for nome in arquivos_do_projeto():
        caminho = RAIZ / nome
        executavel = os.access(caminho, os.X_OK) or nome.endswith((".sh", ".command"))
        esperado[f"{NOME}/{nome}"] = (caminho.read_bytes(), 0o100755 if executavel else 0o100644)
    return esperado


def conteudo_do_zip(caminho: Path) -> dict[str, tuple[bytes, int]] | None:
    if not caminho.exists():
        return None
    try:
        with zipfile.ZipFile(caminho) as z:
            return {i.filename: (z.read(i), i.external_attr >> 16) for i in z.infolist()}
    except zipfile.BadZipFile:
        return None


def montar_zip(esperado: dict[str, tuple[bytes, int]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        for nome, (dados, modo) in esperado.items():
            info = zipfile.ZipInfo(nome, date_time=DATA_FIXA)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = modo << 16
            z.writestr(info, dados)
    return buf.getvalue()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true", help="só confere se o zip está atualizado")
    args = ap.parse_args(argv)

    esperado = conteudo_esperado()
    em_dia = conteudo_do_zip(DESTINO) == esperado     # compara o conteúdo, não os bytes comprimidos

    if args.check:
        if em_dia:
            print(f"OK: {DESTINO.relative_to(RAIZ)} está atualizado.")
            return 0
        print(f"DESATUALIZADO: rode 'python scripts/gerar_zip.py' e faça commit de {DESTINO.relative_to(RAIZ)}.",
              file=sys.stderr)
        return 1

    if em_dia:
        print(f"{DESTINO.relative_to(RAIZ)} já estava atualizado.")
        return 0
    novo = montar_zip(esperado)
    DESTINO.parent.mkdir(parents=True, exist_ok=True)
    DESTINO.write_bytes(novo)
    print(f"Gerado {DESTINO.relative_to(RAIZ)} ({len(novo) / 1024:.0f} KB, {len(esperado)} arquivos).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
