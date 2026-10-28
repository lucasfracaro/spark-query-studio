import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "gerar_zip", Path(__file__).resolve().parent.parent / "scripts" / "gerar_zip.py")
gerar_zip = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gerar_zip)


def test_conteudo_do_zip():
    esperado = gerar_zip.conteudo_esperado()
    nomes = set(esperado)
    assert "spark-query-studio/app.py" in nomes and "spark-query-studio/LICENSE" in nomes
    assert not any(n.startswith(("spark-query-studio/dist/", "spark-query-studio/.github/")) for n in nomes)
    assert "spark-query-studio/.gitignore" not in nomes
    assert esperado["spark-query-studio/run.sh"][1] == 0o100755


def test_zip_gerado_tem_o_conteudo_esperado(tmp_path):
    esperado = gerar_zip.conteudo_esperado()
    destino = tmp_path / "x.zip"
    destino.write_bytes(gerar_zip.montar_zip(esperado))
    assert gerar_zip.conteudo_do_zip(destino) == esperado
    # determinístico: gerar de novo produz os mesmos bytes
    assert gerar_zip.montar_zip(esperado) == destino.read_bytes()


def test_zip_inexistente_ou_corrompido(tmp_path):
    assert gerar_zip.conteudo_do_zip(tmp_path / "nao_existe.zip") is None
    ruim = tmp_path / "ruim.zip"
    ruim.write_bytes(b"isso nao e um zip")
    assert gerar_zip.conteudo_do_zip(ruim) is None
