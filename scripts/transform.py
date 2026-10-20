"""
HealthData-Pipeline: Transform Script (Validação e Limpeza)
============================================================

Lê os Parquets brutos de data/, aplica validações e limpezas e grava
Delta Lakes em data_delta/, uma tabela por fonte.

Input (data/):
    ibge_localidades/  -> estados + municipios
    ibge_sidra/        -> populacao estimada por UF
    sim/               -> mortalidade por UF (DO<Uf>2023)
    cnes/              -> estabelecimentos por UF (EF<Uf>202312)

Output (data_delta/):
    dim_estados/       -> dimensão de UFs (limpa)
    dim_municipios/    -> dimensão de municípios (colunas renomeadas)
    populacao_uf/      -> população por UF (schema renomeado)
    mortalidade/       -> todas as UFs combinadas, datas e idades parseadas
    estabelecimentos/  -> todas as UFs combinadas, campos tipados

Validações aplicadas:
    - dim_estados: 27 UFs, sem nulos, siglas únicas
    - dim_municipios: ~5570 municípios, IDs únicos, UF válida
    - populacao_uf: 27 UFs, população > 0, sem duplicadas
    - mortalidade: DTOBITO em 2023, SEXO em {0,1,2}, CODMUNOCOR com 6 dígitos
    - estabelecimentos: CNES não vazio, CODUFMUN válido
"""

import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

# deltalake (delta-rs): escreve Delta sem Spark.
# Nota: o rename nao funciona no FUSE mount do /Workspace, entao gravamos
# em um diretorio temporario e copiamos os arquivos para data_delta/.
try:
    from deltalake import WriterProperties, write_deltalake
except ImportError:
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "deltalake", "--quiet"],
        check=True,
    )
    from deltalake import WriterProperties, write_deltalake

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

def _find_base_dir() -> Path:
    """Localiza a raiz do projeto (HealthData-Pipeline) sem hardcode de usuario."""
    marker = Path("scripts") / "transform.py"
    # 1. Execucao direta do arquivo: scripts/.. e a raiz
    try:
        p = Path(__file__).resolve().parent.parent
        if (p / marker).is_file():
            return p
    except NameError:
        pass
    # 2. Sobe a partir do diretorio atual procurando a pasta do projeto
    #    (funciona de qualquer subdiretorio do workspace do usuario, sem
    #    precisar saber o email — o projeto e filho de um ancestral do cwd)
    for cand in [Path.cwd(), *Path.cwd().parents]:
        if cand.name == "HealthData-Pipeline" and (cand / marker).is_file():
            return cand
        proj = cand / "HealthData-Pipeline"
        if (proj / marker).is_file():
            return proj
    raise RuntimeError("Nao foi possivel localizar a raiz do projeto HealthData-Pipeline")

BASE_DIR = _find_base_dir()

RAW_DIR = BASE_DIR / "data"
DELTA_DIR = BASE_DIR / "data_delta"

SIM_YEAR = 2023
CNES_YEAR = 2023
CNES_MONTH = 12

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class ValidationReport:
    """Acumula resultados de validação por tabela."""

    def __init__(self, table: str):
        self.table = table
        self.checks: list[tuple[str, bool, str]] = []

    def check(self, name: str, condition: bool, detail: str = "") -> bool:
        ok = bool(condition)
        self.checks.append((name, ok, detail))
        mark = "OK " if ok else "FAIL"
        print(f"    [{mark}] {name}" + (f" — {detail}" if detail else ""))
        return ok

    @property
    def passed(self) -> bool:
        return all(ok for _, ok, _ in self.checks)


def write_delta(df: pd.DataFrame, name: str) -> None:
    """Grava um DataFrame pandas como Delta Lake em data_delta/<name>/.

    Estrategia: grava em diretorio temporario (rename permitido) e depois
    copia a arvore de arquivos para data_delta/ (escrita simples funciona no FUSE).
    """
    # Colunas 100% nulas com dtype object geram tipo arrow "null", invalido no
    # Delta Lake. Converter para string nullable antes de gravar.
    for c in df.columns:
        if df[c].isna().all() and df[c].dtype == object:
            df[c] = df[c].astype("string")

    out = DELTA_DIR / name
    # zstd: ~21% menor que snappy nestes dados (nivel 19 chega a ~31%, mas grava mais lento)
    props = WriterProperties(compression="zstd")
    with tempfile.TemporaryDirectory(prefix="delta_") as tmp:
        tmp_path = Path(tmp) / name
        write_deltalake(str(tmp_path), df, mode="overwrite", writer_properties=props)
        if out.exists():
            shutil.rmtree(out)
        shutil.copytree(tmp_path, out)
    print(f"    Delta salvo: {out}  ({len(df)} rows, {len(df.columns)} cols)")


def empty_to_na(df: pd.DataFrame) -> pd.DataFrame:
    """Converte strings vazias e placeholders DataSUS em None."""
    return df.replace({"": None, "0": None, 0: None, "0000": None, "000000": None})


def parse_datasus_date(series: pd.Series) -> pd.Series:
    """Converte datas DataSUS (DDMMAAAA string) para datetime, inválidas viram NaT."""
    def _parse(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return pd.NaT
        s = str(v).strip()
        if len(s) != 8 or not s.isdigit():
            return pd.NaT
        try:
            return datetime.strptime(s, "%d%m%Y")
        except ValueError:
            return pd.NaT
    return series.apply(_parse)


# ---------------------------------------------------------------------------
# 1. dim_estados
# ---------------------------------------------------------------------------

def transform_estados() -> None:
    print("\n=== dim_estados ===")
    df = pd.read_parquet(RAW_DIR / "ibge_localidades" / "estados.parquet")
    rpt = ValidationReport("dim_estados")

    rpt.check("27 UFs", len(df) == 27, f"{len(df)} rows")
    rpt.check("sem nulos", df[["id", "sigla", "nome", "regiao_nome"]].notna().all().all())
    rpt.check("siglas únicas", df["sigla"].is_unique)
    rpt.check("siglas com 2 chars", (df["sigla"].str.len() == 2).all())

    df = df[["id", "sigla", "nome", "regiao_id", "regiao_sigla", "regiao_nome"]]
    df = df.sort_values("id").reset_index(drop=True)
    if rpt.passed:
        write_delta(df, "dim_estados")
    else:
        print("    SKIP: validação falhou")
    return rpt


# ---------------------------------------------------------------------------
# 2. dim_municipios
# ---------------------------------------------------------------------------

def transform_municipios() -> None:
    print("\n=== dim_municipios ===")
    df = pd.read_parquet(RAW_DIR / "ibge_localidades" / "municipios.parquet")
    rpt = ValidationReport("dim_municipios")

    rpt.check("~5570 municípios", 5500 <= len(df) <= 5600, f"{len(df)} rows")
    rpt.check("IDs únicos", df["id"].is_unique)
    rpt.check("sem nulos no nome", df["nome"].notna().all())

    # Limpeza: achatar 'regiao-imediata' (dict não achatado no extract)
    if "regiao-imediata" in df.columns:
        ri = pd.json_normalize(df["regiao-imediata"]).add_prefix("regiao_imediata_")
        df = df.drop(columns=["regiao-imediata"]).reset_index(drop=True).join(ri)

    # Renomear colunas com pontos/maiúsculas: microrregiao_mesorregiao.UF.id -> uf_id
    rename = {}
    for c in df.columns:
        new = re.sub(r"^microrregiao_mesorregiao\.UF\.", "uf_", c)
        new = re.sub(r"^microrregiao_mesorregiao\.", "mesorregiao_", new)
        new = re.sub(r"^regiao_imediata_regiao-intermediaria\.", "regiao_intermediaria_", new)
        new = new.replace(".id", "_id").replace(".nome", "_nome").replace(".sigla", "_sigla")
        rename[c] = re.sub(r"[^0-9a-zA-Z_]", "_", new)
    df = df.rename(columns=rename)

    # Converter IDs float (por json_normalize com nulos) para Int64
    for c in df.columns:
        if c.endswith("_id") and df[c].dtype == "float64":
            df[c] = df[c].astype("Int64")

    rpt.check("colunas renomeadas sem pontos", not any("." in c for c in df.columns))

    if rpt.passed:
        write_delta(df, "dim_municipios")
    else:
        print("    SKIP: validação falhou")
    return rpt


# ---------------------------------------------------------------------------
# 3. populacao_uf (SIDRA)
# ---------------------------------------------------------------------------

def transform_populacao() -> None:
    print("\n=== populacao_uf ===")
    src = next((RAW_DIR / "ibge_sidra").glob("populacao_estimada_*.parquet"))
    df = pd.read_parquet(src)
    rpt = ValidationReport("populacao_uf")

    # Limpeza: renomear colunas SIDRA para nomes amigáveis
    df = df.rename(columns={
        "D1C": "uf_codigo", "D1N": "uf_nome", "V": "populacao", "D3N": "ano",
    })
    df = df[["uf_codigo", "uf_nome", "populacao", "ano"]].copy()
    df["uf_codigo"] = df["uf_codigo"].astype("int64")
    df["populacao"] = df["populacao"].astype("int64")
    df["ano"] = df["ano"].astype("int64")

    rpt.check("27 UFs", len(df) == 27, f"{len(df)} rows")
    rpt.check("UFs únicas", df["uf_codigo"].is_unique)
    rpt.check("população > 0", (df["populacao"] > 0).all())
    rpt.check("ano único", df["ano"].nunique() == 1, f"ano={df['ano'].iloc[0]}")
    rpt.check("soma plausível (> 200M)", df["populacao"].sum() > 200_000_000,
              f"total={df['populacao'].sum():,}")

    df = df.sort_values("uf_codigo").reset_index(drop=True)
    if rpt.passed:
        write_delta(df, "populacao_uf")
    else:
        print("    SKIP: validação falhou")
    return rpt


# ---------------------------------------------------------------------------
# 4. mortalidade (SIM — todas as UFs)
# ---------------------------------------------------------------------------

# Codificacao empiricamente validada do campo IDADE do SIM (SP 2023, 100% match
# contra DTOBITO-DTNASC): 1o digito = unidade, 2 ultimos = valor.
IDADE_TIPO = {
    "0": "minutos",    # < 1 hora de vida
    "1": "horas",      # < 1 dia
    "2": "dias",       # < 1 mes
    "3": "meses",      # < 1 ano
    "4": "anos",       # 1 a 99 anos (97.6% dos registros)
    "5": "anos_100",   # 100+ anos (valor + 100)
    "9": "ignorado",
}
# Fator de conversao para anos (aproximado)
IDADE_FATOR_ANOS = {
    "minutos": 1 / (60 * 24 * 365.25),
    "horas": 1 / (24 * 365.25),
    "dias": 1 / 365.25,
    "meses": 1 / 12,
    "anos": 1.0,
    "anos_100": 1.0,
}
SEXO_MAP = {"0": "ignorado", "1": "masculino", "2": "feminino"}


def transform_mortalidade() -> None:
    print("\n=== mortalidade (SIM) ===")
    sim_dir = RAW_DIR / "sim"
    frames = []
    for f in sorted(sim_dir.glob(f"DO*{SIM_YEAR}.parquet")):
        uf = f.stem.replace("DO", "").replace(str(SIM_YEAR), "")
        d = pd.read_parquet(f)
        d["uf"] = uf
        frames.append(d)
        print(f"    lido {f.name}: {len(d)} rows")
    df = pd.concat(frames, ignore_index=True)
    rpt = ValidationReport("mortalidade")

    # --- Limpeza ---
    # Datas DDMMYYYY -> datetime
    for col in ["DTOBITO", "DTNASC", "DTCADASTRO", "DTATESTADO"]:
        if col in df.columns:
            df[col] = parse_datasus_date(df[col])

    # IDADE (formato DataSUS: 1o digito = unidade, 2 ultimos = valor)
    def _parse_idade(v):
        s = str(v).strip() if v is not None else ""
        if len(s) != 3 or not s.isdigit():
            return ("ignorado", None)
        tipo = IDADE_TIPO.get(s[0], "ignorado")
        try:
            valor = int(s[1:])
        except ValueError:
            return ("ignorado", None)
        if s[0] == "5":
            valor += 100  # 5XX = 100 + XX anos
        return (tipo, valor)
    parsed = df["IDADE"].apply(_parse_idade)
    df["idade_tipo"] = parsed.apply(lambda t: t[0])
    df["idade_valor"] = parsed.apply(lambda t: t[1])
    # idade_anos: idade aproximada em anos (float), util para analise
    df["idade_anos"] = df.apply(
        lambda r: round(r["idade_valor"] * IDADE_FATOR_ANOS[r["idade_tipo"]], 4)
        if r["idade_valor"] is not None and r["idade_tipo"] != "ignorado" else None,
        axis=1,
    )
    df = df.drop(columns=["IDADE"])

    # SEXO código -> descrição
    df["sexo"] = df["SEXO"].map(SEXO_MAP).fillna("ignorado")
    df = df.drop(columns=["SEXO"])

    # Códigos numéricos
    for col in ["CODMUNOCOR", "CODMUNRES", "CODMUNNATU", "CODESTAB"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # HORAOBITO: HHMM -> int
    df["HORAOBITO"] = pd.to_numeric(df["HORAOBITO"], errors="coerce").astype("Int64")

    # Strings vazias -> None nos campos de texto principais
    for col in ["CAUSABAS", "CAUSABAS_O", "LINHAA", "NATURAL", "OCUP", "ESTABDESCR"]:
        if col in df.columns:
            df[col] = df[col].replace("", None)

    # --- Validações ---
    dt_ok = df["DTOBITO"].notna().mean()
    rpt.check("DTOBITO parseável >= 99%", dt_ok >= 0.99, f"{dt_ok:.2%}")
    in_year = (df["DTOBITO"].dt.year == SIM_YEAR).mean()
    rpt.check(f"DTOBITO em {SIM_YEAR} >= 95%", in_year >= 0.95, f"{in_year:.2%}")
    mun_ok = df["CODMUNOCOR"].dropna().astype(str).str.len().eq(6).mean()
    rpt.check("CODMUNOCOR com 6 dígitos >= 99%", mun_ok >= 0.99, f"{mun_ok:.2%}")
    rpt.check("causa básica preenchida >= 99%", df["CAUSABAS"].notna().mean() >= 0.99,
              f"{df['CAUSABAS'].notna().mean():.2%}")
    rpt.check("27 UFs presentes", df["uf"].nunique() == 27, f"{df['uf'].nunique()} UFs")
    # consistencia idade_anos vs idade derivada de DTNASC->DTOBITO (tolerancia 1 ano)
    idade_derivada = ((df["DTOBITO"] - df["DTNASC"]).dt.days / 365.25).round(1)
    both = df["idade_anos"].notna() & idade_derivada.notna()
    if both.any():
        consist = ((df["idade_anos"] - idade_derivada).abs() <= 1)[both].mean()
        rpt.check("idade_anos consistente c/ DTNASC (±1 ano) >= 95%", consist >= 0.95,
                  f"{consist:.2%} em {both.sum()} registros")
    rpt.check("sem CONTADOR duplicado", df["CONTADOR"].is_unique if "CONTADOR" in df.columns else True)

    if rpt.passed:
        write_delta(df, "mortalidade")
    else:
        print("    SKIP: validação falhou")
    return rpt


# ---------------------------------------------------------------------------
# 5. estabelecimentos (CNES — todas as UFs)
# ---------------------------------------------------------------------------

def transform_estabelecimentos() -> None:
    print("\n=== estabelecimentos (CNES) ===")
    cnes_dir = RAW_DIR / "cnes"
    frames = []
    for f in sorted(cnes_dir.glob(f"EF*{CNES_YEAR}{CNES_MONTH:02d}.parquet")):
        uf = f.stem.replace("EF", "").replace(f"{CNES_YEAR}{CNES_MONTH:02d}", "")
        d = pd.read_parquet(f)
        d["uf"] = uf
        frames.append(d)
        print(f"    lido {f.name}: {len(d)} rows")
    df = pd.concat(frames, ignore_index=True)
    rpt = ValidationReport("estabelecimentos")

    # --- Limpeza ---
    # CNES e códigos numéricos
    for col in ["CNES", "CODUFMUN", "REGSAUDE", "CPF_CNPJ"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Strings vazias -> None nos classificadores
    for col in ["TPGESTAO", "ATIVIDAD", "CLIENTEL", "TP_UNID", "TURNO_AT",
                "NIV_HIER", "TERCEIRO", "VINC_SUS", "TP_PREST", "SGRUPHAB",
                "ESFERA_A", "NATUREZA", "NAT_JUR"]:
        if col in df.columns:
            df[col] = df[col].replace("", None)

    # Competência (AAAAMM) -> date (primeiro dia do mês)
    if "COMPETEN" in df.columns:
        def _comp(v):
            s = str(v).strip()
            if len(s) == 6 and s.isdigit():
                try:
                    return datetime(int(s[:4]), int(s[4:6]), 1)
                except ValueError:
                    return None
            return None
        df["competencia"] = df["COMPETEN"].apply(_comp)
        df = df.drop(columns=["COMPETEN"])

    # --- Validações ---
    rpt.check("CNES preenchido 100%", df["CNES"].notna().all())
    rpt.check("CODUFMUN com 6 dígitos",
             df["CODUFMUN"].dropna().astype(str).str.len().eq(6).all())
    dup = df.duplicated(subset=["CNES", "uf"])
    rpt.check("CNES único por UF", not dup.any(), f"{dup.sum()} duplicados")
    rpt.check("competência = %d-%02d" % (CNES_YEAR, CNES_MONTH),
             (df["competencia"] == datetime(CNES_YEAR, CNES_MONTH, 1)).all())

    if rpt.passed:
        write_delta(df, "estabelecimentos")
    else:
        print("    SKIP: validação falhou")
    return rpt


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("HealthData-Pipeline: Transform (Validação e Limpeza)")
    print(f"Raw:      {RAW_DIR}")
    print(f"Delta:    {DELTA_DIR}")
    print("=" * 60)

    DELTA_DIR.mkdir(parents=True, exist_ok=True)

    steps = [
        ("dim_estados",       transform_estados),
        ("dim_municipios",    transform_municipios),
        ("populacao_uf",      transform_populacao),
        ("mortalidade",       transform_mortalidade),
        ("estabelecimentos",  transform_estabelecimentos),
    ]

    results = {}
    for name, func in steps:
        try:
            rpt = func()
            if rpt is None or rpt.passed:
                results[name] = "OK"
            else:
                failed = sum(1 for _, ok, _ in rpt.checks if not ok)
                results[name] = f"SKIP ({failed} checks falharam)"
        except Exception as exc:
            results[name] = f"ERRO: {exc}"
            print(f"\n  ERRO em {name}: {exc}")

    print("\n" + "=" * 60)
    print("Transform Summary")
    print("-" * 60)
    for name, status in results.items():
        print(f"  {name:<20s} {status}")
    print("=" * 60)


if __name__ == "__main__":
    main()
