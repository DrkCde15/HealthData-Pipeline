"""
HealthData-Pipeline: Analytics Script (Camada Gold)
=====================================================

Lê os Delta Lakes de data_delta/ (camada silver), agrega em tabelas analíticas
(camada gold) e grava de volta em data_delta/, com validações de integridade.

Input (data_delta/ — silver):
    mortalidade/    -> óbitos 2023 (27 UFs, datas/idades parseadas)
    populacao_uf/   -> população por UF (2022, SIDRA)
    dim_estados/    -> dimensão de UFs (nome, região)

Output (data_delta/ — gold):
    obitos_uf_mes/          -> óbitos por UF x mês
    taxa_mortalidade_uf/    -> óbitos, população e taxa por 100 mil habitantes
    obitos_capitulo_cid10/  -> óbitos por UF x capítulo CID-10
    obitos_causa_cid10/     -> óbitos por causa básica (CID-10 exato, nacional)
    obitos_faixa_etaria/    -> óbitos por UF x sexo x faixa etária

Validações:
    - Soma de óbitos de cada agregação == total da tabela mortalidade
    - obitos_uf_mes: 27 UFs x 12 meses, meses 1-12
    - taxa_mortalidade_uf: 27 UFs, taxa entre 300 e 1200 por 100k

Nota: população de 2022 (SIDRA p/last) x óbitos de 2023 -> taxa bruta
aproximada de mortalidade por 100 mil habitantes.
"""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

try:
    from deltalake import DeltaTable, WriterProperties, write_deltalake
except ImportError:
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "deltalake", "--quiet"],
        check=True,
    )
    from deltalake import DeltaTable, WriterProperties, write_deltalake

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

def _find_base_dir() -> Path:
    """Localiza a raiz do projeto (HealthData-Pipeline) sem hardcode de usuario."""
    marker = Path("scripts") / "analytics.py"
    try:
        p = Path(__file__).resolve().parent.parent
        if (p / marker).is_file():
            return p
    except NameError:
        pass
    for cand in [Path.cwd(), *Path.cwd().parents]:
        if cand.name == "HealthData-Pipeline" and (cand / marker).is_file():
            return cand
        proj = cand / "HealthData-Pipeline"
        if (proj / marker).is_file():
            return proj
    raise RuntimeError("Nao foi possivel localizar a raiz do projeto HealthData-Pipeline")

BASE_DIR = _find_base_dir()
DELTA_DIR = BASE_DIR / "data_delta"

# ---------------------------------------------------------------------------
# Helpers (mesmos padroes do transform.py)
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
    """Grava um DataFrame pandas como Delta Lake (zstd) em data_delta/<name>/."""
    for c in df.columns:
        if df[c].isna().all() and df[c].dtype == object:
            df[c] = df[c].astype("string")

    out = DELTA_DIR / name
    props = WriterProperties(compression="zstd")
    with tempfile.TemporaryDirectory(prefix="delta_") as tmp:
        tmp_path = Path(tmp) / name
        write_deltalake(str(tmp_path), df, mode="overwrite", writer_properties=props)
        if out.exists():
            shutil.rmtree(out)
        shutil.copytree(tmp_path, out)
    print(f"    Delta salvo: {out}  ({len(df)} rows, {len(df.columns)} cols)")


def read_silver(name: str) -> pd.DataFrame:
    """Lê uma tabela silver de data_delta/."""
    return DeltaTable(str(DELTA_DIR / name)).to_pandas()


# ---------------------------------------------------------------------------
# Domínio: CID-10 (capítulos) e faixas etárias
# ---------------------------------------------------------------------------

# (letra_ini, num_ini, letra_fim, num_fim, nome do capítulo)
CID10_CAPITULOS = [
    ("A", 0, "B", 99, "I. Doenças infecciosas e parasitárias"),
    ("C", 0, "D", 48, "II. Neoplasias (tumores)"),
    ("D", 50, "D", 89, "III. Doenças do sangue e órgãos hematopoiéticos"),
    ("E", 0, "E", 90, "IV. Endócrinas, nutricionais e metabólicas"),
    ("F", 0, "F", 99, "V. Transtornos mentais e comportamentais"),
    ("G", 0, "G", 99, "VI. Doenças do sistema nervoso"),
    ("H", 0, "H", 59, "VII. Doenças do olho e anexos"),
    ("H", 60, "H", 95, "VIII. Doenças do ouvido e apêndice mastoide"),
    ("I", 0, "I", 99, "IX. Doenças do aparelho circulatório"),
    ("J", 0, "J", 99, "X. Doenças do aparelho respiratório"),
    ("K", 0, "K", 93, "XI. Doenças do aparelho digestivo"),
    ("L", 0, "L", 99, "XII. Doenças da pele e tecido subcutâneo"),
    ("M", 0, "M", 99, "XIII. Doenças osteomusculares e tecido conjuntivo"),
    ("N", 0, "N", 99, "XIV. Doenças do aparelho geniturinário"),
    ("O", 0, "O", 99, "XV. Gravidez, parto e puerpério"),
    ("P", 0, "P", 96, "XVI. Afecções originadas no período perinatal"),
    ("Q", 0, "Q", 99, "XVII. Malformações congênitas"),
    ("R", 0, "R", 99, "XVIII. Sintomas, sinais e achados anormais"),
    ("S", 0, "T", 98, "XIX. Lesões, envenenamentos e causas externas"),
    ("V", 1, "Y", 98, "XX. Causas externas de morbidade e mortalidade"),
    ("Z", 0, "Z", 99, "XXI. Contato com serviços de saúde"),
    ("U", 0, "U", 99, "XXII. Códigos para fins especiais"),
]


def cid10_capitulo(causa) -> str:
    """Mapeia um código CID-10 (ex.: 'I219') para o nome do capítulo."""
    s = str(causa).strip().upper() if causa is not None else ""
    if len(s) < 3 or not s[0].isalpha() or not s[1:3].isdigit():
        return "ignorado"
    letra, num = s[0], int(s[1:3])
    for l1, n1, l2, n2, nome in CID10_CAPITULOS:
        if (l1, n1) <= (letra, num) <= (l2, n2):
            return nome
    return "ignorado"


# (limite inferior inclusive, limite superior exclusive, rótulo)
FAIXAS_ETARIAS = [
    (0, 1, "<1 ano"), (1, 5, "1-4"), (5, 10, "5-9"), (10, 15, "10-14"),
    (15, 20, "15-19"), (20, 25, "20-24"), (25, 30, "25-29"), (30, 35, "30-34"),
    (35, 40, "35-39"), (40, 45, "40-44"), (45, 50, "45-49"), (50, 55, "50-54"),
    (55, 60, "55-59"), (60, 65, "60-64"), (65, 70, "65-69"), (70, 75, "70-74"),
    (75, 80, "75-79"),
]
FAIXA_ORDEM = [f[2] for f in FAIXAS_ETARIAS] + ["80+", "ignorado"]


def faixa_etaria(idade_anos) -> str:
    """Classifica idade_anos na faixa etária (padrão DATASUS); None -> ignorado."""
    if idade_anos is None or pd.isna(idade_anos):
        return "ignorado"
    for lo, hi, label in FAIXAS_ETARIAS:
        if lo <= idade_anos < hi:
            return label
    if idade_anos >= 80:
        return "80+"
    return "ignorado"


# ---------------------------------------------------------------------------
# 1. obitos_uf_mes
# ---------------------------------------------------------------------------

def analytics_obitos_uf_mes(mort: pd.DataFrame, est: pd.DataFrame, total: int):
    print("\n=== obitos_uf_mes ===")
    rpt = ValidationReport("obitos_uf_mes")

    m = mort[["uf", "DTOBITO"]].dropna(subset=["DTOBITO"]).copy()
    m["mes"] = m["DTOBITO"].dt.month
    df = m.groupby(["uf", "mes"], as_index=False).size().rename(columns={"size": "obitos"})
    df = df.merge(
        est[["sigla", "nome", "regiao_nome"]].rename(
            columns={"sigla": "uf", "nome": "uf_nome", "regiao_nome": "regiao"}),
        on="uf",
    )
    df = df[["uf", "uf_nome", "regiao", "mes", "obitos"]].sort_values(["uf", "mes"]).reset_index(drop=True)

    rpt.check("27 UFs x 12 meses (324 rows)", len(df) == 324, f"{len(df)} rows")
    rpt.check("soma == total de óbitos", df["obitos"].sum() == total,
              f"{df['obitos'].sum():,} == {total:,}")
    rpt.check("meses 1-12", set(df["mes"]) == set(range(1, 13)))

    if rpt.passed:
        write_delta(df, "obitos_uf_mes")
    else:
        print("    SKIP: validação falhou")
    return rpt


# ---------------------------------------------------------------------------
# 2. taxa_mortalidade_uf
# ---------------------------------------------------------------------------

def analytics_taxa_mortalidade(mort: pd.DataFrame, pop: pd.DataFrame,
                               est: pd.DataFrame, total: int):
    print("\n=== taxa_mortalidade_uf ===")
    rpt = ValidationReport("taxa_mortalidade_uf")

    ob = mort.groupby("uf", as_index=False).size().rename(columns={"size": "obitos"})
    df = (
        est[["id", "sigla", "nome", "regiao_nome"]]
        .merge(ob, left_on="sigla", right_on="uf", how="left")
        .drop(columns=["uf"])
        .merge(pop[["uf_codigo", "populacao"]], left_on="id", right_on="uf_codigo")
        .drop(columns=["uf_codigo", "id"])
        .rename(columns={"sigla": "uf", "nome": "uf_nome", "regiao_nome": "regiao"})
    )
    df["taxa_por_100k"] = (df["obitos"] / df["populacao"] * 100_000).round(1)
    df = df[["uf", "uf_nome", "regiao", "populacao", "obitos", "taxa_por_100k"]]
    df = df.sort_values("taxa_por_100k", ascending=False).reset_index(drop=True)

    rpt.check("27 UFs", len(df) == 27, f"{len(df)} rows")
    rpt.check("soma de óbitos == total", df["obitos"].sum() == total,
              f"{df['obitos'].sum():,} == {total:,}")
    taxa_ok = df["taxa_por_100k"].between(300, 1200).all()
    rpt.check("taxa entre 300 e 1200 por 100k", taxa_ok,
              f"min={df['taxa_por_100k'].min()}, max={df['taxa_por_100k'].max()}")

    if rpt.passed:
        write_delta(df, "taxa_mortalidade_uf")
    else:
        print("    SKIP: validação falhou")
    return rpt, df


# ---------------------------------------------------------------------------
# 3. obitos_capitulo_cid10
# ---------------------------------------------------------------------------

def analytics_capitulo_cid10(mort: pd.DataFrame, est: pd.DataFrame, total: int):
    print("\n=== obitos_capitulo_cid10 ===")
    rpt = ValidationReport("obitos_capitulo_cid10")

    c = mort[["uf", "CAUSABAS"]].copy()
    c["capitulo"] = c["CAUSABAS"].map(cid10_capitulo)
    df = c.groupby(["uf", "capitulo"], as_index=False).size().rename(columns={"size": "obitos"})
    df = df.merge(
        est[["sigla", "nome", "regiao_nome"]].rename(
            columns={"sigla": "uf", "nome": "uf_nome", "regiao_nome": "regiao"}),
        on="uf",
    )
    df = df[["uf", "uf_nome", "regiao", "capitulo", "obitos"]]
    df = df.sort_values(["uf", "obitos"], ascending=[True, False]).reset_index(drop=True)

    rpt.check("27 UFs", df["uf"].nunique() == 27, f"{df['uf'].nunique()} UFs")
    rpt.check("soma == total de óbitos", df["obitos"].sum() == total,
              f"{df['obitos'].sum():,} == {total:,}")

    if rpt.passed:
        write_delta(df, "obitos_capitulo_cid10")
    else:
        print("    SKIP: validação falhou")
    return rpt, df


# ---------------------------------------------------------------------------
# 4. obitos_causa_cid10 (nacional, CID exato — fonte do top 10)
# ---------------------------------------------------------------------------

def analytics_causa_cid10(mort: pd.DataFrame, total: int):
    print("\n=== obitos_causa_cid10 ===")
    rpt = ValidationReport("obitos_causa_cid10")

    df = mort["CAUSABAS"].value_counts().reset_index()
    df.columns = ["causa", "obitos"]
    df["capitulo"] = df["causa"].map(cid10_capitulo)
    df["pct"] = (df["obitos"] / total * 100).round(2)
    df = df[["causa", "capitulo", "obitos", "pct"]].reset_index(drop=True)

    rpt.check("soma == total de óbitos", df["obitos"].sum() == total,
              f"{df['obitos'].sum():,} == {total:,}")
    rpt.check("top causa plausível (< 15% do total)", df["obitos"].iloc[0] / total < 0.15,
              f"top={df['causa'].iloc[0]} ({df['pct'].iloc[0]}%)")

    if rpt.passed:
        write_delta(df, "obitos_causa_cid10")
    else:
        print("    SKIP: validação falhou")
    return rpt, df


# ---------------------------------------------------------------------------
# 5. obitos_faixa_etaria
# ---------------------------------------------------------------------------

def analytics_faixa_etaria(mort: pd.DataFrame, est: pd.DataFrame, total: int):
    print("\n=== obitos_faixa_etaria ===")
    rpt = ValidationReport("obitos_faixa_etaria")

    f = mort[["uf", "sexo", "idade_anos"]].copy()
    f["faixa_etaria"] = f["idade_anos"].map(faixa_etaria)
    df = f.groupby(["uf", "sexo", "faixa_etaria"], as_index=False).size().rename(columns={"size": "obitos"})
    df = df.merge(
        est[["sigla", "nome", "regiao_nome"]].rename(
            columns={"sigla": "uf", "nome": "uf_nome", "regiao_nome": "regiao"}),
        on="uf",
    )
    # ordena faixas na ordem cronologica correta
    df["faixa_etaria"] = pd.Categorical(df["faixa_etaria"], categories=FAIXA_ORDEM, ordered=True)
    df = df[["uf", "uf_nome", "regiao", "sexo", "faixa_etaria", "obitos"]]
    df = df.sort_values(["uf", "sexo", "faixa_etaria"]).reset_index(drop=True)
    df["faixa_etaria"] = df["faixa_etaria"].astype(str)

    rpt.check("27 UFs", df["uf"].nunique() == 27, f"{df['uf'].nunique()} UFs")
    rpt.check("soma == total de óbitos", df["obitos"].sum() == total,
              f"{df['obitos'].sum():,} == {total:,}")
    faixas_ok = set(df["faixa_etaria"]).issubset(set(FAIXA_ORDEM))
    rpt.check("faixas válidas", faixas_ok, f"{df['faixa_etaria'].nunique()} faixas")

    if rpt.passed:
        write_delta(df, "obitos_faixa_etaria")
    else:
        print("    SKIP: validação falhou")
    return rpt


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("HealthData-Pipeline: Analytics (Camada Gold)")
    print(f"Delta:    {DELTA_DIR}")
    print("=" * 60)

    mort = read_silver("mortalidade")
    pop = read_silver("populacao_uf")
    est = read_silver("dim_estados")
    total = len(mort)
    print(f"Silver: mortalidade={total:,} rows, populacao_uf={len(pop)}, dim_estados={len(est)}")

    steps = [
        ("obitos_uf_mes",       lambda: analytics_obitos_uf_mes(mort, est, total)),
        ("taxa_mortalidade_uf", lambda: analytics_taxa_mortalidade(mort, pop, est, total)),
        ("obitos_capitulo_cid10", lambda: analytics_capitulo_cid10(mort, est, total)),
        ("obitos_causa_cid10",  lambda: analytics_causa_cid10(mort, total)),
        ("obitos_faixa_etaria", lambda: analytics_faixa_etaria(mort, est, total)),
    ]

    results = {}
    dfs = {}  # dataframes para o resumo analítico
    for name, func in steps:
        try:
            out = func()
            rpt = out[0] if isinstance(out, tuple) else out
            if isinstance(out, tuple):
                dfs[name] = out[1]
            if rpt is None or rpt.passed:
                results[name] = "OK"
            else:
                failed = sum(1 for _, ok, _ in rpt.checks if not ok)
                results[name] = f"SKIP ({failed} checks falharam)"
        except Exception as exc:
            results[name] = f"ERRO: {exc}"
            print(f"\n  ERRO em {name}: {exc}")

    # ------------------------------------------------------------------
    # Resumo analítico (usa só tabelas validadas)
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("Analytics Summary")
    print("-" * 60)
    for name, status in results.items():
        print(f"  {name:<24s} {status}")

    if "obitos_causa_cid10" in dfs and results.get("obitos_causa_cid10") == "OK":
        print("\nTop 10 causas básicas (CID-10) — Brasil 2023:")
        for _, r in dfs["obitos_causa_cid10"].head(10).iterrows():
            print(f"  {r['causa']:<8s} {r['obitos']:>7,d}  ({r['pct']:>5.2f}%)  {r['capitulo'][:60]}")

    if "obitos_capitulo_cid10" in dfs and results.get("obitos_capitulo_cid10") == "OK":
        cap = dfs["obitos_capitulo_cid10"].groupby("capitulo", as_index=False)["obitos"].sum()
        cap = cap.sort_values("obitos", ascending=False)
        print("\nTop 5 capítulos CID-10 — Brasil 2023:")
        for _, r in cap.head(5).iterrows():
            pct = r['obitos'] / total * 100
            print(f"  {r['obitos']:>8,d}  ({pct:>5.2f}%)  {r['capitulo']}")

    if "taxa_mortalidade_uf" in dfs and results.get("taxa_mortalidade_uf") == "OK":
        tx = dfs["taxa_mortalidade_uf"]
        print("\nTaxa bruta de mortalidade (por 100k hab.):")
        print(f"  maior: {tx.iloc[0]['uf']} ({tx.iloc[0]['uf_nome']}) — {tx.iloc[0]['taxa_por_100k']}")
        print(f"  menor: {tx.iloc[-1]['uf']} ({tx.iloc[-1]['uf_nome']}) — {tx.iloc[-1]['taxa_por_100k']}")

    print("=" * 60)


if __name__ == "__main__":
    main()
