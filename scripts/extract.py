"""
HealthData-Pipeline: Extract Script
=====================================

Downloads data from Brazilian health data sources and saves as Parquet files
organized in separate folders under the data/ directory.

Sources:
    - IBGE Localidades (API REST)      -> Geographic dimension
    - IBGE SIDRA      (API estatística) -> Population
    - PNI             (pysus FTP)       -> Doses applied
    - SIM             (pysus FTP)       -> Mortality
    - CNES            (pysus FTP)       -> Establishments

Output structure:
    data/
    |-- ibge_localidades/
    |-- ibge_sidra/
    |-- pni/
    |-- sim/
    |-- cnes/

Requirements:
    pip install requests pandas pyarrow pysus
"""

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests

# Garante typing_extensions >= 4.16 (necessario para pysus.sentinel)
import typing_extensions
if not hasattr(typing_extensions, "sentinel"):
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "typing_extensions>=4.16", "--quiet"],
        capture_output=True,
    )
    import importlib
    importlib.reload(typing_extensions)
# Fallback: adiciona sentinel se ainda nao existir (Databricks system path)
if not hasattr(typing_extensions, "sentinel"):
    def _sentinel(name, *, raise_on_access=False):
        class _S:
            __slots__ = ()
            def __repr__(self): return name
            def __bool__(self): return False
            if raise_on_access:
                def __getattr__(self, attr):
                    raise TypeError(f"Sentinel '{name}' was accessed")
        return _S()
    typing_extensions.sentinel = _sentinel

# Auto-instala pysus se nao estiver disponivel
try:
    import pysus
except (ImportError, ModuleNotFoundError):
    print("Installing pysus...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "pysus", "--quiet"],
        check=True,
    )
    import pysus

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _find_base_dir() -> Path:
    """Localiza a raiz do projeto (HealthData-Pipeline) sem hardcode de usuario."""
    marker = Path("scripts") / "extract.py"
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
DATA_DIR = BASE_DIR / "data"

# --- API endpoints ---------------------------------------------------------
IBGE_LOCALIDADES_URL = "https://servicodados.ibge.gov.br/api/v1/localidades"
# SIDRA table 9514 = Populacao residente (v/93 = variavel, p/last = ano mais recente)
IBGE_SIDRA_URL = "https://api.sidra.ibge.gov.br/values/t/9514/n3/all/v/93/p/last"

# --- pysus / DataSUS settings -----------------------------------------------
# SIM, CNES e PNI são acessados via pysus (FTP do DataSUS)
SIM_YEAR = 2023
CNES_YEAR = 2023
CNES_MONTH = 12        # mês de referência (dezembro/2023)
CNES_GROUP = "EF"      # EF = Estabelecimentos e Filiações
PNI_YEAR = 2023

# 27 unidades federativas do Brasil
UF_CODES = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]

# Request settings
TIMEOUT_API = 60


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_dir(path: Path) -> None:
    """Create directory if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)


def save_parquet(df: pd.DataFrame, folder: str, filename: str) -> Path:
    """Save a DataFrame as Parquet inside data/<folder>/<filename>.parquet."""
    out_dir = DATA_DIR / folder
    ensure_dir(out_dir)
    out_path = out_dir / f"{filename}.parquet"
    df.to_parquet(out_path, index=False)
    print(f"  ✓ Saved: {out_path}  ({len(df)} rows, {len(df.columns)} cols)")
    return out_path


def flatten_json_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Expand nested dict/list columns into flat columns via json_normalize."""
    for col in columns:
        if col in df.columns:
            expanded = pd.json_normalize(df[col]).add_prefix(f"{col}_")
            df = df.drop(columns=[col]).reset_index(drop=True).join(expanded)
    return df


# ---------------------------------------------------------------------------
# 1. IBGE Localidades  (API REST – geographic dimension)
# ---------------------------------------------------------------------------

def extract_ibge_localidades() -> None:
    """Fetch states (UFs) and municipalities from IBGE Localidades API."""
    print("\n=== IBGE Localidades ===")

    # --- States ---
    print("  Fetching states...")
    resp = requests.get(f"{IBGE_LOCALIDADES_URL}/estados", timeout=TIMEOUT_API)
    resp.raise_for_status()
    states = pd.DataFrame(resp.json())
    states = flatten_json_columns(states, ["regiao"])
    save_parquet(states, "ibge_localidades", "estados")

    # --- Municipalities ---
    print("  Fetching municipalities...")
    resp = requests.get(f"{IBGE_LOCALIDADES_URL}/municipios", timeout=TIMEOUT_API)
    resp.raise_for_status()
    munis = pd.DataFrame(resp.json())
    # Flatten microrregiao -> mesorregiao, UF, regiao
    if "microrregiao" in munis.columns:
        micro_expanded = pd.json_normalize(munis["microrregiao"]).add_prefix("microrregiao_")
        munis = munis.drop(columns=["microrregiao"]).reset_index(drop=True).join(micro_expanded)
        if "microrregiao_mesorregiao" in munis.columns:
            meso_expanded = pd.json_normalize(munis["microrregiao_mesorregiao"]).add_prefix("mesorregiao_")
            munis = munis.drop(columns=["microrregiao_mesorregiao"]).reset_index(drop=True).join(meso_expanded)
        if "microrregiao_UF" in munis.columns:
            uf_expanded = pd.json_normalize(munis["microrregiao_UF"]).add_prefix("uf_")
            munis = munis.drop(columns=["microrregiao_UF"]).reset_index(drop=True).join(uf_expanded)
            if "uf_regiao" in munis.columns:
                reg_expanded = pd.json_normalize(munis["uf_regiao"]).add_prefix("regiao_")
                munis = munis.drop(columns=["uf_regiao"]).reset_index(drop=True).join(reg_expanded)
    save_parquet(munis, "ibge_localidades", "municipios")

    print("  ✓ IBGE Localidades done.")


# ---------------------------------------------------------------------------
# 2. IBGE SIDRA  (API estatística – population)
# ---------------------------------------------------------------------------

def extract_ibge_sidra() -> None:
    """Fetch population estimates by state from SIDRA table 9514."""
    print("\n=== IBGE SIDRA (Population) ===")

    print("  Fetching population data (table 9514, latest year)...")
    resp = requests.get(IBGE_SIDRA_URL, timeout=TIMEOUT_API)
    resp.raise_for_status()
    data = resp.json()

    # SIDRA returns a list of dicts; first element has column descriptions
    if isinstance(data, list) and len(data) > 1:
        df = pd.DataFrame(data[1:])
    else:
        df = pd.DataFrame(data)

    # Determinar o ano dos dados a partir da coluna D3N
    year = df["D3N"].iloc[0] if "D3N" in df.columns else "unknown"
    save_parquet(df, "ibge_sidra", f"populacao_estimada_{year}")
    print("  ✓ IBGE SIDRA done.")


# ---------------------------------------------------------------------------
# 3. PNI  (pysus Saude API - doses applied)
# ---------------------------------------------------------------------------

def extract_pni() -> None:
    """Fetch vaccination doses from DataSUS PNI via pysus.saude.vacinacao (monthly ZIPs).

    Uses pysus only to list available files, then downloads directly via requests
    to avoid async issues in Databricks.
    """
    print("\n=== PNI (Doses Applied) ===")

    import tempfile
    import zipfile

    # Obter lista de arquivos de vacinacao (usar pysus apenas para listagem)
    print("  Listing vaccination files...")
    file_bag = pysus.saude.vacinacao(download=False)

    # Filtrar arquivos do ano alvo: vacinacao_<mes>_<ano>_csv.zip
    target_suffix = f"_{PNI_YEAR}_csv.zip"
    pni_files = [
        f for f in file_bag.files
        if f.name.startswith("vacinacao_") and f.name.endswith(target_suffix)
    ]

    if not pni_files:
        print(f"  No vaccination files found for year {PNI_YEAR}.")
        print("  PNI done.")
        return

    print(f"  Found {len(pni_files)} monthly files for {PNI_YEAR}.")

    # Baixar e converter cada arquivo mensal
    for file_url in pni_files:
        month_name = file_url.name.replace("vacinacao_", "").replace(target_suffix, "")
        print(f"  Downloading {file_url.name}...")
        try:
            # Download direto via requests (streaming, ~1.9GB por arquivo)
            resp = requests.get(file_url.path, stream=True, timeout=600)
            resp.raise_for_status()

            # Salvar ZIP em arquivo temporario
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
                for chunk in resp.iter_content(chunk_size=8192):
                    tmp.write(chunk)
                zip_path = tmp.name

            print(f"    Downloaded {Path(zip_path).stat().st_size / 1e6:.1f} MB")

            # Extrair CSV do ZIP e converter para Parquet
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                if not csv_names:
                    print(f"    No CSV found in {file_url.name}")
                    continue
                with zf.open(csv_names[0]) as f:
                    df = pd.read_csv(f, sep=";", encoding="latin-1", low_memory=False)

            save_parquet(df, "pni", f"vacinacao_{month_name}_{PNI_YEAR}")

            # Limpar o ZIP temporario
            Path(zip_path).unlink(missing_ok=True)

        except Exception as e:
            print(f"    Error processing {file_url.name}: {e}")
            # Garantir limpeza
            try:
                Path(zip_path).unlink(missing_ok=True)
            except NameError:
                pass

    print("  PNI done.")


# ---------------------------------------------------------------------------
# 4. SIM  (ZIP 2023 – mortality)
# ---------------------------------------------------------------------------

def extract_sim() -> None:
    """Download SIM mortality data via pysus FTP (per UF, CID-10)."""
    print("\n=== SIM (Mortality) ===")

    for uf in UF_CODES:
        print(f"  Fetching SIM {uf} {SIM_YEAR}...")
        try:
            df = pysus.ftp.sim(uf, SIM_YEAR, as_dataframe=True)
            if df is not None and len(df) > 0:
                save_parquet(df, "sim", f"DO{uf}{SIM_YEAR}")
            else:
                print(f"    {uf}: no data")
        except Exception as e:
            print(f"    {uf}: ⚠ {e}")

    print("  ✓ SIM done.")


# ---------------------------------------------------------------------------
# 5. CNES  (pysus FTP – establishments)
# ---------------------------------------------------------------------------

def extract_cnes() -> None:
    """Download CNES establishment data via pysus FTP (per UF)."""
    print("\n=== CNES (Establishments) ===")

    for uf in UF_CODES:
        print(f"  Fetching CNES {uf} {CNES_YEAR}-{CNES_MONTH:02d} (group={CNES_GROUP})...")
        try:
            df = pysus.ftp.cnes(uf, CNES_YEAR, CNES_MONTH, group=CNES_GROUP, as_dataframe=True)
            if df is not None and len(df) > 0:
                save_parquet(df, "cnes", f"{CNES_GROUP}{uf}{CNES_YEAR}{CNES_MONTH:02d}")
            else:
                print(f"    {uf}: no data")
        except Exception as e:
            print(f"    {uf}: ⚠ {e}")

    print("  ✓ CNES done.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Run all extractors and print a summary."""
    print("=" * 60)
    print("HealthData-Pipeline: Extract")
    print(f"Data directory: {DATA_DIR}")
    print("=" * 60)

    ensure_dir(DATA_DIR)

    extractors = [
        ("IBGE Localidades", extract_ibge_localidades),
        ("IBGE SIDRA",       extract_ibge_sidra),
        ("PNI",              extract_pni),
        ("SIM",              extract_sim),
        ("CNES",             extract_cnes),
    ]

    results: dict[str, str] = {}
    for name, func in extractors:
        try:
            func()
            results[name] = "✓ Success"
        except Exception as exc:
            results[name] = f"✗ {exc}"
            print(f"\n  ✗ Error in {name}: {exc}")

    print("\n" + "=" * 60)
    print("Extraction Summary")
    print("-" * 60)
    for name, status in results.items():
        print(f"  {name:<20s} {status}")
    print("=" * 60)


if __name__ == "__main__":
    main()
