# HealthData-Pipeline

Pipeline de extração de dados de saúde pública brasileira. Os dados são coletados de múltiplas fontes (APIs REST, FTP do DataSUS e S3) e convertidos para Parquet, organizados em subpastas dentro de `data/`.

## Fontes de Dados

| Fonte              | Acesso              | Uso                  | Status |
|--------------------|---------------------|----------------------|--------|
| IBGE Localidades   | API REST            | Dimensão geográfica  | ✓      |
| IBGE SIDRA         | API REST (SIDRA)    | População (2022)     | ✓      |
| SIM                | pysus FTP (DataSUS) | Mortalidade (2023)   | ✓      |
| CNES               | pysus FTP (DataSUS) | Estabelecimentos     | ✓      |

### Detalhes por fonte

* **IBGE Localidades** — estados (27) e municípios (5.571) com regiões, mesorregiões e microrregiões.
* **IBGE SIDRA** — população residente por UF (tabela 9514, variável 93, ano mais recente disponível = 2022).
* **SIM** — óbitos por UF (CID-10, 2023), 27 arquivos, ~1.2M registros, 87 colunas.
* **CNES** — estabelecimentos de saúde (grupo EF, dezembro/2023), 21 UFs com dados.

## Estrutura do Projeto

```
HealthData-Pipeline/
├── .gitignore
├── README.md
├── requirements.txt
├── scripts/
│   ├── extract.py          # Extração: fontes -> data/ (Parquet bruto)
│   └── transform.py        # Validação e limpeza: data/ -> data_delta/ (Delta Lake)
├── data/                    # Dados brutos (Parquet) — ignorado pelo Git
│   ├── ibge_localidades/    # Estados e municípios
│   ├── ibge_sidra/          # População estimada
│   ├── sim/                 # Mortalidade
│   └── cnes/                # Estabelecimentos de saúde
└── data_delta/              # Delta Lakes validados e limpos — ignorado pelo Git
    ├── dim_estados/         # 27 UFs
    ├── dim_municipios/      # 5.571 municípios (colunas renomeadas)
    ├── populacao_uf/        # População por UF (2022)
    ├── mortalidade/         # 1.47M óbitos, 27 UFs combinadas
    └── estabelecimentos/    # 680 estabelecimentos CNES
```

## Uso

```bash
# Instalar dependências
pip install -r requirements.txt

# 1. Extração: fontes -> data/ (Parquet bruto)
python scripts/extract.py

# 2. Validação e limpeza: data/ -> data_delta/ (Delta Lake)
python scripts/transform.py
```

O script baixa os dados de todas as fontes e salva cada uma em sua respectiva subpasta como arquivos Parquet:

* **IBGE Localidades e SIDRA** — via API REST (`requests`)
* **SIM e CNES** — via `pysus.ftp` (FTP do DataSUS), iterando sobre as 27 UFs

O script auto-instala `pysus` e `typing_extensions>=4.16` se necessário.

## Transform (Validação e Limpeza)

O `transform.py` lê os Parquets brutos de `data/`, aplica validações (se alguma falha, a tabela não é gravada) e limpezas, gravando cada fonte como um Delta Lake em `data_delta/`:

* **dim_estados / dim_municipios** — colunas renomeadas (sem pontos/hífens), IDs tipados como Int64
* **populacao_uf** — schema renomeado (D1C→uf_codigo, D1N→uf_nome, V→populacao)
* **mortalidade** — 27 UFs combinadas; datas DDMMYYYY→datetime; IDADE decodificada em `idade_tipo`/`idade_valor`/`idade_anos` (codificação validada empiricamente contra DTNASC: 4XX=anos, 3XX=meses, 2XX=dias, 1XX=horas, 0XX=minutos, 5XX=100+ anos); SEXO→descrição; códigos de município tipados
* **estabelecimentos** — 21 UFs combinadas; CNES/CODUFMUN tipados como Int64; COMPETEN→date

Nota técnica: o Delta é gravado via `deltalake` (delta-rs) em diretório temporário e copiado para `data_delta/` — o rename não é permitido no FUSE mount do `/Workspace`.
