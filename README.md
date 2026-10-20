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
│   ├── transform.py        # Validação e limpeza: data/ -> data_delta/ (Delta silver)
│   └── analytics.py        # Agregações: silver -> data_delta/ (Delta gold)
├── data/                    # Dados brutos (Parquet) — ignorado pelo Git
│   ├── ibge_localidades/    # Estados e municípios
│   ├── ibge_sidra/          # População estimada
│   ├── sim/                 # Mortalidade
│   └── cnes/                # Estabelecimentos de saúde
└── data_delta/              # Delta Lakes — ignorado pelo Git
    ├── dim_estados/         # [silver] 27 UFs
    ├── dim_municipios/      # [silver] 5.571 municípios (colunas renomeadas)
    ├── populacao_uf/        # [silver] População por UF (2022)
    ├── mortalidade/         # [silver] 1.47M óbitos, 27 UFs combinadas
    ├── estabelecimentos/    # [silver] 680 estabelecimentos CNES
    ├── obitos_uf_mes/       # [gold] Óbitos por UF x mês (324 rows)
    ├── taxa_mortalidade_uf/ # [gold] Taxa bruta por 100k hab. (27 rows)
    ├── obitos_capitulo_cid10/   # [gold] Óbitos por UF x capítulo CID-10
    ├── obitos_causa_cid10/  # [gold] Óbitos por causa exata (fonte do top 10)
    └── obitos_faixa_etaria/ # [gold] Óbitos por UF x sexo x faixa etária
```

## Uso

```bash
# Instalar dependências
pip install -r requirements.txt

# 1. Extração: fontes -> data/ (Parquet bruto)
python scripts/extract.py

# 2. Validação e limpeza: data/ -> data_delta/ (Delta silver)
python scripts/transform.py

# 3. Agregações analíticas: silver -> gold
python scripts/analytics.py
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

## Analytics (Camada Gold)

O `analytics.py` lê os Deltas silver e grava 5 tabelas gold agregadas (todas validadas: soma de óbitos == 1.465.610, 27 UFs, 12 meses):

* **obitos_uf_mes** — óbitos por UF x mês, com nome e região
* **taxa_mortalidade_uf** — óbitos + população + taxa bruta por 100k hab. (pop. 2022 x óbitos 2023, aproximada)
* **obitos_capitulo_cid10** — óbitos por UF x capítulo CID-10 (mapeamento letra+2 dígitos → 22 capítulos)
* **obitos_causa_cid10** — óbitos por causa básica exata, nacional, ordenado (fonte do top 10)
* **obitos_faixa_etaria** — óbitos por UF x sexo x faixa etária (padrão DATASUS, 18 faixas + ignorado)

Destaques 2023: I219 (infarto) é a maior causa isolada (6,3%); capítulo IX (circulatório) lidera com 26,5%; RJ tem a maior taxa bruta (904/100k) e DF a menor (500/100k).

## .gitignore

A pasta `data/`, `data_delta/` e arquivos `*.parquet` são ignorados pelo Git. Os dados são gerados pelo script e não devem ser versionados.
