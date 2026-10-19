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
│   └── extract.py          # Script de extração
└── data/                    # Dados extraídos (Parquet) — ignorado pelo Git
    ├── ibge_localidades/    # Estados e municípios
    ├── ibge_sidra/          # População estimada
    ├── sim/                 # Mortalidade
    └── cnes/                # Estabelecimentos de saúde
```

## Uso

```bash
# Instalar dependências
pip install -r requirements.txt

# Executar extração
python scripts/extract.py
```

O script baixa os dados de todas as fontes e salva cada uma em sua respectiva subpasta como arquivos Parquet:

* **IBGE Localidades e SIDRA** — via API REST (`requests`)
* **SIM e CNES** — via `pysus.ftp` (FTP do DataSUS), iterando sobre as 27 UFs

O script auto-instala `pysus` e `typing_extensions>=4.16` se necessário.
