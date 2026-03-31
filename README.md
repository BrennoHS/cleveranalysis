# AdOps Discrepancy Analyzer

Ferramenta interna para comparar relatórios de anúncios do publisher com dados internos do ad server da Clever no Elasticsearch. Desenvolvida para times de AdOps e Suporte.

---

## Stack

| Camada     | Tecnologia                          |
|------------|-------------------------------------|
| Backend    | Python 3.11+ · FastAPI · Uvicorn   |
| Frontend   | HTML · CSS · Vanilla JS            |
| IA         | Google Gemini (análise/explicação) |
| Dados      | Elasticsearch (REST API)           |

---

## Estrutura do Projeto

```
adops-discrepancy-tool/
├── backend/
│   ├── main.py          # App FastAPI + rotas
│   ├── parser.py        # Parser determinístico de relatório (+ fallback opcional com IA)
│   ├── elastic.py       # Consultas ao Elasticsearch
│   ├── analysis.py      # Cálculo de discrepâncias
│   ├── ai_explainer.py  # Geração de explicação com Gemini
│   ├── requirements.txt
│   └── .env             # Variáveis locais (criar a partir de .env.example)
├── frontend/
│   └── index.html       # UI em arquivo único
└── README.md
```

---

## Configuração

### 1. Clonar / baixar o projeto

```bash
cd adops-discrepancy-tool
```

### 2. Criar ambiente virtual e instalar dependências

Usar ambiente virtual torna a instalação reproduzível entre máquinas.

Windows (PowerShell):

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r backend/requirements.txt
```

macOS / Linux:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r backend/requirements.txt
```

### 3. Configurar variáveis de ambiente

Crie seu arquivo local a partir do template:

```bash
cp backend/.env.example backend/.env
```

Depois edite o `backend/.env`:

```env
# Chave da API do Google Gemini
GEMINI_API_KEY=your_gemini_api_key

# Liga/desliga explicação com IA
GEMINI_EXPLAINER_ENABLED=false

# Conexão com Elasticsearch
ES_URL=https://your-es-cluster-host:9200
ES_USERNAME=your_username
ES_PASSWORD=your_password

# Índices disponíveis
ES_INDICES=served-impressions,core-view-events

# Mapeamento de campos - ajuste conforme seu schema
ES_DATE_FIELD=@timestamp
ES_PUBLISHER_FIELD=publisher
ES_SERVED_FIELD=served_impressions
ES_VIEWABLE_FIELD=viewable_impressions
```

As variáveis opcionais adicionais estão documentadas em `backend/.env.example`
(`GEMINI_PARSER_*`, `KIBANA_VERSION`, `ES_TIMEZONE`, `ES_PUBLISHER_MATCH_MODE`, `ES_IP_FALLBACK_MAX_HITS`).

> **Observação sobre acesso ao Elasticsearch:** se seu cluster estiver atrás de VPN ou WARP, garanta que está ativo antes de subir o servidor.

### 4. Iniciar o servidor

```bash
python start.py
```

Você deve ver:
```
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

### 5. Abrir o frontend

Acesse http://127.0.0.1:8000 no navegador, ou abra `frontend/index.html` diretamente.

### 6. Verificação rápida de saúde

```bash
curl http://127.0.0.1:8000/health
```

Resposta esperada:

```json
{"status":"ok"}
```

---

## Rodar em Outro Ambiente (Checklist)

Use este checklist para migrar o projeto para outra máquina com mínimo atrito:

1. Instalar Python 3.12.
2. Clonar/copiar o projeto.
3. Criar e ativar `.venv`.
4. Instalar dependências de `backend/requirements.txt`.
5. Criar `backend/.env` a partir de `backend/.env.example`.
6. Preencher credenciais ES/Gemini e mapeamento de campos.
7. Executar `python start.py`.
8. Validar `GET /health`.

Seguindo esses passos, o projeto deve funcionar sem alterações de código.

---

## Notas de Segurança

- Nunca faça commit de credenciais reais no controle de versão.
- Mantenha `backend/.env` somente local.
- Faça rotação de chaves/senhas que já tenham sido expostas acidentalmente.

---

## Uso

1. Informe os dados do publisher usando um dos dois modos:
- Modo texto: cole o relatório do publisher na área de texto (texto livre, tabela, CSV-like etc.).
- Modo arquivo: envie uma planilha estruturada (.xlsx/.csv) e informe o período da análise.

   **Exemplo de entrada:**
   ```
   Campaign Report - January 2025
   Publisher: example.com
   Period: Jan 01, 2025 - Jan 31, 2025

   Impressions:          1,240,000
   Measurable:           1,240,000
   Active Views:           682,000
   Viewability Rate:         55.0%
   ```

2. Opcionalmente, informe Publisher / Domínio para filtrar no Elasticsearch.

3. Clique em **Analyze Report** (ou pressione `Ctrl+Enter`).

4. O sistema irá:
- Extrair métricas do relatório com parser determinístico (regex/tabela/data).
- Consultar o Elasticsearch no mesmo período e publisher.
- Calcular métricas de discrepância.
- Exibir tabela comparativa e explicação gerada por IA.

Por padrão, a IA é usada apenas para o texto final de explicação. O parser do relatório roda localmente.

Fallback opcional no parser:
- Defina `GEMINI_PARSER_ENABLED=true` em `backend/.env` se quiser que o Gemini complemente campos ausentes quando a extração determinística não encontrar todos os dados necessários.

---

## Classificação de Discrepância

| Gap de Viewability | Status             |
|--------------------|--------------------|
| < 10 pp            | ✅ Normal          |
| 10 - 25 pp         | ⚠️ Atenção         |
| > 25 pp            | 🔴 Alta Discrepância |

---

## Mapeamento de Campos Elasticsearch

A ferramenta foi desenhada para funcionar com qualquer schema ES. Configure os campos no `.env`:

| Variável de Ambiente       | Padrão              | Descrição                                      |
|----------------------------|---------------------|------------------------------------------------|
| `ES_DATE_FIELD`            | `date`              | Campo de data usado no filtro de período       |
| `ES_PUBLISHER_FIELD`       | `publisher`         | Campo de publisher/domínio                     |
| `ES_SERVED_FIELD`          | `served_impressions`| Métrica de impressões served                   |
| `ES_VIEWABLE_FIELD`        | `viewable_impressions`| Métrica de impressões viewable               |
| `ES_TIMEZONE`              | `Europe/Lisbon`     | Timezone para interpretar filtros de data      |
| `ES_PUBLISHER_MATCH_MODE`  | `contains`          | Modo de match de publisher: `contains` ou `exact` |

Notas para alinhamento com Kibana:
- Filtros de data são interpretados em `ES_TIMEZONE` e convertidos para UTC internamente.
- Se precisar de paridade estrita com filtros do Kibana, use `ES_PUBLISHER_MATCH_MODE=exact` para evitar wildcard amplo.

---

## Referência da API

### `POST /analyze`

**Request:**
```json
{
  "report_text": "raw publisher report text...",
  "publisher": "example.com"
}
```

**Response:**
```json
{
  "start_date": "2025-01-01",
  "end_date": "2025-01-31",
  "publisher": "example.com",
  "pub_served": 1240000,
  "pub_viewable": 682000,
  "pub_viewability_pct": 55.0,
  "clever_served": 1198000,
  "clever_viewable": 634000,
  "clever_viewability_pct": 52.9,
  "diff_served": 42000,
  "diff_viewable": 48000,
  "viewability_diff_pp": 2.1,
  "viewability_diff_pct": 3.97,
  "status": "Normal",
  "explanation": "..."
}
```

### `GET /health`
Retorna `{"status": "ok"}`.

### `POST /analyze-file`
Consome `multipart/form-data` para arquivos estruturados de relatório.

Campos obrigatórios do formulário:
- `file`: relatório .xlsx ou .csv exportado de publisher/GAM.
- `start_date`: data no formato `YYYY-MM-DD` (inclusiva).
- `end_date`: data no formato `YYYY-MM-DD` (exclusiva).

Campos opcionais do formulário:
- `publisher`
- `script_id`

Comportamento do período:
- O backend usa semântica `[start_date, end_date)` (mesmo estilo do range absoluto do Kibana com fim na meia-noite).
- Exemplo: `start_date=2026-03-24` e `end_date=2026-03-25` inclui exatamente as 24 horas do dia 24.

---

## Mapeamento de Terminologia do Parser

O parser determinístico mapeia automaticamente termos comuns de relatório de publisher:

| Interpretado Como         | Labels Aceitos                                    |
|---------------------------|---------------------------------------------------|
| `served_impressions`      | served impressions, impressions, measurable       |
| `viewable_impressions`    | viewable impressions, active views, viewable      |

---

## Solução de Problemas

**`422 Could not extract required fields`**
- O texto do relatório pode estar sem contagens de impressões ou datas. Tente incluir labels explícitos.

**`502 Elasticsearch query failed`**
- Verifique se ES_URL, credenciais e nome de índice estão corretos.
- Garanta que VPN/WARP está conectada (se exigido).
- Verifique se os nomes de campo no ES batem com seu schema real.

**`Failed to parse report`**
- Garanta que o relatório contém served impressions e intervalo de datas.
- Se estiver usando fallback de parser com IA, confirme que `GEMINI_API_KEY` é válida.
