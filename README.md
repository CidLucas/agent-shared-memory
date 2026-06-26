# 🧠 Memória Incremental para Agentes de IA — Shared Memory com MCP

> **Código original de produção** extraído da **Blu Platform** — um sistema multiagente real rodando em produção.  
> Nada de demos acadêmicas, provas de conceito ou brinquedos de tutorial.  
> Isso é o `shared_business_memory` que conecta dezenas de agentes de IA especializados em um ecossistema coerente.

---

## 📋 TL;DR

- **O que é:** Sistema de memória compartilhada e incremental para múltiplos agentes de IA. Agentes escrevem fatos, decisões, descobertas e snapshots em um banco central e leem o contexto uns dos outros — sem precisar de conversas diretas.
- **Arquitetura:** MCP Server (Model Context Protocol) + SQLite (via adapters que substituem o Supabase original).
- **Tamanho real:** `memory_module.py` com **3.467 linhas**, 13 tools MCP, pre/post-flight hooks, backup e prune automáticos.
- **Para quem é:** Tech leads, arquitetos de IA, engenheiros de ML e founders que estão construindo sistemas multiagente sérios e precisam de um design de memória battle-tested.
- **Licença:** MIT — use, estude, adapte, critique.

---

## 🎯 O que este repositório contém

Este é **código original de produção** extraído do monorepo da **Blu Platform**, uma plataforma multiagente B2B que atende centenas de empresas. Diferente de repositórios acadêmicos que implementam "memória para agentes" com 200 linhas e um vetor mockado, este código:

- ✅ **Esteve em produção** — cada linha foi escrita para resolver problemas reais de escalabilidade, concorrência e isolamento de tenants.
- ✅ **Tem 3.467 linhas só no módulo de memória** — validação de entidades, controle de permissão de escrita (Single Writer principle), TTL tiers, snapshots com frontmatter, grafos semânticos, soft-delete, export.
- ✅ **É framework-agnostic** — usa o protocolo MCP padrão, não um framework proprietário.
- ✅ **Funciona offline** — graças aos adapters SQLite que substituem o Supabase sem modificar uma linha do código original.

### Estrutura de diretórios

```
agent-shared-memory/
├── run.py                              # Entrypoint: monkey-patch + init + MCP server
├── pyproject.toml                      # Dependências mínimas (fastmcp, httpx, mcp)
│
├── libs/
│   └── blu_agent_framework/            # 🔵 CÓDIGO ORIGINAL
│       └── src/blu_agent_framework/
│           ├── handoff/
│           │   ├── handoff_hook.py              # Hook: escreve learning notes na handoff
│           │   └── shared_memory_context.py     # Loader: carrega contexto da shared memory
│           ├── onboarding/
│           │   └── onboarding_shared_memory_hook.py  # Hook pós-ETL onboarding
│           └── utils/
│               ├── llm_parse.py                 # Parsing de respostas LLM
│               └── observability.py             # Tracing e observabilidade
│
├── services/
│   ├── tool_pool_api/                  # 🔵 CÓDIGO ORIGINAL
│   │   └── src/tool_pool_api/server/tool_modules/
│   │       ├── memory_module.py        # ★ 3.467 linhas — 13 tools MCP
│   │       ├── memory_pre_flight.py    # Hook pré-execução (lê contexto)
│   │       ├── memory_post_flight.py   # Hook pós-execução (persiste resultados)
│   │       └── utils/entity.py         # Validação e normalização de entidades
│   │
│   └── routine_engine/                 # 🔵 CÓDIGO ORIGINAL
│       └── src/routines/
│           ├── backup_shared_memory.py # Backup diário (dump + gzip + storage)
│           └── prune_shared_memory.py  # Limpeza automática (soft/hard delete)
│
└── adapters/                           # 🟢 ADAPTADOS (SQLite no lugar de Supabase)
    ├── blu_supabase_client/            # ★ SQLite fake do Supabase client
    ├── blu_auth/                       # Stub de autenticação
    ├── blu_context_service/            # Stub de schemas de contexto
    └── tool_pool_api/                  # Stub do módulo tool_pool_api
```

---

## 📁 Estrutura

| Diretório | O que é | Tipo |
|---|---|---|
| `libs/blu_agent_framework/` | Handoff hooks, shared memory context loader, onboarding hook pós-ETL | 🔵 Original |
| `services/tool_pool_api/` | MCP Server de memória com 13 tools, pre-flight hook, post-flight hook | 🔵 Original |
| `services/routine_engine/` | Backup diário com compressão gzip + prune automático com soft/hard delete | 🔵 Original |
| `adapters/` | Stubs SQLite que substituem Supabase (Auth, Context Service, Storage) | 🟢 Adaptado |

---

## 🏗 Como funciona

```
┌──────────────────────────────────────────────────────────────────┐
│                    SHARED BUSINESS MEMORY                        │
│                                                                  │
│  ┌──────────┐     ┌──────────────────────┐     ┌──────────┐     │
│  │ Agente A │ ──→ │   MCP Server (FastMCP)│ ←── │ Agente B │     │
│  │ (Escrita)│     │  memory_module.py     │     │ (Leitura)│     │
│  └──────────┘     └──────────┬───────────┘     └──────────┘     │
│                              │                                   │
│                              ▼                                   │
│               ┌──────────────────────────┐                      │
│               │    SQLite / Supabase      │                      │
│               │  shared_business_memory   │                      │
│               │  shared_memory_links      │                      │
│               │  shared_business_memory_meta                     │
│               └──────────────────────────┘                      │
│                                                                  │
│  Fluxo típico:                                                   │
│    Agent A → shared_memory_write → SQLite → shared_memory_read → Agent B │
│                                                                  │
│  Ciclo de vida:                                                  │
│    shared_memory_upsert → backup (02:00) → prune (03:00)         │
└──────────────────────────────────────────────────────────────────┘
```

### Conceitos-chave

**Entidades:** 9 tipos validados — `skill`, `client`, `contact`, `supplier`, `user`, `snapshot`, `routine`, `agent_result`, `agent_metadata`.

**Single Writer Principle:** Cada `source` só pode escrever em `entity_types` específicos. Exemplo: `source='manual'` não pode escrever `snapshot` ou `routine`.

**TTL Tiers:** 5 níveis de retenção — `curated` (nunca expira), `migration` (90d), `specialist` (30d), `memory_agent_hi` (14d), `memory_agent_lo` (7d). Após soft-delete, +90 dias para hard-delete definitivo.

**Semantic Links:** Relacionamentos nomeados entre entidades (`works_for`, `prefers`, `depends_on`, etc.) com grafo navegável via BFS.

**Auto-linking:** Ao escrever um fato, o sistema detecta automaticamente referências a outras entidades no formato `[label](entity_type:entity_name)` e cria links.

---

## 🔧 MCP Tools

Todas as 13 tools expostas pelo servidor MCP:

| Tool | Descrição | Operação |
|---|---|---|
| `shared_memory_list` | Lista entidades com entradas de memória | Leitura |
| `shared_memory_read` | Lê um fato específico (chave composta) | Leitura |
| `shared_memory_upsert` | Insere ou atualiza um fato (versionado) | Escrita |
| `shared_memory_meta_upsert` | Insere/atualiza meta entry (pipeline data) | Escrita |
| `shared_memory_write` | Escreve novo fato (strict INSERT ou upsert) | Escrita |
| `shared_memory_search` | Busca semântica via embeddings (Cohere) | Leitura |
| `shared_memory_flush` | Soft-delete (marca `flushed_at` no metadata) | Deleção |
| `shared_memory_link` | Cria link semântico entre entidades | Escrita |
| `shared_memory_unlink` | Remove link por ID | Deleção |
| `shared_memory_get_links` | Consulta links por entidade e/ou tipo | Leitura |
| `shared_memory_meta_read` | Lê meta entry da `shared_business_memory_meta` | Leitura |
| `shared_memory_meta_list` | Lista meta entries (opcional: filtro por tipo) | Leitura |
| `shared_memory_export` | Exporta todos os fatos do cliente (backup/analytics) | Leitura |
| `shared_memory_graph` | Navega o grafo semântico (BFS, shortest path, cluster) | Leitura |
| `shared_memory_pre_flight` | Lê contexto de execuções recentes do agente (internal) | Leitura |

---

## 🚀 Quick Start

```bash
# 1. Clone e instale
git clone https://github.com/seu-usuario/agent-shared-memory.git
cd agent-shared-memory
pip install .

# 2. Inicialize o banco SQLite
python run.py --init

# 3. Inicie o servidor MCP
python run.py

# Opcional: porta e host customizados
python run.py --host 0.0.0.0 --port 8000

# Banco customizado
python run.py --db /path/to/memory.db
```

O servidor inicia em `http://0.0.0.0:8000` e expõe as tools MCP via transporte HTTP padrão.

---

## 🔧 Adapters — Por que existem

O código original da Blu Platform depende do Supabase (banco PostgreSQL gerenciado, autenticação, storage). Para tornar o repositório executável localmente **sem modificar uma linha do código original**, criamos **adapters** que implementam a mesma interface do Supabase usando SQLite puro.

```
Código original (memory_module.py)
         │
         ▼  import blu_supabase_client
         │
    ┌────┴────┐
    │ Adapter │  ← SQLite puro, mesma API do Supabase
    └─────────┘
         │
         ▼  SQLite local (shared_memory.db)
```

**O que os adapters fazem:**

| Adapter | Original | Substituído por |
|---|---|---|
| `blu_supabase_client/` | Supabase REST client + PostgreSQL | SQLite com query builder compatível |
| `blu_auth/` | Auth0 / JWT validation | Stub que sempre retorna autenticado |
| `blu_context_service/` | Context schemas remotos | Constantes locais |
| `tool_pool_api/` | Módulo de registro de tools | Stub de módulo |

O resultado: o `memory_module.py` de 3.467 linhas roda **sem alterações** — o adapter faz ponte entre a interface que o código espera (Supabase) e a implementação local (SQLite).

---

## 🧠 Conceitos de Design

### Memória Incremental, não Conversacional

Agentes não conversam entre si. Eles escrevem fatos em uma memória compartilhada indexada por `(client_id, entity_type, entity_name, key)` e leem os fatos que outros agentes escreveram. Isso elimina:

- 🔴 Acoplamento temporal (agentes precisariam estar online ao mesmo tempo)
- 🔴 Perda de contexto em handoffs
- 🔴 Duplicação de informações entre agentes

### Ciclo de Vida Completo

```
1. Escrita → shared_memory_write / shared_memory_upsert
2. Leitura → shared_memory_read / shared_memory_list
3. Backup → backup_shared_memory.py (diário 02:00 UTC)
4. Soft-delete → prune_shared_memory.py (diário 03:00 UTC)
5. Hard-delete → prune_shared_memory.py (após 90 dias do soft-delete)
```

### Pre-flight / Post-flight Hooks

- **Pre-flight:** Antes de um agente executar, carrega seu contexto recente (`agent_metadata` + `agent_results`) da shared memory. Fail-open: se falhar, retorna contexto vazio.
- **Post-flight:** Após a execução, persiste resultados, decisões, descobertas e metadados da execução na shared memory. Fire-and-forget: nunca bloqueia o usuário.

### Onboarding Hook

Quando uma nova empresa é cadastrada, o hook `onboarding_shared_memory_hook.py` escreve o snapshot inicial (company profile, brand voice, goals) na shared memory — pronto para qualquer agente consumir.

---

## 🏗 Arquitetura dos Adapters

O adapter `blu_supabase_client` implementa:

```
get_supabase_client() → _SupabaseClient
    ├── .table("shared_business_memory") → _QueryBuilder
    │       .select("*")
    │       .eq("client_id", "...")
    │       .eq("entity_type", "skill")
    │       .order("updated_at", desc=True)
    │       .limit(10)
    │       .execute() → _SupabaseResponse(data=[...], count=N)
    │
    ├── .storage.from_("bucket").upload(path, file)
    └── .rpc("function_name", {...}).execute()
```

Tabelas SQLite criadas automaticamente:

- `shared_business_memory` — fatos principais (UNIQUE em client_id, entity_type, entity_name, key)
- `shared_memory_links` — links semânticos entre entidades
- `shared_business_memory_meta` — metadados operacionais (synthesis, dedup, kg)

---

## 📜 Licença

MIT. Use, estude, modifique, critique, distribua.  
Código original da Blu Platform, extraído e adaptado para a comunidade.

---

**Feito com ☕ e 🧠 por engenheiros que acreditam que agentes de IA precisam de memória compartilhada, não de chatrooms.**
