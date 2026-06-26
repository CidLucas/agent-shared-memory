"""
run.py — Entrypoint para Shared Memory Server com código original.

Este script:
1. Adiciona os adapters ao sys.path (substituem Supabase por SQLite)
2. Faz monkey-patch nos imports problemáticos
3. Carrega e executa o código ORIGINAL do memory_module.py

Uso:
    python run.py              # Inicia servidor MCP
    python run.py --init       # Só inicializa banco
    python run.py --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("run")

# ── Adiciona adapters ao sys.path ANTES de qualquer import ──────────
REPO_ROOT = Path(__file__).parent
ADAPTERS_DIR = str(REPO_ROOT / "adapters")
LIBS_DIR = str(REPO_ROOT / "libs")
SERVICES_DIR = str(REPO_ROOT / "services")

# Adiciona no início pra ter prioridade sobre imports quebrados
sys.path.insert(0, ADAPTERS_DIR)
sys.path.insert(0, LIBS_DIR)
sys.path.insert(0, SERVICES_DIR)

# ── Monkey-patch via sys.modules ────────────────────────────────────
# Esses módulos precisam existir ANTES do memory_module ser importado
import types


def _ensure_module(name: str) -> types.ModuleType:
    """Garante que um módulo existe em sys.modules (evita ImportError)."""
    if name not in sys.modules:
        mod = types.ModuleType(name)
        mod.__path__ = []
        sys.modules[name] = mod
    return sys.modules[name]


# Módulos que não existem mas são importados pelo código original
_ensure_module("blu_models")
_ensure_module("blu_models.ingestion")
_ensure_module("blu_models.ingestion.blu_schema")
_ensure_module("blu_models.knowledge_base_config")
_ensure_module("blu_models.blu_client_context")


def main() -> None:
    parser = argparse.ArgumentParser(description="Shared Memory MCP Server (código original)")
    parser.add_argument("--init", action="store_true", help="Só inicializa banco e sai")
    parser.add_argument("--host", default=os.getenv("HOST", "0.0.0.0"), help="Host")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")), help="Porta")
    parser.add_argument("--db", default=os.getenv("SHARED_MEMORY_DB", "shared_memory.db"), help="Caminho do SQLite")
    args = parser.parse_args()

    # Seta o path do banco
    os.environ["SHARED_MEMORY_DB"] = args.db

    # ── Importa o código original ───────────────────────────────────
    # Isso executa o memory_module.py que registra as tools no FastMCP
    # Os imports quebrados (blu_supabase_client, blu_auth, etc.) já
    # foram resolvidos pelos adapters em sys.path

    # Inicializa banco
    from blu_supabase_client import _get_connection
    _get_connection()
    logger.info("Banco inicializado: %s", args.db)

    if args.init:
        logger.info("Banco pronto. Saindo.")
        return

    # O memory_module registra as tools no FastMCP durante o import
    from tool_pool_api.server.tool_modules.memory_module import mcp

    logger.info("Iniciando Shared Memory MCP Server em %s:%s", args.host, args.port)
    mcp.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
