"""Stub para blu_auth.mcp.auth_middleware."""
from __future__ import annotations
from typing import Any, Callable

def mcp_inject_client_id(get_context_service_fn: Callable) -> Any:
    """Decorator stub — apenas retorna a função sem modificar."""
    def decorator(func: Callable) -> Callable:
        return func
    return decorator
