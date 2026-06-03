from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

BACKEND_DIR = Path(__file__).resolve().parents[1]


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


CONFIG: Dict[str, Any] = {
    "db_path": os.getenv(
        "PUMP_ENGINE_DB_PATH",
        str(BACKEND_DIR / "data" / "pump_engine_runs.jsonl"),
    ),
    "timeout_seconds": _float_env("PUMP_ENGINE_TIMEOUT_SECONDS", 12.0),
    "dexscreener_base_url": os.getenv("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/"),
    "goplus_base_url": os.getenv("GOPLUS_BASE_URL", "https://api.gopluslabs.io").rstrip("/"),
    "honeypot_base_url": os.getenv("HONEYPOT_BASE_URL", "https://api.honeypot.is").rstrip("/"),
    "tokensniffer_base_url": os.getenv("TOKENSNIFFER_BASE_URL", "https://tokensniffer.com").rstrip("/"),
    "honeypot_api_key": os.getenv("HONEYPOT_API_KEY", "").strip(),
    "tokensniffer_api_key": os.getenv("TOKENSNIFFER_API_KEY", "").strip(),
    "engine_version": os.getenv("PUMP_ENGINE_VERSION", "2026.03.31"),
    "max_batch_size": _int_env("PUMP_ENGINE_MAX_BATCH_SIZE", 25),
    "stable_quote_symbols": {
        "USDC",
        "USDT",
        "DAI",
        "FDUSD",
        "BUSD",
        "USDE",
        "USDY",
        "WETH",
        "ETH",
        "WBTC",
        "BTC",
        "WSOL",
        "SOL",
        "WBNB",
        "BNB",
    },
    "dex_chain_aliases": {
        "eth": "ethereum",
        "ethereum": "ethereum",
        "bsc": "bsc",
        "binance-smart-chain": "bsc",
        "bnb": "bsc",
        "base": "base",
        "sol": "solana",
        "solana": "solana",
        "arb": "arbitrum",
        "arbitrum": "arbitrum",
        "arbitrum-one": "arbitrum",
        "polygon": "polygon",
        "matic": "polygon",
        "polygon-pos": "polygon",
        "avax": "avalanche",
        "avalanche": "avalanche",
        "avalanche-c-chain": "avalanche",
        "optimism": "optimism",
        "optimistic-ethereum": "optimism",
    },
    "goplus_chain_ids": {
        "ethereum": "1",
        "bsc": "56",
        "polygon": "137",
        "arbitrum": "42161",
        "avalanche": "43114",
        "base": "8453",
        "optimism": "10",
    },
    "honeypot_chain_ids": {
        "ethereum": 1,
        "bsc": 56,
        "base": 8453,
    },
    "tokensniffer_chain_ids": {
        "ethereum": 1,
        "bsc": 56,
        "polygon": 137,
        "arbitrum": 42161,
        "avalanche": 43114,
        "base": 8453,
        "optimism": 10,
        "solana": 101,
    },
}


def normalize_chain(chain: str) -> str:
    normalized = (chain or "").strip().lower()
    return CONFIG["dex_chain_aliases"].get(normalized, normalized)


def get_goplus_chain_id(chain: str) -> Optional[str]:
    return CONFIG["goplus_chain_ids"].get(normalize_chain(chain))


def get_honeypot_chain_id(chain: str) -> Optional[int]:
    return CONFIG["honeypot_chain_ids"].get(normalize_chain(chain))


def get_tokensniffer_chain_id(chain: str) -> Optional[int]:
    return CONFIG["tokensniffer_chain_ids"].get(normalize_chain(chain))


def looks_like_quote_symbol(symbol: str) -> bool:
    return (symbol or "").strip().upper() in CONFIG["stable_quote_symbols"]
