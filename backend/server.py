"""
PumpRadar - FastAPI Backend
Crypto pump/dump signal analyzer with AI, LunarCrush & CoinGecko
"""
# NOTICE FOR ANY AI/AGENT WORKING IN THIS REPO:
# Do not modify the frontend unless the user explicitly asks for it in the current conversation.
# If unsure, stop and ask before touching anything under frontend/.
import os
import asyncio
import logging
import uuid
import re
from collections import Counter
import base64
import hmac
import requests
import hashlib
import secrets
import struct
import httpx
import stripe
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Annotated, Tuple
from urllib.parse import quote, unquote

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Depends, status, Request, Response, Cookie
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr, Field
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from passlib.context import CryptContext
from jose import JWTError, jwt
import resend
from crypto_pump_engine.pipeline import PumpEngineError
from crypto_pump_engine.router import get_pipeline as get_pump_engine_pipeline, router as pump_engine_router
try:
    from telethon import TelegramClient, events
    from telethon.errors import SessionPasswordNeededError
except Exception:
    TelegramClient = None
    events = None
    SessionPasswordNeededError = Exception

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
MONGO_URL = os.environ["MONGO_URL"]
DB_NAME = os.environ["DB_NAME"]
JWT_SECRET = os.environ["JWT_SECRET"]
JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINUTES = int(os.environ.get("JWT_EXPIRE_MINUTES", "10080"))
RESEND_API_KEY = os.environ["RESEND_API_KEY"]
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "noreply@arbitrajz.com")
LUNARCRUSH_API_KEY = os.environ["LUNARCRUSH_API_KEY"]
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "").strip()
OPENROUTER_BASE_URL = os.environ.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").strip()
PUMP_ENGINE_AI_MODEL = os.environ.get("PUMP_ENGINE_AI_MODEL", "openai/gpt-4.1-mini").strip()
AI_PROVIDER_PRIMARY = os.environ.get("AI_PROVIDER_PRIMARY", "openrouter").strip().lower()
STRIPE_API_KEY = os.environ["STRIPE_API_KEY"]
APP_URL = os.environ.get("APP_URL", "http://localhost:3000")
LOGO_URL = f"{APP_URL}/logo-pumpradar.png"
TELEGRAM_API_ID = os.environ.get("TELEGRAM_API_ID", "").strip()
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "").strip()
TELEGRAM_PHONE = os.environ.get("TELEGRAM_PHONE", "").strip()
TELEGRAM_SESSION_NAME = os.environ.get("TELEGRAM_SESSION_NAME", "pumpradar-telegram").strip() or "pumpradar-telegram"
TELEGRAM_LIVE_ENABLED = os.environ.get("TELEGRAM_LIVE_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
TELEGRAM_EARLY_SIGNAL_HOURS = 12
TELEGRAM_EARLY_SIGNAL_LIMIT = 15
X_API_KEY = os.environ.get("X_API_KEY", "").strip()
X_API_SECRET = os.environ.get("X_API_SECRET", "").strip()
X_BEARER_TOKEN = unquote(os.environ.get("X_BEARER_TOKEN", "").strip())
X_API_BASE = os.environ.get("X_API_BASE", "https://api.x.com/2").strip() or "https://api.x.com/2"
SUPER_ADMIN_EMAIL = os.environ.get("SUPER_ADMIN_EMAIL", "vault@pump.arbitrajz.com").strip().lower()
SUPER_ADMIN_PASSWORD = os.environ.get("SUPER_ADMIN_PASSWORD", "")
SUPER_ADMIN_TOTP_SECRET = os.environ.get("SUPER_ADMIN_TOTP_SECRET", "")
SUPER_ADMIN_ISSUER = os.environ.get("SUPER_ADMIN_ISSUER", "PumpRadar Super Admin")
SUPER_ADMIN_TOKEN_EXPIRE_HOURS = int(os.environ.get("SUPER_ADMIN_TOKEN_EXPIRE_HOURS", "12"))


def get_primary_ai_config() -> dict:
    """Return primary OpenAI-compatible AI configuration.

    Priority:
    - OpenRouter if configured
    - OpenAI direct if configured
    """
    if AI_PROVIDER_PRIMARY == "openai" and OPENAI_API_KEY:
        return {
            "provider": "openai",
            "api_key": OPENAI_API_KEY,
            "base_url": "https://api.openai.com/v1",
            "model": os.environ.get("OPENAI_MODEL", "gpt-4.1-mini").strip(),
        }

    if OPENROUTER_API_KEY:
        return {
            "provider": "openrouter",
            "api_key": OPENROUTER_API_KEY,
            "base_url": OPENROUTER_BASE_URL.rstrip("/"),
            "model": PUMP_ENGINE_AI_MODEL,
        }

    if OPENAI_API_KEY:
        return {
            "provider": "openai",
            "api_key": OPENAI_API_KEY,
            "base_url": "https://api.openai.com/v1",
            "model": os.environ.get("OPENAI_MODEL", "gpt-4.1-mini").strip(),
        }

    return {
        "provider": "none",
        "api_key": "",
        "base_url": "",
        "model": "",
    }


async def call_openai_compatible_text(
    *,
    system_instruction: str,
    user_prompt: str,
    temperature: float = 0.2,
    max_tokens: int = 1600,
) -> dict:
    """Call OpenAI-compatible chat completion endpoint and return text.

    This does not replace Gemini yet. It is a primary-provider wrapper ready for controlled migration.
    """
    cfg = get_primary_ai_config()
    if not cfg.get("api_key") or not cfg.get("base_url") or not cfg.get("model"):
        return {
            "ok": False,
            "provider": cfg.get("provider"),
            "error": "primary_ai_not_configured",
            "text": "",
        }

    url = f"{cfg['base_url']}/chat/completions"
    headers = {
        "Authorization": f"Bearer {cfg['api_key']}",
        "Content-Type": "application/json",
    }

    if cfg.get("provider") == "openrouter":
        headers["HTTP-Referer"] = APP_URL
        headers["X-Title"] = "PumpRadar"

    payload = {
        "model": cfg["model"],
        "messages": [
            {"role": "system", "content": system_instruction or ""},
            {"role": "user", "content": user_prompt or ""},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    try:
        def _request():
            return requests.post(url, headers=headers, json=payload, timeout=45)

        resp = await asyncio.to_thread(_request)
        if resp.status_code >= 400:
            return {
                "ok": False,
                "provider": cfg.get("provider"),
                "status_code": resp.status_code,
                "error": resp.text[:800],
                "text": "",
            }

        data = resp.json() or {}
        choices = data.get("choices") or []
        text = ""
        if choices:
            text = ((choices[0].get("message") or {}).get("content") or "").strip()

        return {
            "ok": bool(text),
            "provider": cfg.get("provider"),
            "model": cfg.get("model"),
            "text": text,
            "raw": data,
        }
    except Exception as exc:
        return {
            "ok": False,
            "provider": cfg.get("provider"),
            "error": str(exc),
            "text": "",
        }


async def call_openai_compatible_json(
    *,
    system_instruction: str,
    user_prompt: str,
    temperature: float = 0.1,
    max_tokens: int = 1800,
) -> dict:
    """Call primary OpenAI-compatible provider and parse strict JSON."""
    import json as json_lib

    result = await call_openai_compatible_text(
        system_instruction=system_instruction,
        user_prompt=user_prompt,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    if not result.get("ok"):
        return {
            **result,
            "json": None,
        }

    text_response = (result.get("text") or "").strip()
    if text_response.startswith("```"):
        text_response = "\n".join(
            line for line in text_response.splitlines()
            if not line.strip().startswith("```")
        ).strip()

    try:
        parsed = json_lib.loads(text_response)
        return {
            **result,
            "json": parsed,
        }
    except Exception as exc:
        return {
            **result,
            "ok": False,
            "error": f"json_parse_failed: {exc}",
            "json": None,
        }



# Configure Stripe
stripe.api_key = STRIPE_API_KEY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)  # nu loga URL-urile (ascunde api-key Helius etc.)
EXCHANGE_METADATA_CACHE: Dict[str, dict] = {}
COIN_TICKERS_CACHE: Dict[str, List[dict]] = {}
X_INTELLIGENCE_CACHE: Dict[str, dict] = {}
DASHBOARD_PAYLOAD_CACHE: Dict[str, dict] = {}
AI_DECISION_SIGNALS_CACHE: Dict[str, dict] = {}
COIN_DETAIL_CACHE: Dict[str, dict] = {}
TELEGRAM_CONSENSUS_CACHE: Dict[str, dict] = {}
CROSS_PLATFORM_CACHE: Dict[str, dict] = {}
NEW_ALGORITHM_SIGNALS_CACHE: Dict[str, dict] = {}
TELEGRAM_SIGNAL_MAP_CACHE: Dict[str, dict] = {}
TELEGRAM_CALIBRATION_CACHE: Dict[str, dict] = {}
SOCIAL_INTELLIGENCE_CACHE: Dict[str, dict] = {}
COIN_MARKET_CACHE: Dict[str, dict] = {}
COIN_CHART_CACHE: Dict[str, dict] = {}
COIN_EXTENDED_DETAILS_CACHE: Dict[str, dict] = {}
HOLDER_DISTRIBUTION_CACHE: Dict[str, dict] = {}
GOPLUS_SECURITY_CACHE: Dict[str, dict] = {}
GOPLUS_RUGPULL_CACHE: Dict[str, dict] = {}
ORDERBOOK_CACHE: Dict[str, dict] = {}
DERIVATIVES_CACHE: Dict[str, dict] = {}
CASE_REPLAY_CACHE: Dict[str, dict] = {}
SIGNAL_SCAN_LOCK = asyncio.Lock()
SIGNAL_SCAN_STATE: Dict[str, Any] = {
    "running": False,
    "trigger": None,
    "started_at": None,
    "finished_at": None,
    "last_snapshot_at": None,
    "last_error": None,
    "last_result": None,
}
telegram_client: Any = None
telegram_listener_task: Optional[asyncio.Task] = None
telegram_auth_state: Dict[str, Any] = {}
COINGECKO_NETWORK_MAP = {
    "ethereum": "eth",
    "binance-smart-chain": "bsc",
    "polygon-pos": "polygon_pos",
    "arbitrum-one": "arbitrum",
    "optimistic-ethereum": "optimism",
    "avalanche": "avax",
    "base": "base",
    "solana": "solana",
}
GOPLUS_CHAIN_MAP = {
    "ethereum": "1",
    "binance-smart-chain": "56",
    "polygon-pos": "137",
    "arbitrum-one": "42161",
    "optimistic-ethereum": "10",
    "avalanche": "43114",
    "base": "8453",
}

resend.api_key = RESEND_API_KEY

# ─────────────────────────────────────────────
# App & DB
# ─────────────────────────────────────────────
app = FastAPI(title="PumpRadar API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(pump_engine_router)

client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]

# --- on-chain intelligence module (read-only, separate writers) ---
from onchain_routes import router as onchain_router, set_db as _set_onchain_db
_set_onchain_db(db)
app.include_router(onchain_router)

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer = HTTPBearer(auto_error=False)

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def api_ok(data: Any) -> dict:
    return {"success": True, "data": data}

def api_err(msg: str, code: str = "ERROR") -> dict:
    return {"success": False, "error": {"code": code, "message": msg}}

def build_signal_scan_status() -> dict:
    def to_iso(value: Any) -> Optional[str]:
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    return {
        "running": bool(SIGNAL_SCAN_STATE.get("running")),
        "trigger": SIGNAL_SCAN_STATE.get("trigger"),
        "started_at": to_iso(SIGNAL_SCAN_STATE.get("started_at")),
        "finished_at": to_iso(SIGNAL_SCAN_STATE.get("finished_at")),
        "last_snapshot_at": to_iso(SIGNAL_SCAN_STATE.get("last_snapshot_at")),
        "last_error": SIGNAL_SCAN_STATE.get("last_error"),
        "last_result": SIGNAL_SCAN_STATE.get("last_result"),
    }

def get_memory_cache(cache: Dict[str, dict], key: str, ttl_seconds: int) -> Optional[Any]:
    now = datetime.now(timezone.utc)
    cached = cache.get(key)
    if not cached:
        return None
    cached_at = cached.get("timestamp")
    if not isinstance(cached_at, datetime):
        return None
    if (now - cached_at).total_seconds() >= ttl_seconds:
        cache.pop(key, None)
        return None
    return cached.get("data")

def set_memory_cache(cache: Dict[str, dict], key: str, data: Any) -> Any:
    cache[key] = {
        "timestamp": datetime.now(timezone.utc),
        "data": data,
    }
    return data

def looks_like_placeholder(value: str, prefix: str) -> bool:
    return not value or value.startswith(f"YOUR_{prefix}") or value.endswith("_HERE")

def hash_password(p: str) -> str:
    return pwd_ctx.hash(p)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)

def create_token(
    user_id: str,
    email: str,
    expires_delta: Optional[timedelta] = None,
    token_type: str = "access",
) -> str:
    exp = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=JWT_EXPIRE_MINUTES))
    payload = {
        "sub": user_id,
        "email": email,
        "exp": exp,
        "type": token_type,
        "jti": str(uuid.uuid4()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def decode_token(token: str) -> dict:
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])

def normalize_totp_secret(secret: str) -> str:
    return re.sub(r"[^A-Z2-7]", "", secret.upper())

def build_totp_code(secret: str, ts: Optional[int] = None, interval_seconds: int = 30) -> str:
    normalized = normalize_totp_secret(secret)
    if not normalized:
        return ""
    padding = "=" * (-len(normalized) % 8)
    key = base64.b32decode(normalized + padding, casefold=True)
    timestamp = ts or int(datetime.now(timezone.utc).timestamp())
    counter = timestamp // interval_seconds
    digest = hmac.new(key, counter.to_bytes(8, "big"), hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    binary = struct.unpack(">I", digest[offset:offset + 4])[0] & 0x7FFFFFFF
    return str(binary % 1_000_000).zfill(6)

def verify_totp_code(secret: str, code: str, window: int = 1) -> bool:
    normalized_code = re.sub(r"\D", "", code or "")
    if len(normalized_code) != 6:
        return False
    now_ts = int(datetime.now(timezone.utc).timestamp())
    for step in range(-window, window + 1):
        if build_totp_code(secret, ts=now_ts + (step * 30)) == normalized_code:
            return True
    return False

def build_super_admin_setup_uri(email: str, secret: str) -> str:
    account_name = quote(email)
    issuer = quote(SUPER_ADMIN_ISSUER)
    return f"otpauth://totp/{issuer}:{account_name}?secret={secret}&issuer={issuer}&algorithm=SHA1&digits=6&period=30"

async def ensure_super_admin_seeded() -> Optional[dict]:
    if not SUPER_ADMIN_EMAIL:
        return None

    existing = await db.super_admin_accounts.find_one({"email": SUPER_ADMIN_EMAIL})
    normalized_secret = normalize_totp_secret(SUPER_ADMIN_TOTP_SECRET)
    if not SUPER_ADMIN_PASSWORD or not normalized_secret:
        logger.warning("Super admin credentials not fully configured in environment")
        return None

    if existing:
        updates: Dict[str, Any] = {}
        if not verify_password(SUPER_ADMIN_PASSWORD, existing.get("password_hash", "")):
            updates["password_hash"] = hash_password(SUPER_ADMIN_PASSWORD)
        if existing.get("totp_secret") != normalized_secret:
            updates["totp_secret"] = normalized_secret
            updates["totp_uri"] = build_super_admin_setup_uri(SUPER_ADMIN_EMAIL, normalized_secret)
        if existing.get("active") is not True:
            updates["active"] = True
        if updates:
            updates["updated_at"] = datetime.now(timezone.utc)
            await db.super_admin_accounts.update_one({"_id": existing["_id"]}, {"$set": updates})
            existing = await db.super_admin_accounts.find_one({"_id": existing["_id"]})
        return existing

    super_admin_doc = {
        "email": SUPER_ADMIN_EMAIL,
        "password_hash": hash_password(SUPER_ADMIN_PASSWORD),
        "totp_secret": normalized_secret,
        "totp_uri": build_super_admin_setup_uri(SUPER_ADMIN_EMAIL, normalized_secret),
        "active": True,
        "failed_attempts": 0,
        "locked_until": None,
        "last_login_at": None,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }
    result = await db.super_admin_accounts.insert_one(super_admin_doc)
    super_admin_doc["_id"] = result.inserted_id
    return super_admin_doc

async def get_current_super_admin(creds: HTTPAuthorizationCredentials = Depends(bearer)) -> dict:
    if not creds:
        raise HTTPException(status_code=401, detail=api_err("Super admin authentication required", "SUPER_ADMIN_AUTH_REQUIRED"))

    try:
        payload = decode_token(creds.credentials)
        if payload.get("type") != "super_admin":
            raise HTTPException(status_code=401, detail=api_err("Invalid super admin session", "INVALID_SUPER_ADMIN_TOKEN"))

        admin_id = payload.get("sub")
        admin_doc = await db.super_admin_accounts.find_one({"_id": ObjectId(admin_id), "active": True})
        if not admin_doc:
            raise HTTPException(status_code=401, detail=api_err("Super admin account not found", "SUPER_ADMIN_NOT_FOUND"))
        return admin_doc
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail=api_err("Invalid super admin session", "INVALID_SUPER_ADMIN_TOKEN"))

def doc_to_user(doc: dict) -> dict:
    if not doc:
        return {}
    return {
        "id": str(doc["_id"]),
        "email": doc["email"],
        "name": doc.get("name", ""),
        "roles": doc.get("roles", ["viewer"]),
        "avatar": doc.get("avatar"),
        "emailVerified": doc.get("email_verified", False),
        "subscription": doc.get("subscription", "free"),
        "subscriptionExpiry": doc.get("subscription_expiry"),
        "createdAt": doc.get("created_at", "").isoformat() if isinstance(doc.get("created_at"), datetime) else doc.get("created_at", ""),
    }

async def get_current_user(creds: HTTPAuthorizationCredentials = Depends(bearer)) -> dict:
    if not creds:
        raise HTTPException(status_code=401, detail="Not authenticated")

    token = creds.credentials

    if token.startswith("user-access-"):
        legacy_auth_id = token.replace("user-access-", "", 1)
        user = await db.users.find_one({"legacy_auth_id": legacy_auth_id})
        if not user:
            raise HTTPException(status_code=401, detail="Invalid token")
        return user

    try:
        payload = decode_token(token)
        if payload.get("type") != "access":
            raise HTTPException(status_code=401, detail="Invalid token")
        user_id = payload.get("sub")
        user = await db.users.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return user
    except (JWTError, Exception) as e:
        raise HTTPException(status_code=401, detail="Invalid token")

async def get_optional_user(creds: HTTPAuthorizationCredentials = Depends(bearer)) -> Optional[dict]:
    if not creds:
        return None

    token = creds.credentials

    try:
        if token.startswith("user-access-"):
            legacy_auth_id = token.replace("user-access-", "", 1)
            return await db.users.find_one({"legacy_auth_id": legacy_auth_id})

        payload = decode_token(token)
        if payload.get("type") != "access":
            return None
        user_id = payload.get("sub")
        return await db.users.find_one({"_id": ObjectId(user_id)})
    except Exception:
        return None

# ─────────────────────────────────────────────
# Email helpers
# ─────────────────────────────────────────────
async def send_verification_email(email: str, name: str, token: str):
    verify_url = f"{APP_URL}/auth/verify-email?token={token}"
    html = f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;background:#0f172a;border-radius:12px">
      <div style="text-align:center;margin-bottom:24px">
        <img src="{LOGO_URL}" alt="PumpRadar" style="width:64px;height:64px;border-radius:12px" />
        <h2 style="color:#fff;margin:16px 0 0 0">Verify your email to continue</h2>
      </div>
      <div style="background:#1e293b;padding:20px;border-radius:8px;color:#fff">
        <p style="margin:0 0 16px 0">Hi {name},</p>
        <p style="margin:0 0 16px 0;color:#94a3b8">Please verify your email address to continue to PumpRadar's secure 7-day trial setup.</p>
        <p style="margin:0 0 20px 0;color:#94a3b8">After verification, we will sign you in automatically and send you to Stripe checkout, where the client adds a card to start the 7-day free trial.</p>
        <div style="text-align:center">
          <a href="{verify_url}" style="background:linear-gradient(135deg,#10b981,#059669);color:#fff;padding:14px 32px;border-radius:8px;text-decoration:none;display:inline-block;font-weight:bold">
            Verify Email And Continue
          </a>
        </div>
        <div style="margin-top:20px;padding:14px;border-radius:8px;background:#0f172a;border:1px solid #334155">
          <p style="margin:0 0 8px 0;color:#cbd5e1;font-size:13px">If the button does not appear or does not work, open this verification link manually:</p>
          <p style="margin:0;word-break:break-all">
            <a href="{verify_url}" style="color:#38bdf8;font-size:13px;text-decoration:underline">{verify_url}</a>
          </p>
        </div>
        <p style="color:#64748b;font-size:12px;margin:20px 0 0 0;text-align:center">This link expires in 24 hours.</p>
      </div>
    </div>"""
    text = (
        f"Hi {name},\n\n"
        "Please verify your email to continue to PumpRadar's secure 7-day trial setup.\n\n"
        "Open this link to verify your email and continue:\n"
        f"{verify_url}\n\n"
        "After verification, you will be signed in automatically and sent to secure card setup for the 7-day free trial.\n"
        "This link expires in 24 hours.\n"
    )
    try:
        await asyncio.to_thread(resend.Emails.send, {
            "from": f"PumpRadar <{SENDER_EMAIL}>",
            "to": [email],
            "subject": "Verify your email to continue your PumpRadar trial",
            "html": html,
            "text": text,
        })
    except Exception as e:
        logger.error(f"Email send error: {e}")

async def send_reset_email(email: str, token: str):
    reset_url = f"{APP_URL}/auth/reset-password?token={token}"
    html = f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;background:#0f172a;border-radius:12px">
      <div style="text-align:center;margin-bottom:24px">
        <img src="{LOGO_URL}" alt="PumpRadar" style="width:64px;height:64px;border-radius:12px" />
        <h2 style="color:#fff;margin:16px 0 0 0">Password Reset</h2>
      </div>
      <div style="background:#1e293b;padding:20px;border-radius:8px;color:#fff">
        <p style="margin:0 0 16px 0;color:#94a3b8">You requested a password reset. Click the button below:</p>
        <div style="text-align:center">
          <a href="{reset_url}" style="background:linear-gradient(135deg,#6366f1,#4f46e5);color:#fff;padding:14px 32px;border-radius:8px;text-decoration:none;display:inline-block;font-weight:bold">
            Reset Password
          </a>
        </div>
        <p style="color:#64748b;font-size:12px;margin:20px 0 0 0;text-align:center">This link expires in 1 hour. If you didn't request this, ignore this email.</p>
      </div>
    </div>"""
    try:
        await asyncio.to_thread(resend.Emails.send, {
            "from": f"PumpRadar <{SENDER_EMAIL}>",
            "to": [email],
            "subject": "Password Reset - PumpRadar",
            "html": html,
        })
    except Exception as e:
        logger.error(f"Reset email error: {e}")

async def send_trial_started_email(email: str, name: str, plan_name: str, trial_end: datetime):
    billing_dt = trial_end.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;background:#0f172a;border-radius:12px;color:#fff">
      <div style="text-align:center;margin-bottom:24px">
        <img src="{LOGO_URL}" alt="PumpRadar" style="width:64px;height:64px;border-radius:12px" />
        <h2 style="margin:16px 0 0 0">Your 7-day trial is live</h2>
      </div>
      <div style="background:#1e293b;padding:20px;border-radius:8px">
        <p>Hi {name or 'Trader'},</p>
        <p>Your card-backed trial for the <strong>{plan_name}</strong> plan has started.</p>
        <p>The trial ends on <strong>{billing_dt}</strong>. If you keep the subscription active, Stripe will automatically start the paid plan after the trial.</p>
        <p>Billing details were collected securely by Stripe during checkout.</p>
        <div style="text-align:center;margin-top:20px">
          <a href="{APP_URL}/dashboard" style="background:linear-gradient(135deg,#10b981,#059669);color:#fff;padding:14px 32px;border-radius:8px;text-decoration:none;display:inline-block;font-weight:bold">
            Open PumpRadar
          </a>
        </div>
      </div>
    </div>"""
    try:
        await asyncio.to_thread(resend.Emails.send, {
            "from": f"PumpRadar <{SENDER_EMAIL}>",
            "to": [email],
            "subject": f"Your PumpRadar {plan_name} trial has started",
            "html": html,
        })
    except Exception as e:
        logger.error(f"Trial started email error: {e}")

async def send_trial_reminder_email(email: str, name: str, plan_name: str, trial_end: datetime):
    billing_dt = trial_end.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;background:#0f172a;border-radius:12px;color:#fff">
      <div style="text-align:center;margin-bottom:24px">
        <img src="{LOGO_URL}" alt="PumpRadar" style="width:64px;height:64px;border-radius:12px" />
        <h2 style="margin:16px 0 0 0">Your trial ends soon</h2>
      </div>
      <div style="background:#1e293b;padding:20px;border-radius:8px">
        <p>Hi {name or 'Trader'},</p>
        <p>Your <strong>{plan_name}</strong> trial ends on <strong>{billing_dt}</strong>.</p>
        <p>If you do nothing, the subscription will continue and Stripe will charge the saved payment method. Cancel before the deadline if you do not want the paid plan.</p>
        <div style="text-align:center;margin-top:20px">
          <a href="{APP_URL}/subscription" style="background:linear-gradient(135deg,#6366f1,#4f46e5);color:#fff;padding:14px 32px;border-radius:8px;text-decoration:none;display:inline-block;font-weight:bold">
            Review Subscription
          </a>
        </div>
      </div>
    </div>"""
    try:
        await asyncio.to_thread(resend.Emails.send, {
            "from": f"PumpRadar <{SENDER_EMAIL}>",
            "to": [email],
            "subject": f"Your PumpRadar trial for {plan_name} ends soon",
            "html": html,
        })
    except Exception as e:
        logger.error(f"Trial reminder email error: {e}")

async def send_subscription_activated_email(email: str, name: str, plan_name: str, expiry: datetime):
    expiry_dt = expiry.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;background:#0f172a;border-radius:12px;color:#fff">
      <div style="text-align:center;margin-bottom:24px">
        <img src="{LOGO_URL}" alt="PumpRadar" style="width:64px;height:64px;border-radius:12px" />
        <h2 style="margin:16px 0 0 0">Subscription activated</h2>
      </div>
      <div style="background:#1e293b;padding:20px;border-radius:8px">
        <p>Hi {name or 'Trader'},</p>
        <p>Your <strong>{plan_name}</strong> subscription is now active and billing has been confirmed.</p>
        <p>Your current access window runs until <strong>{expiry_dt}</strong>.</p>
        <div style="text-align:center;margin-top:20px">
          <a href="{APP_URL}/dashboard" style="background:linear-gradient(135deg,#10b981,#059669);color:#fff;padding:14px 32px;border-radius:8px;text-decoration:none;display:inline-block;font-weight:bold">
            Open Dashboard
          </a>
        </div>
      </div>
    </div>"""
    try:
        await asyncio.to_thread(resend.Emails.send, {
            "from": f"PumpRadar <{SENDER_EMAIL}>",
            "to": [email],
            "subject": f"Your PumpRadar {plan_name} subscription is active",
            "html": html,
        })
    except Exception as e:
        logger.error(f"Subscription activation email error: {e}")

# ─────────────────────────────────────────────
# AUTH MODELS
# ─────────────────────────────────────────────
class LoginDTO(BaseModel):
    email: EmailStr
    password: str
    remember: Optional[bool] = False

class RegisterDTO(BaseModel):
    email: EmailStr
    password: str
    name: str
    confirmPassword: Optional[str] = None

class ForgotPasswordDTO(BaseModel):
    email: EmailStr

class ResetPasswordDTO(BaseModel):
    token: str
    password: str
    confirmPassword: Optional[str] = None

class VerifyEmailDTO(BaseModel):
    token: str

class ResendVerificationDTO(BaseModel):
    email: EmailStr

class SuperAdminLoginDTO(BaseModel):
    email: EmailStr
    password: str
    totpCode: str = Field(..., min_length=6, max_length=8)

# ─────────────────────────────────────────────
# AUTH ENDPOINTS
# ─────────────────────────────────────────────
@app.post("/api/auth/register")
async def register(dto: RegisterDTO):
    existing = await db.users.find_one({"email": dto.email.lower()})
    if existing:
        raise HTTPException(status_code=400, detail=api_err("Email already registered", "EMAIL_EXISTS"))
    
    verify_token = secrets.token_urlsafe(32)
    verify_expiry = datetime.now(timezone.utc) + timedelta(hours=24)
    
    user_doc = {
        "email": dto.email.lower(),
        "name": dto.name,
        "password_hash": hash_password(dto.password),
        "roles": ["viewer"],
        "email_verified": False,
        "verify_token": verify_token,
        "verify_token_expiry": verify_expiry,
        "subscription": "free",
        "subscription_expiry": None,
        "created_at": datetime.now(timezone.utc),
    }
    result = await db.users.insert_one(user_doc)
    user_doc["_id"] = result.inserted_id

    # Send verification email
    asyncio.create_task(send_verification_email(dto.email, dto.name, verify_token))

    return api_ok({
        "user": doc_to_user(user_doc),
        "message": "Account created! Please check your email to verify.",
    })

@app.post("/api/auth/login")
async def login(dto: LoginDTO):
    user = await db.users.find_one({"email": dto.email.lower()})
    if not user or not verify_password(dto.password, user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail=api_err("Incorrect email or password", "INVALID_CREDENTIALS"))

    if not user.get("email_verified"):
        raise HTTPException(status_code=403, detail=api_err("Please verify your email before signing in", "EMAIL_NOT_VERIFIED"))

    expire = timedelta(days=30) if dto.remember else timedelta(minutes=JWT_EXPIRE_MINUTES)
    access_token = create_token(str(user["_id"]), user["email"], expire, token_type="access")
    refresh_token = create_token(str(user["_id"]), user["email"], timedelta(days=30), token_type="refresh")
    
    return api_ok({
        "user": doc_to_user(user),
        "accessToken": access_token,
        "refreshToken": refresh_token,
    })

@app.post("/api/auth/forgot-password")
async def forgot_password(dto: ForgotPasswordDTO):
    user = await db.users.find_one({"email": dto.email.lower()})
    if user:
        reset_token = secrets.token_urlsafe(32)
        reset_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        await db.users.update_one(
            {"_id": user["_id"]},
            {"$set": {"reset_token": reset_token, "reset_token_expiry": reset_expiry}}
        )
        asyncio.create_task(send_reset_email(dto.email, reset_token))
    
    return api_ok({"message": "If this email exists, you will receive reset instructions."})

@app.post("/api/auth/reset-password")
async def reset_password(dto: ResetPasswordDTO):
    user = await db.users.find_one({
        "reset_token": dto.token,
        "reset_token_expiry": {"$gt": datetime.now(timezone.utc)}
    })
    if not user:
        raise HTTPException(status_code=400, detail=api_err("Invalid or expired token", "INVALID_TOKEN"))
    
    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"password_hash": hash_password(dto.password)}, "$unset": {"reset_token": "", "reset_token_expiry": ""}}
    )
    return api_ok({"message": "Password has been reset successfully."})

@app.post("/api/auth/verify-email")
async def verify_email(dto: VerifyEmailDTO):
    user = await db.users.find_one({
        "verify_token": dto.token,
        "verify_token_expiry": {"$gt": datetime.now(timezone.utc)}
    })
    if not user:
        raise HTTPException(status_code=400, detail=api_err("Invalid or expired token", "INVALID_TOKEN"))
    
    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"email_verified": True}, "$unset": {"verify_token": "", "verify_token_expiry": ""}}
    )
    user["email_verified"] = True
    access_token = create_token(str(user["_id"]), user["email"], token_type="access")
    refresh_token = create_token(str(user["_id"]), user["email"], timedelta(days=30), token_type="refresh")
    return api_ok({
        "message": "Email verified successfully!",
        "user": doc_to_user(user),
        "accessToken": access_token,
        "refreshToken": refresh_token,
    })

@app.post("/api/auth/logout")
async def logout(user=Depends(get_current_user)):
    return api_ok({"message": "Logged out successfully"})

@app.get("/api/auth/me")
async def get_me(user=Depends(get_current_user)):
    return api_ok({"user": doc_to_user(user)})

@app.post("/api/auth/refresh")
async def refresh_token(request: Request):
    body = await request.json()
    refresh = body.get("refreshToken", "")
    try:
        payload = decode_token(refresh)
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Invalid refresh token")
        user_id = payload.get("sub")
        user = await db.users.find_one({"_id": ObjectId(user_id)})
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        new_access = create_token(str(user["_id"]), user["email"], token_type="access")
        new_refresh = create_token(str(user["_id"]), user["email"], timedelta(days=30), token_type="refresh")
        return api_ok({"accessToken": new_access, "refreshToken": new_refresh})
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

@app.post("/api/auth/resend-verification")
async def resend_verification(dto: ResendVerificationDTO):
    """Resend verification email without requiring login."""
    user = await db.users.find_one({"email": dto.email.lower()})
    if not user:
        return api_ok({"message": "If this email exists, a verification email has been sent."})

    if user.get("email_verified"):
        return api_ok({"message": "Email already verified"})

    verify_token = secrets.token_urlsafe(32)
    verify_expiry = datetime.now(timezone.utc) + timedelta(hours=24)

    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"verify_token": verify_token, "verify_token_expiry": verify_expiry}}
    )

    asyncio.create_task(send_verification_email(user["email"], user.get("name", ""), verify_token))

    return api_ok({"message": "Verification email sent! Please check your inbox."})

# ─────────────────────────────────────────────
# SUPER ADMIN AUTH
# ─────────────────────────────────────────────
@app.post("/api/super-admin/login")
async def super_admin_login(dto: SuperAdminLoginDTO):
    admin_doc = await ensure_super_admin_seeded()
    if not admin_doc or dto.email.lower() != admin_doc.get("email"):
        raise HTTPException(status_code=401, detail=api_err("Invalid super admin credentials", "INVALID_SUPER_ADMIN_CREDENTIALS"))

    now = datetime.now(timezone.utc)
    locked_until = admin_doc.get("locked_until")
    if isinstance(locked_until, str):
        locked_until = datetime.fromisoformat(locked_until.replace("Z", "+00:00"))
    if locked_until and locked_until.tzinfo is None:
        locked_until = locked_until.replace(tzinfo=timezone.utc)

    if locked_until and locked_until > now:
        raise HTTPException(
            status_code=423,
            detail=api_err(
                f"Super admin access is temporarily locked until {locked_until.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
                "SUPER_ADMIN_LOCKED",
            ),
        )

    password_ok = verify_password(dto.password, admin_doc.get("password_hash", ""))
    totp_ok = verify_totp_code(admin_doc.get("totp_secret", ""), dto.totpCode)

    if not password_ok or not totp_ok:
        failed_attempts = int(admin_doc.get("failed_attempts", 0)) + 1
        update_doc: Dict[str, Any] = {
            "failed_attempts": failed_attempts,
            "updated_at": now,
        }
        if failed_attempts >= 5:
            update_doc["failed_attempts"] = 0
            update_doc["locked_until"] = now + timedelta(minutes=15)
        await db.super_admin_accounts.update_one(
            {"_id": admin_doc["_id"]},
            {"$set": update_doc},
        )
        raise HTTPException(status_code=401, detail=api_err("Invalid super admin credentials", "INVALID_SUPER_ADMIN_CREDENTIALS"))

    await db.super_admin_accounts.update_one(
        {"_id": admin_doc["_id"]},
        {"$set": {"failed_attempts": 0, "locked_until": None, "last_login_at": now, "updated_at": now}},
    )
    refreshed = await db.super_admin_accounts.find_one({"_id": admin_doc["_id"]})
    access_token = create_token(
        str(admin_doc["_id"]),
        admin_doc["email"],
        timedelta(hours=SUPER_ADMIN_TOKEN_EXPIRE_HOURS),
        token_type="super_admin",
    )

    return api_ok({
        "accessToken": access_token,
        "account": {
            "email": refreshed["email"],
            "issuer": SUPER_ADMIN_ISSUER,
            "lastLoginAt": refreshed.get("last_login_at").isoformat() if refreshed.get("last_login_at") else None,
        },
    })

@app.get("/api/super-admin/me")
async def super_admin_me(admin=Depends(get_current_super_admin)):
    return api_ok({
        "account": {
            "email": admin["email"],
            "issuer": SUPER_ADMIN_ISSUER,
            "lastLoginAt": admin.get("last_login_at").isoformat() if admin.get("last_login_at") else None,
        }
    })

# ─────────────────────────────────────────────
# GOOGLE OAUTH
# ─────────────────────────────────────────────
# Google OAuth session verification endpoint
GOOGLE_AUTH_URL = "https://demobackend.emergentagent.com/auth/v1/env/oauth/session-data"

class GoogleAuthDTO(BaseModel):
    session_id: str

@app.post("/api/auth/google")
async def google_auth(dto: GoogleAuthDTO, response: Response):
    """Exchange Google OAuth session_id for user session"""
    try:
        # Call Google Auth to get user data
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                GOOGLE_AUTH_URL,
                headers={"X-Session-ID": dto.session_id},
                timeout=10.0
            )
            if resp.status_code != 200:
                logger.error(f"Google Auth error: {resp.status_code} - {resp.text}")
                raise HTTPException(status_code=401, detail=api_err("Google authentication failed", "GOOGLE_AUTH_FAILED"))
            
            google_data = resp.json()
        
        email = google_data.get("email", "").lower()
        name = google_data.get("name", "")
        picture = google_data.get("picture", "")
        
        if not email:
            raise HTTPException(status_code=400, detail=api_err("No email from Google", "NO_EMAIL"))
        
        # Check if user exists
        user = await db.users.find_one({"email": email})
        
        if user:
            # Update existing user with Google info
            await db.users.update_one(
                {"_id": user["_id"]},
                {"$set": {
                    "name": name or user.get("name", ""),
                    "avatar": picture,
                    "email_verified": True,  # Google emails are verified
                    "google_id": google_data.get("id"),
                    "last_login": datetime.now(timezone.utc),
                }}
            )
            user = await db.users.find_one({"_id": user["_id"]})
        else:
            # Create new user with free access until checkout starts a trial
            user_doc = {
                "email": email,
                "name": name,
                "avatar": picture,
                "google_id": google_data.get("id"),
                "password_hash": "",  # No password for Google users
                "roles": ["viewer"],
                "email_verified": True,
                "subscription": "free",
                "subscription_expiry": None,
                "created_at": datetime.now(timezone.utc),
                "last_login": datetime.now(timezone.utc),
            }
            result = await db.users.insert_one(user_doc)
            user_doc["_id"] = result.inserted_id
            user = user_doc
        
        # Create JWT tokens
        access_token = create_token(str(user["_id"]), user["email"], token_type="access")
        refresh_token = create_token(str(user["_id"]), user["email"], timedelta(days=30), token_type="refresh")
        
        logger.info(f"Google auth successful for {email}")
        
        return api_ok({
            "user": doc_to_user(user),
            "accessToken": access_token,
            "refreshToken": refresh_token,
        })
        
    except HTTPException:
        raise
    except httpx.RequestError as e:
        logger.error(f"Google Auth upstream error: {e}")
        raise HTTPException(
            status_code=503,
            detail=api_err("Google authentication is temporarily unavailable", "GOOGLE_AUTH_UNAVAILABLE"),
        )
    except Exception as e:
        logger.error(f"Google auth error: {e}")
        raise HTTPException(status_code=500, detail=api_err("Google authentication failed", "GOOGLE_AUTH_FAILED"))

# ─────────────────────────────────────────────
# SUBSCRIPTION CHECK MIDDLEWARE
# ─────────────────────────────────────────────
async def check_subscription(user: dict) -> dict:
    """Check if user has active subscription. Returns subscription status."""
    subscription = user.get("subscription", "free")
    expiry = user.get("subscription_expiry")
    
    if subscription == "free":
        return {"active": False, "reason": "No active subscription"}
    
    if expiry:
        # Handle both datetime and string formats
        if isinstance(expiry, str):
            expiry = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        
        if expiry < datetime.now(timezone.utc):
            return {"active": False, "reason": "Subscription expired", "expired_at": expiry.isoformat()}
    
    return {"active": True, "subscription": subscription, "expires_at": expiry.isoformat() if expiry else None}

async def require_active_subscription(user=Depends(get_current_user)) -> dict:
    """Dependency that requires an active subscription"""
    status = await check_subscription(user)
    if not status["active"]:
        raise HTTPException(
            status_code=402,  # Payment Required
            detail=api_err(f"Subscription required: {status.get('reason', 'No active subscription')}", "SUBSCRIPTION_REQUIRED")
        )
    return user


# ─────────────────────────────────────────────
# CRYPTO DATA FETCHING
# ─────────────────────────────────────────────
CG_HEADERS = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}

CG_MARKETS_CACHE: Dict[str, dict] = {}

def get_coingecko_markets(per_page: int = 250, pages: int = 2) -> List[dict]:
    cache_key = f"cg_markets_{per_page}_{pages}"
    cached = get_memory_cache(CG_MARKETS_CACHE, cache_key, ttl_seconds=5400)
    if cached is not None:
        logger.info("CoinGecko markets served from cache")
        return cached
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        all_rows: List[dict] = []
        seen_ids: set[str] = set()
        for page in range(1, max(1, pages) + 1):
            params = {
                "vs_currency": "usd",
                "order": "volume_desc",
                "per_page": min(max(1, per_page), 250),
                "page": page,
                "price_change_percentage": "1h,24h,7d",
                "sparkline": "false",
            }
            r = requests.get(url, params=params, headers=CG_HEADERS, timeout=30)
            if r.status_code == 429:
                logger.warning("CoinGecko rate limit on markets page %s - switching to CoinPaprika fallback", page)
                return fetch_coinpaprika_markets(250)
            if r.status_code == 402 or r.status_code == 403:
                logger.warning("CoinGecko quota exhausted - switching to CoinPaprika fallback")
                return fetch_coinpaprika_markets(250)
            r.raise_for_status()
            page_rows = r.json() or []
            if not page_rows:
                break
            for row in page_rows:
                row_id = (row.get("id") or f"{row.get('symbol', '')}:{row.get('name', '')}").strip().lower()
                if not row_id or row_id in seen_ids:
                    continue
                seen_ids.add(row_id)
                all_rows.append(row)
        set_memory_cache(CG_MARKETS_CACHE, cache_key, all_rows)
        return all_rows
    except Exception as e:
        logger.error(f"CoinGecko error: {e}")
        return []

def fetch_geckoterminal_markets(limit: int = 250) -> List[dict]:
    """Fetch market data from GeckoTerminal as CoinGecko replacement."""
    import asyncio
    networks = ["solana", "eth", "bsc"]
    modes = ["trending", "new"]
    seen = set()
    rows = []
    for network in networks:
        for mode in modes:
            try:
                candidates = fetch_geckoterminal_pool_candidates(network=network, mode=mode, limit=50)
                for c in candidates:
                    symbol = (c.get("symbol") or "").upper()
                    if not symbol or symbol in seen:
                        continue
                    seen.add(symbol)
                    price_change = c.get("price_change_pct") or {}
                    volume = c.get("volume_usd") or {}
                    vol_h24 = float(volume.get("h24") or 0)
                    mcap = float(c.get("market_cap_usd") or c.get("fdv_usd") or 0)
                    if not vol_h24 or not mcap:
                        continue
                    rows.append({
                        "id": c.get("token_address") or symbol.lower(),
                        "symbol": symbol,
                        "name": c.get("name") or symbol,
                        "current_price": float(c.get("price_usd") or 0),
                        "market_cap": float(c.get("market_cap_usd") or c.get("fdv_usd") or c.get("reserve_usd") or 0),
                        "total_volume": vol_h24,
                        "price_change_percentage_1h_in_currency": float(price_change.get("h1") or 0),
                        "price_change_percentage_24h": float(price_change.get("h24") or 0),
                        "price_change_percentage_7d_in_currency": float(price_change.get("h6") or 0),
                        "source": "geckoterminal",
                        "network": network,
                        "pool_address": c.get("pool_address"),
                        "token_address": c.get("token_address"),
                    })
            except Exception as e:
                logger.warning(f"GeckoTerminal markets fetch error {network}/{mode}: {e}")
            import time; time.sleep(1)
    rows = [r for r in rows if r["market_cap"] >= 1000]
    rows.sort(key=lambda x: (x["total_volume"] / x["market_cap"] * 100) if x["market_cap"] > 0 else 0, reverse=True)
    logger.info(f"GeckoTerminal markets: {len(rows)} coins fetched")
    return rows[:limit]

def fetch_coinpaprika_markets(limit: int = 250) -> List[dict]:
    """Fallback market data from CoinPaprika (free, no rate limit)."""
    try:
        # Fetch more to filter out majors
        resp = requests.get(
            "https://api.coinpaprika.com/v1/tickers?limit=500",
            timeout=20,
        )
        if resp.status_code != 200:
            return []
        major_symbols = {"BTC", "ETH", "USDT", "USDC", "BNB", "XRP", "ADA", "DOGE", "SOL", "TRX", "STETH", "WBTC", "WETH", "DAI", "LEO", "SHIB", "LTC", "AVAX", "DOT", "LINK"}
        rows = []
        for item in resp.json():
            symbol = (item.get("symbol") or "").upper()
            rank = item.get("rank") or 999
            if symbol in major_symbols or rank <= 30:
                continue
            usd = (item.get("quotes") or {}).get("USD") or {}
            price = usd.get("price", 0)
            volume = usd.get("volume_24h", 0)
            mcap = usd.get("market_cap", 0)
            if not price or not volume or not mcap:
                continue
            vol_mcap = (volume / mcap * 100) if mcap > 0 else 0
            if vol_mcap < 3:
                continue
            rows.append({
                "id": item.get("id", ""),
                "symbol": symbol,
                "name": item.get("name", ""),
                "current_price": price,
                "market_cap": float(c.get("market_cap_usd") or c.get("fdv_usd") or c.get("reserve_usd") or 0),
                "total_volume": volume,
                "price_change_percentage_1h_in_currency": usd.get("percent_change_1h", 0),
                "price_change_percentage_24h": usd.get("percent_change_24h", 0),
                "price_change_percentage_7d_in_currency": usd.get("percent_change_7d", 0),
                "source": "coinpaprika",
            })
        # Sort by vol/mcap ratio descending
        rows.sort(key=lambda x: (x["total_volume"] / x["market_cap"] * 100) if x["market_cap"] > 0 else 0, reverse=True)
        rows = rows[:limit]
        logger.info(f"CoinPaprika fallback: {len(rows)} coins fetched")
        return rows
    except Exception as e:
        logger.error(f"CoinPaprika fallback error: {e}")
        return []

def get_coingecko_market_snapshot(symbol: str, preferred_name: Optional[str] = None, preferred_coin_id: Optional[str] = None) -> dict:
    resolved_coin_id = (preferred_coin_id or "").strip() or resolve_coingecko_coin_id(symbol, preferred_name=preferred_name)
    if not resolved_coin_id:
        return {}
    cache_key = f"{resolved_coin_id}::market"
    cached = get_memory_cache(COIN_MARKET_CACHE, cache_key, ttl_seconds=3600)
    if cached is not None:
        return cached
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "ids": resolved_coin_id,
                "price_change_percentage": "1h,24h,7d",
            },
            headers=CG_HEADERS,
            timeout=15,
        )
        if resp.status_code == 200 and resp.json():
            return set_memory_cache(COIN_MARKET_CACHE, cache_key, (resp.json() or [])[0])
    except Exception as e:
        logger.error("CoinGecko market snapshot error for %s: %s", resolved_coin_id, e)
    return {}

def get_lunarcrush_data(limit=50) -> List[dict]:
    """Try LunarCrush - gracefully fallback if subscription required"""
    try:
        url = "https://lunarcrush.com/api4/public/coins/list/v2"
        headers = {"Authorization": f"Bearer {LUNARCRUSH_API_KEY}"}
        params = {"sort": "galaxy_score", "limit": limit}
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            data = r.json()
            return data.get("data", [])
        if r.status_code == 402:
            logger.warning("LunarCrush key is accepted but the current plan does not include this endpoint - using CoinGecko only")
            return []
        logger.warning(f"LunarCrush unavailable (status {r.status_code}) - using CoinGecko only")
        return []
    except Exception as e:
        logger.error(f"LunarCrush error: {e}")
        return []

def parse_lunarcrush_topic_markdown(markdown_text: str) -> dict:
    def extract_number(pattern: str, cast=float):
        match = re.search(pattern, markdown_text, re.IGNORECASE)
        if not match:
            return None
        raw = match.group(1).replace(",", "").replace("$", "").strip()
        try:
            return cast(raw)
        except Exception:
            return None

    def extract_block(header: str) -> List[str]:
        match = re.search(rf"{re.escape(header)}:\n((?:- .+\n)+)", markdown_text, re.IGNORECASE)
        if not match:
            return []
        block = match.group(1)
        return [line[2:].strip() for line in block.strip().splitlines() if line.startswith("- ")]

    def extract_posts(section_title_pattern: str) -> List[dict]:
        section_match = re.search(
            rf"{section_title_pattern}.*?\n\n(.*?)(?:\n### |\Z)",
            markdown_text,
            re.IGNORECASE | re.DOTALL,
        )
        if not section_match:
            return []
        section = section_match.group(1)
        pattern = re.compile(
            r'"(?P<text>.*?)"\s+\n\[(?P<label>[^\]]+)\]\((?P<url>[^)]+)\)\s+(?P<meta>.*?)(?:\n\n|\Z)',
            re.DOTALL,
        )
        items = []
        for match in pattern.finditer(section):
            items.append({
                "text": match.group("text").strip(),
                "label": match.group("label").strip(),
                "url": match.group("url").strip(),
                "meta": " ".join(match.group("meta").split()),
            })
            if len(items) >= 4:
                break
        return items

    def extract_accounts() -> List[str]:
        match = re.search(
            r"\*\*Top accounts mentioned or mentioned by\*\*\n(.*?)(?:\n\*\*|\n### |\Z)",
            markdown_text,
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return []
        handles = re.findall(r"\[@?([A-Za-z0-9_\.]+)\]\(", match.group(1))
        deduped = []
        seen = set()
        for handle in handles:
            normalized = handle.strip().lstrip("@")
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            deduped.append(normalized)
            if len(deduped) >= 8:
                break
        return deduped

    lines = [line.strip() for line in markdown_text.splitlines() if line.strip()]
    title_line = next((line for line in lines if line.startswith("# ")), "").replace("# ", "").strip()
    summary = ""
    if title_line:
        try:
            title_index = lines.index(f"# {title_line}")
        except ValueError:
            title_index = 0
        for line in lines[title_index + 1:]:
            if not line.startswith("[") and not line.startswith("###") and not line.startswith("*") and not line.startswith("-"):
                summary = line
                break

    insights = re.findall(r"^- (.+)$", markdown_text, re.MULTILINE)
    return {
        "title": title_line,
        "summary": summary,
        "price_usd": extract_number(r"### Price:\s*\$([\d,]+(?:\.\d+)?)"),
        "alt_rank": extract_number(r"### AltRank:\s*([\d,]+)", int),
        "galaxy_score": extract_number(r"### Galaxy Score:\s*([\d,]+(?:\.\d+)?)"),
        "engagements_24h": extract_number(r"### Engagements:\s*([\d,]+)", int),
        "mentions_24h": extract_number(r"### Mentions:\s*([\d,]+)", int),
        "creators_24h": extract_number(r"### Creators:\s*([\d,]+)", int),
        "sentiment_pct": extract_number(r"### Sentiment:\s*([\d,]+)%", int),
        "social_dominance_pct": extract_number(r"### Social Dominance:\s*([\d,]+(?:\.\d+)?)"),
        "insights": insights[:6],
        "supportive_themes": extract_block("Most Supportive Themes"),
        "critical_themes": extract_block("Most Critical Themes"),
        "top_accounts": extract_accounts(),
        "top_news": extract_posts(r"### Top .*? News"),
        "top_social_posts": extract_posts(r"### Top .*? Social Posts"),
        "source": "LunarCrush AI",
        "limited_mode": "Limited data mode" in markdown_text,
    }

def parse_lunarcrush_creator_markdown(markdown_text: str) -> dict:
    def extract_number(pattern: str, cast=float):
        match = re.search(pattern, markdown_text, re.IGNORECASE)
        if not match:
            return None
        raw = match.group(1).replace(",", "").replace("$", "").strip()
        try:
            return cast(raw)
        except Exception:
            return None

    def extract_list_block(header_pattern: str, item_pattern: str) -> List[str]:
        match = re.search(header_pattern, markdown_text, re.IGNORECASE | re.DOTALL)
        if not match:
            return []
        items = re.findall(item_pattern, match.group(1), re.IGNORECASE)
        deduped = []
        seen = set()
        for item in items:
            normalized = item.strip()
            lowered = normalized.lower()
            if not normalized or lowered in seen:
                continue
            seen.add(lowered)
            deduped.append(normalized)
            if len(deduped) >= 8:
                break
        return deduped

    def extract_posts(section_title_pattern: str) -> List[dict]:
        section_match = re.search(
            rf"{section_title_pattern}.*?\n\n(.*?)(?:\n### |\Z)",
            markdown_text,
            re.IGNORECASE | re.DOTALL,
        )
        if not section_match:
            return []
        section = section_match.group(1)
        pattern = re.compile(
            r'"(?P<text>.*?)"\s+\n\[(?P<label>[^\]]+)\]\((?P<url>[^)]+)\)\s+(?P<meta>.*?)(?:\n\n|\Z)',
            re.DOTALL,
        )
        items = []
        for match in pattern.finditer(section):
            items.append({
                "text": match.group("text").strip(),
                "label": match.group("label").strip(),
                "url": match.group("url").strip(),
                "meta": " ".join(match.group("meta").split()),
            })
            if len(items) >= 3:
                break
        return items

    lines = [line.strip() for line in markdown_text.splitlines() if line.strip()]
    title_line = next((line for line in lines if line.startswith("# ")), "").replace("# ", "").strip()
    handle_match = re.search(r"@([A-Za-z0-9_\.]+)", title_line)
    summary = ""
    if title_line:
        try:
            title_index = lines.index(f"# {title_line}")
        except ValueError:
            title_index = 0
        for line in lines[title_index + 1:]:
            if not line.startswith("[") and not line.startswith("###") and not line.startswith("*") and not line.startswith("-"):
                summary = line
                break

    return {
        "title": title_line,
        "screen_name": handle_match.group(1) if handle_match else None,
        "summary": summary,
        "engagements": extract_number(r"### Engagements:\s*([\d,]+)", int),
        "mentions": extract_number(r"### Mentions:\s*([\d,]+)", int),
        "followers": extract_number(r"### Followers:\s*([\d,]+)", int),
        "creator_rank": extract_number(r"### CreatorRank:\s*([\d,]+)", int),
        "influence_topics": extract_list_block(
            r"\*\*Social topic influence\*\*\n(.*?)(?:\n\*\*|\n### |\Z)",
            r"\[([^\]]+)\]\(",
        ),
        "top_assets": extract_list_block(
            r"\*\*Top assets mentioned\*\*\n(.*?)(?:\n### |\Z)",
            r"\[([^\]]+)\]\(",
        ),
        "top_social_posts": extract_posts(r"### Top .*? Social Posts"),
        "source": "LunarCrush AI",
        "limited_mode": "Limited data mode" in markdown_text,
    }

def score_lunarcrush_creator_intelligence(creator: dict, asset_symbol: Optional[str] = None, asset_name: Optional[str] = None) -> dict:
    followers = int(creator.get("followers") or 0)
    engagements = int(creator.get("engagements") or 0)
    mentions = int(creator.get("mentions") or 0)
    creator_rank = int(creator.get("creator_rank") or 0)
    top_assets = creator.get("top_assets") or []
    influence_topics = creator.get("influence_topics") or []
    summary = (creator.get("summary") or "").lower()

    follower_score = min(22, followers / 20_000)
    engagement_score = min(20, engagements / 2_500)
    mention_score = min(12, mentions * 0.6)
    engagement_rate = (engagements / followers * 100) if followers else 0
    engagement_rate_score = min(12, engagement_rate * 2.5)

    if creator_rank > 0:
        if creator_rank <= 10_000:
            rank_score = 22
        elif creator_rank <= 50_000:
            rank_score = 18
        elif creator_rank <= 250_000:
            rank_score = 12
        elif creator_rank <= 1_000_000:
            rank_score = 7
        else:
            rank_score = 3
    else:
        rank_score = 0

    asset_focus_hits = 0
    asset_tokens = {token.lower() for token in [asset_symbol, asset_name] if token}
    searchable_blocks = [str(item).lower() for item in top_assets + influence_topics]
    if asset_tokens:
        for token in asset_tokens:
            if any(token in block for block in searchable_blocks) or token in summary:
                asset_focus_hits += 1
    asset_focus_score = min(12, asset_focus_hits * 6)

    crypto_focus_score = 6 if any(
        keyword in " ".join(searchable_blocks)
        for keyword in ["crypto", "bitcoin", "memecoin", "altcoin", "defi", "pump"]
    ) else 0

    trust_score = round(min(
        100,
        follower_score +
        engagement_score +
        mention_score +
        engagement_rate_score +
        rank_score +
        asset_focus_score +
        crypto_focus_score
    ))

    if trust_score >= 75:
        trust_badge = "High Conviction Voice"
    elif trust_score >= 58:
        trust_badge = "Credible Amplifier"
    elif trust_score >= 40:
        trust_badge = "Speculative Amplifier"
    else:
        trust_badge = "Low-Signal Account"

    if trust_score >= 72:
        influence_tier = "strong"
    elif trust_score >= 55:
        influence_tier = "credible"
    elif trust_score >= 40:
        influence_tier = "speculative"
    else:
        influence_tier = "thin"

    risk_flags: List[str] = []
    if followers < 5_000 and engagements > 5_000:
        risk_flags.append("Engagement concentration is unusually high relative to follower base.")
    if creator_rank and creator_rank > 500_000:
        risk_flags.append("Creator rank is weak, so narrative durability is less reliable.")
    if asset_tokens and asset_focus_score == 0:
        risk_flags.append("Creator is discussing the wider narrative more than this exact asset.")

    creator["trust_score"] = trust_score
    creator["trust_badge"] = trust_badge
    creator["influence_tier"] = influence_tier
    creator["engagement_rate_pct"] = round(engagement_rate, 2) if engagement_rate else 0
    creator["asset_focus_score"] = asset_focus_score
    creator["crypto_focus_score"] = crypto_focus_score
    creator["risk_flags"] = risk_flags
    return creator

def build_coin_cross_platform_consensus(
    *,
    symbol: str,
    signal_type: str,
    signal_strength: float,
    manipulation_profile: dict,
    decision_engine: Optional[dict],
    lunar_topic: Optional[dict],
    lunar_creators: Optional[List[dict]],
    is_trending: bool,
) -> dict:
    lunar_creators = lunar_creators or []
    telegram_mentions = int(manipulation_profile.get("telegram_mentions") or 0)
    telegram_sources = int(manipulation_profile.get("telegram_sources") or 0)
    coordination = float(manipulation_profile.get("coordinated_hype_score") or 0)
    social_burst = float(manipulation_profile.get("social_burst_score") or 0)
    dump_risk = float(manipulation_profile.get("dump_risk_score") or 0)
    manipulation_score = float(manipulation_profile.get("manipulation_score") or 0)
    stage = manipulation_profile.get("stage") or "active"

    market_score = min(100, signal_strength * 0.55 + manipulation_score * 0.45)
    telegram_score = min(100, coordination * 0.55 + telegram_mentions * 6 + telegram_sources * 10)

    topic_mentions = float((lunar_topic or {}).get("mentions_24h") or 0)
    topic_creators = float((lunar_topic or {}).get("creators_24h") or 0)
    topic_sentiment = float((lunar_topic or {}).get("sentiment_pct") or 0)
    topic_dominance = float((lunar_topic or {}).get("social_dominance_pct") or 0)
    creator_trust = max([float(item.get("trust_score") or 0) for item in lunar_creators] or [0])
    x_score = min(100, topic_mentions / 250 + topic_creators / 80 + topic_sentiment * 0.22 + creator_trust * 0.45 + topic_dominance * 4)

    narrative_score = min(100, social_burst * 0.5 + (10 if is_trending else 0) + topic_dominance * 5 + topic_sentiment * 0.2)

    overall_score = round(min(100, market_score * 0.34 + telegram_score * 0.23 + x_score * 0.23 + narrative_score * 0.20))

    if overall_score >= 75:
        verdict = "Aligned"
        badge = "Aligned"
    elif overall_score >= 58:
        verdict = "Building"
        badge = "Building"
    elif overall_score >= 42:
        verdict = "Speculative"
        badge = "Speculative"
    else:
        verdict = "Thin Confirmation"
        badge = "Thin"

    supportive_signals: List[str] = []
    conflict_flags: List[str] = []

    if market_score >= 65:
        supportive_signals.append(f"Market structure is active with a {signal_strength:.0f}/100 signal.")
    if telegram_score >= 50:
        supportive_signals.append(f"Telegram coordination is visible across {telegram_sources} source{'s' if telegram_sources != 1 else ''}.")
    if x_score >= 50:
        supportive_signals.append("X / creator activity is reinforcing the narrative around this asset.")
    if narrative_score >= 50:
        supportive_signals.append("Social narrative momentum is elevated beyond price action alone.")

    if telegram_score < 30:
        conflict_flags.append("Telegram confirmation is thin right now.")
    if x_score < 30:
        conflict_flags.append("X / creator confirmation is weak or still diffuse.")
    if dump_risk >= 80:
        conflict_flags.append("Exit risk is high enough to weaken the quality of late entries.")
    if signal_type == "pump" and stage in {"extended breakout", "blow-off risk"}:
        conflict_flags.append("The move is already stretched, so consensus helps less than timing discipline.")

    summary = (
        f"{symbol} cross-platform read is {verdict.lower()}: market {round(market_score)}/100, "
        f"Telegram {round(telegram_score)}/100, X {round(x_score)}/100, narrative {round(narrative_score)}/100."
    )

    return {
        "score": overall_score,
        "verdict": verdict,
        "badge": badge,
        "summary": summary,
        "platform_breakdown": {
            "market": round(market_score),
            "telegram": round(telegram_score),
            "x": round(x_score),
            "narrative": round(narrative_score),
        },
        "supportive_signals": supportive_signals[:4],
        "conflict_flags": conflict_flags[:4],
        "aligned_creators": [
            {
                "screen_name": creator.get("screen_name"),
                "trust_score": creator.get("trust_score", 0),
                "trust_badge": creator.get("trust_badge", "Low-Signal Account"),
            }
            for creator in sorted(lunar_creators, key=lambda item: item.get("trust_score", 0), reverse=True)[:3]
        ],
    }

def build_dashboard_cross_platform_consensus(snapshot: Optional[dict], telegram_consensus_payload: Optional[dict] = None) -> List[dict]:
    if not snapshot:
        return []

    hot_lookup = {
        (item.get("symbol") or "").upper(): item
        for item in (telegram_consensus_payload or {}).get("hot_symbols", []) or []
    }
    pool = (snapshot.get("pump_signals", []) or [])[:3] + (snapshot.get("dump_signals", []) or [])[:2]
    items = []

    for signal in pool:
        symbol = (signal.get("symbol") or "").upper()
        if not symbol:
            continue
        profile = signal.get("manipulation_profile") or {}
        tg = hot_lookup.get(symbol, {})
        signal_name = signal.get("name") or symbol
        market_score = min(100, (signal.get("signal_strength", 0) or 0) * 0.55 + (profile.get("manipulation_score", 0) or 0) * 0.45)
        telegram_score = min(100, (profile.get("coordinated_hype_score", 0) or 0) * 0.55 + (tg.get("mentions", 0) or 0) * 8 + (tg.get("unique_sources", 0) or 0) * 8)
        narrative_score = min(100, (profile.get("social_burst_score", 0) or 0) * 0.7 + (10 if signal.get("is_trending") else 0))

        social_topic, social_creators = get_social_intelligence_bundle(symbol, signal_name, creator_limit=2)

        topic_mentions = float((social_topic or {}).get("mentions_24h") or 0)
        topic_creators = float((social_topic or {}).get("creators_24h") or 0)
        topic_sentiment = float((social_topic or {}).get("sentiment_pct") or 0)
        topic_dominance = float((social_topic or {}).get("social_dominance_pct") or 0)
        creator_trust = max([float(item.get("trust_score") or 0) for item in social_creators] or [0])
        x_score = min(100, topic_mentions / 250 + topic_creators / 80 + topic_sentiment * 0.22 + creator_trust * 0.45 + topic_dominance * 4)

        consensus_score = round(min(100, market_score * 0.42 + telegram_score * 0.23 + x_score * 0.19 + narrative_score * 0.16))

        if consensus_score >= 72:
            verdict = "Aligned"
            badge = "aligned"
        elif consensus_score >= 55:
            verdict = "Building"
            badge = "building"
        elif consensus_score >= 40:
            verdict = "Speculative"
            badge = "speculative"
        else:
            verdict = "Thin"
            badge = "thin"

        supportive_signals: List[str] = []
        conflict_flags: List[str] = []
        if market_score >= 65:
            supportive_signals.append(f"Market structure is active at {round(market_score)}/100.")
        if telegram_score >= 50:
            supportive_signals.append(f"Telegram breadth is supportive across {tg.get('unique_sources', 0) or profile.get('telegram_sources', 0)} source(s).")
        if x_score >= 48:
            supportive_signals.append("X creator flow is reinforcing the move, not just echoing it.")
        if narrative_score >= 50:
            supportive_signals.append("Narrative momentum is elevated and still expanding.")

        if telegram_score < 30:
            conflict_flags.append("Telegram confirmation is still thin.")
        if x_score < 30:
            conflict_flags.append("X confirmation is weak or diffuse.")
        if (profile.get("dump_risk_score") or 0) >= 80:
            conflict_flags.append("Exit risk is high enough to punish late entries.")

        lead_creator = None
        if social_creators:
            best_creator = sorted(social_creators, key=lambda item: item.get("trust_score", 0), reverse=True)[0]
            lead_creator = {
                "screen_name": best_creator.get("screen_name"),
                "trust_score": best_creator.get("trust_score", 0),
                "trust_badge": best_creator.get("trust_badge", "Low-Signal Account"),
            }

        items.append({
            "symbol": symbol,
            "signal_type": signal.get("signal_type", "pump"),
            "consensus_score": consensus_score,
            "verdict": verdict,
            "badge": badge,
            "market_score": round(market_score),
            "telegram_score": round(telegram_score),
            "x_score": round(x_score),
            "narrative_score": round(narrative_score),
            "summary": f"{symbol} is {verdict.lower()} across market structure, Telegram flow, X amplification, and social momentum.",
            "supportive_signals": supportive_signals[:3],
            "conflict_flags": conflict_flags[:3],
            "lead_creator": lead_creator,
        })

    items.sort(key=lambda item: item.get("consensus_score", 0), reverse=True)
    return items[:4]

def get_lunarcrush_topic_intelligence(symbol: str, coin_name: Optional[str] = None) -> Optional[dict]:
    if not LUNARCRUSH_API_KEY or looks_like_placeholder(LUNARCRUSH_API_KEY, "LUNARCRUSH_API_KEY"):
        return None

    candidates = []
    if coin_name:
        candidates.append(coin_name.strip().lower().replace(" ", "-"))
        candidates.append(coin_name.strip().lower())
    candidates.append(symbol.strip().lower())
    candidates.append(f"${symbol.strip().lower()}")

    tried = set()
    headers = {"Authorization": f"Bearer {LUNARCRUSH_API_KEY}"}

    for candidate in candidates:
        if not candidate or candidate in tried:
            continue
        tried.add(candidate)
        try:
            url = f"https://lunarcrush.ai/topic/{quote(candidate, safe='')}"
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code == 429:
                logger.warning("LunarCrush AI topic rate limit reached for %s", candidate)
                return None
            if response.status_code != 200:
                continue

            text = response.text.strip()
            if not text or text == "no data generated" or text.startswith('{"error"'):
                continue

            parsed = parse_lunarcrush_topic_markdown(text)
            if parsed.get("summary") or parsed.get("mentions_24h") or parsed.get("social_dominance_pct"):
                parsed["topic"] = candidate
                return parsed
        except Exception as e:
            logger.warning("LunarCrush AI topic fetch failed for %s: %s", candidate, e)
            continue
    return None

def get_lunarcrush_creator_intelligence(
    handles: List[str],
    limit: int = 2,
    asset_symbol: Optional[str] = None,
    asset_name: Optional[str] = None,
) -> List[dict]:
    if not LUNARCRUSH_API_KEY or looks_like_placeholder(LUNARCRUSH_API_KEY, "LUNARCRUSH_API_KEY"):
        return []

    headers = {"Authorization": f"Bearer {LUNARCRUSH_API_KEY}"}
    creators = []
    seen = set()

    for raw_handle in handles:
        if len(creators) >= limit:
            break
        handle = (raw_handle or "").strip().lstrip("@")
        lowered = handle.lower()
        if not handle or lowered in seen or lowered in {"lunarcrush", "coingecko"}:
            continue
        seen.add(lowered)
        try:
            url = f"https://lunarcrush.ai/creator/x/{quote(handle, safe='')}"
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code == 429:
                logger.warning("LunarCrush AI creator rate limit reached for %s", handle)
                break
            if response.status_code != 200:
                continue

            text = response.text.strip()
            if not text or text == "no data generated" or text.startswith('{"error"'):
                continue

            parsed = parse_lunarcrush_creator_markdown(text)
            if parsed.get("summary") or parsed.get("followers") or parsed.get("engagements"):
                creators.append(score_lunarcrush_creator_intelligence(parsed, asset_symbol=asset_symbol, asset_name=asset_name))
        except Exception as e:
            logger.warning("LunarCrush AI creator fetch failed for %s: %s", handle, e)
            continue

    return creators

def get_x_coin_intelligence(symbol: str, coin_name: Optional[str] = None, limit: int = 10) -> Optional[dict]:
    if not X_BEARER_TOKEN or looks_like_placeholder(X_BEARER_TOKEN, "X_BEARER_TOKEN"):
        return None

    cache_key = f"{symbol.upper()}::{(coin_name or '').strip().lower()}"
    cached = X_INTELLIGENCE_CACHE.get(cache_key)
    now = datetime.now(timezone.utc)
    if cached and (now - cached.get("timestamp", now)).total_seconds() < 600:
        return cached.get("data")

    query_terms = [f"${symbol.upper()}", symbol.upper()]
    if coin_name and coin_name.strip().lower() != symbol.strip().lower():
        query_terms.append(f"\"{coin_name.strip()}\"")
    query = f"({' OR '.join(dict.fromkeys(query_terms))}) lang:en -is:retweet"

    try:
        response = requests.get(
            f"{X_API_BASE}/tweets/search/recent",
            headers={"Authorization": f"Bearer {X_BEARER_TOKEN}"},
            params={
                "query": query,
                "max_results": min(max(limit, 10), 25),
                "tweet.fields": "created_at,public_metrics,author_id,lang",
                "expansions": "author_id",
                "user.fields": "username,name,description,verified,public_metrics",
            },
            timeout=20,
        )
        if response.status_code == 429:
            logger.warning("X API rate limit reached for %s", symbol)
            return None
        if response.status_code != 200:
            logger.warning("X API search failed for %s with status %s", symbol, response.status_code)
            return None

        payload = response.json()
        tweets = payload.get("data") or []
        users = {item.get("id"): item for item in (payload.get("includes") or {}).get("users", [])}
        if not tweets:
            return None

        author_stats: Dict[str, dict] = {}
        top_posts = []
        total_engagements = 0

        for tweet in tweets:
            metrics = tweet.get("public_metrics") or {}
            engagements = int(metrics.get("like_count", 0) or 0) + int(metrics.get("retweet_count", 0) or 0) + int(metrics.get("reply_count", 0) or 0) + int(metrics.get("quote_count", 0) or 0)
            total_engagements += engagements
            author_id = tweet.get("author_id")
            user = users.get(author_id, {})
            username = user.get("username")
            if author_id:
                bucket = author_stats.setdefault(author_id, {"mentions": 0, "engagements": 0, "user": user})
                bucket["mentions"] += 1
                bucket["engagements"] += engagements
                if user:
                    bucket["user"] = user

            if username:
                top_posts.append({
                    "text": tweet.get("text", ""),
                    "label": "X Link",
                    "url": f"https://x.com/{username}/status/{tweet.get('id')}",
                    "meta": f"@{username} · {engagements} engagements",
                })

        creator_docs = []
        for author_id, stats in author_stats.items():
            user = stats.get("user") or {}
            public_metrics = user.get("public_metrics") or {}
            doc = {
                "title": f"@{user.get('username') or user.get('name') or 'Unknown'} {user.get('name') or ''}".strip(),
                "screen_name": user.get("username"),
                "summary": user.get("description") or f"Recent X account active around {symbol}.",
                "engagements": stats.get("engagements", 0),
                "mentions": stats.get("mentions", 0),
                "followers": public_metrics.get("followers_count", 0),
                "creator_rank": None,
                "influence_topics": [symbol.upper(), coin_name] if coin_name else [symbol.upper()],
                "top_assets": [symbol.upper(), coin_name] if coin_name else [symbol.upper()],
                "top_social_posts": [post for post in top_posts if f"/{user.get('username')}/status/" in post.get("url", "")][:3],
                "source": "X API",
                "limited_mode": False,
            }
            creator_docs.append(score_lunarcrush_creator_intelligence(doc, asset_symbol=symbol, asset_name=coin_name))

        creator_docs.sort(key=lambda item: item.get("trust_score", 0), reverse=True)
        unique_creators = len(author_stats)
        average_engagement = round(total_engagements / max(1, len(tweets)))
        summary = (
            f"{symbol.upper()} generated {len(tweets)} recent public X posts across {unique_creators} active accounts, "
            f"with roughly {total_engagements} combined engagements and about {average_engagement} engagements per post."
        )
        if coin_name:
            summary = (
                f"{coin_name} ({symbol.upper()}) generated {len(tweets)} recent public X posts across {unique_creators} active accounts, "
                f"with roughly {total_engagements} combined engagements and about {average_engagement} engagements per post."
            )

        result = {
            "title": f"{coin_name or symbol.upper()} on X",
            "topic": symbol.lower(),
            "summary": summary,
            "engagements_24h": total_engagements,
            "mentions_24h": len(tweets),
            "creators_24h": unique_creators,
            "sentiment_pct": None,
            "social_dominance_pct": None,
            "insights": [
                f"{symbol.upper()} appeared in {len(tweets)} recent public X posts.",
                f"Those posts came from {unique_creators} unique accounts.",
                f"Combined engagement across tracked posts is {total_engagements}.",
            ],
            "supportive_themes": [],
            "critical_themes": [],
            "top_accounts": [doc.get("screen_name") for doc in creator_docs if doc.get("screen_name")][:6],
            "top_news": [],
            "top_social_posts": sorted(top_posts, key=lambda item: int(re.search(r'(\\d+) engagements', item.get('meta', '0'))[1]) if re.search(r'(\\d+) engagements', item.get('meta', '')) else 0, reverse=True)[:5],
            "creator_docs": creator_docs[:5],
            "source": "X API",
            "limited_mode": False,
        }
        X_INTELLIGENCE_CACHE[cache_key] = {"timestamp": now, "data": result}
        return result
    except Exception as e:
        logger.warning("X API fetch failed for %s: %s", symbol, e)
        return None

def extract_social_creator_candidates(topic_payload: Optional[dict]) -> List[str]:
    if not topic_payload:
        return []

    candidates: List[str] = []
    candidates.extend(topic_payload.get("top_accounts", []) or [])
    for post in topic_payload.get("top_social_posts", []) or []:
        post_url = post.get("url", "")
        handle_match = re.search(r"x\.com/([A-Za-z0-9_\.]+)/status/", post_url, re.IGNORECASE)
        if handle_match:
            candidates.append(handle_match.group(1))

    deduped: List[str] = []
    seen = set()
    for raw_handle in candidates:
        handle = (raw_handle or "").strip().lstrip("@")
        lowered = handle.lower()
        if not handle or lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(handle)
    return deduped

def get_social_intelligence_bundle(symbol: str, coin_name: Optional[str] = None, creator_limit: int = 3) -> Tuple[Optional[dict], List[dict]]:
    cache_key = f"{symbol.upper()}::{(coin_name or '').strip().lower()}::{creator_limit}"
    cached = get_memory_cache(SOCIAL_INTELLIGENCE_CACHE, cache_key, ttl_seconds=600)
    if cached is not None:
        return cached

    x_topic = get_x_coin_intelligence(symbol, coin_name, limit=max(10, creator_limit * 4))
    if x_topic:
        x_creators = list(x_topic.get("creator_docs") or [])
        if x_creators:
            result = (x_topic, x_creators[:creator_limit])
            set_memory_cache(SOCIAL_INTELLIGENCE_CACHE, cache_key, result)
            return result
        x_candidates = extract_social_creator_candidates(x_topic)
        if x_candidates:
            x_creators = get_lunarcrush_creator_intelligence(
                x_candidates,
                limit=creator_limit,
                asset_symbol=symbol,
                asset_name=coin_name,
            )
        result = (x_topic, x_creators[:creator_limit])
        set_memory_cache(SOCIAL_INTELLIGENCE_CACHE, cache_key, result)
        return result

    lunar_topic = get_lunarcrush_topic_intelligence(symbol, coin_name)
    lunar_candidates = extract_social_creator_candidates(lunar_topic)
    lunar_creators = get_lunarcrush_creator_intelligence(
        lunar_candidates,
        limit=creator_limit,
        asset_symbol=symbol,
        asset_name=coin_name,
    ) if lunar_candidates else []
    result = (lunar_topic, lunar_creators[:creator_limit])
    set_memory_cache(SOCIAL_INTELLIGENCE_CACHE, cache_key, result)
    return result


def fetch_helius_whale_activity(contract_address: str, symbol: str = "", min_usd: float = 50000, hours: int = 2) -> dict:
    """Detect whale transactions for a Solana token using Helius."""
    api_key = os.environ.get("HELIUS_API_KEY", "").strip()
    if not api_key or not contract_address:
        return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": "no_key_or_address"}
    try:
        resp = requests.get(
            f"https://api.helius.xyz/v0/addresses/{contract_address}/transactions",
            params={"api-key": api_key, "limit": 50, "type": "TRANSFER"},
            timeout=15,
        )
        if resp.status_code != 200:
            return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": f"helius_{resp.status_code}"}
        txs = resp.json()
        if not isinstance(txs, list):
            return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": "invalid_response"}
        now = datetime.now(timezone.utc).timestamp()
        cutoff = now - (hours * 3600)
        KNOWN_EXCHANGES = {
            "5tzFkiKscXHK5ZXCGbXZxdw7gsolved", "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
            "AC5RDfQFmDS1deWZos921JfqscXDP5jfBi27DsHc3c", "2AQdpHJ2JpcEgPiATUXjQxA8QmafFegfQwSLWSprPicm"
        }
        whale_moves = []
        wallet_activity: dict = {}
        for tx in txs:
            ts = tx.get("timestamp", 0)
            if ts < cutoff:
                continue
            for transfer in tx.get("tokenTransfers", []):
                amount = float(transfer.get("tokenAmount") or 0)
                from_wallet = transfer.get("fromUserAccount", "")
                to_wallet = transfer.get("toUserAccount", "")
                if amount <= 0:
                    continue
                wallet_activity[from_wallet] = wallet_activity.get(from_wallet, 0) + amount
                move = {
                    "from": from_wallet,
                    "to": to_wallet,
                    "amount": amount,
                    "timestamp": ts,
                    "description": tx.get("description", ""),
                    "to_exchange": to_wallet in KNOWN_EXCHANGES,
                }
                whale_moves.append(move)
        large_moves = [m for m in whale_moves if m["amount"] >= min_usd / 100]
        exchange_moves = [m for m in large_moves if m["to_exchange"]]
        unique_buyers = len({m["to"] for m in large_moves})
        unique_sellers = len({m["from"] for m in large_moves if m["to_exchange"]})
        accumulation_detected = unique_buyers >= 3 and unique_buyers > unique_sellers * 1.5
        dump_risk = len(exchange_moves) >= 2 or unique_sellers >= 3
        whale_score = min(100, len(large_moves) * 8 + unique_buyers * 5 + (20 if accumulation_detected else 0) - (30 if dump_risk else 0))
        return {
            "whale_moves": large_moves[:10],
            "whale_score": max(0, whale_score),
            "accumulation_detected": accumulation_detected,
            "dump_risk": dump_risk,
            "unique_buyers": unique_buyers,
            "unique_sellers": unique_sellers,
            "large_move_count": len(large_moves),
            "exchange_move_count": len(exchange_moves),
            "error": None,
        }
    except Exception as e:
        logger.warning(f"Helius whale fetch failed for {symbol}: {e}")
        return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": str(e)}


def fetch_bitquery_whale_activity(contract_address: str, symbol: str = "", chain: str = "ethereum", min_usd: float = 50000, hours: int = 2) -> dict:
    """Detect whale transactions for an EVM token using Bitquery."""
    api_key = os.environ.get("BITQUERY_API_KEY", "").strip()
    if not api_key or not contract_address:
        return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": "no_key_or_address"}
    try:
        network_map = {"ethereum": "ethereum", "bsc": "bsc", "eth": "ethereum"}
        network = network_map.get(chain.lower(), "ethereum")
        query = """
        {
          EVM(network: %s) {
            TokenTransfers(
              where: {
                Transfer: {
                  Currency: {SmartContract: {is: "%s"}},
                  Amount: {ge: "%s"}
                },
                Block: {Time: {since: "%s"}}
              }
              limit: {count: 50}
              orderBy: {descending: Block_Time}
            ) {
              Transfer {
                Amount
                Sender
                Receiver
                Currency { Symbol Name }
              }
              Block { Time }
              Transaction { Hash }
            }
          }
        }
        """ % (
            network,
            contract_address.lower(),
            str(min_usd / 1000),
            (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        resp = requests.post(
            "https://graphql.bitquery.io",
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            json={"query": query},
            timeout=20,
        )
        if resp.status_code != 200:
            return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": f"bitquery_{resp.status_code}"}
        data = resp.json()
        transfers = (data.get("data") or {}).get("EVM", {}).get("TokenTransfers", []) or []
        KNOWN_EXCHANGES_EVM = {"binance", "coinbase", "kraken", "okx", "bybit", "kucoin"}
        whale_moves = []
        unique_buyers = set()
        unique_sellers = set()
        exchange_moves = 0
        for tx in transfers:
            transfer = tx.get("Transfer") or {}
            amount = float(transfer.get("Amount") or 0)
            sender = (transfer.get("Sender") or "").lower()
            receiver = (transfer.get("Receiver") or "").lower()
            if amount <= 0:
                continue
            to_exchange = any(ex in receiver for ex in KNOWN_EXCHANGES_EVM)
            if to_exchange:
                exchange_moves += 1
                unique_sellers.add(sender)
            else:
                unique_buyers.add(receiver)
            whale_moves.append({
                "from": sender,
                "to": receiver,
                "amount": amount,
                "to_exchange": to_exchange,
                "hash": (tx.get("Transaction") or {}).get("Hash", ""),
            })
        accumulation_detected = len(unique_buyers) >= 3 and len(unique_buyers) > len(unique_sellers) * 1.5
        dump_risk = exchange_moves >= 2 or len(unique_sellers) >= 3
        whale_score = min(100, len(whale_moves) * 8 + len(unique_buyers) * 5 + (20 if accumulation_detected else 0) - (30 if dump_risk else 0))
        return {
            "whale_moves": whale_moves[:10],
            "whale_score": max(0, whale_score),
            "accumulation_detected": accumulation_detected,
            "dump_risk": dump_risk,
            "unique_buyers": len(unique_buyers),
            "unique_sellers": len(unique_sellers),
            "large_move_count": len(whale_moves),
            "exchange_move_count": exchange_moves,
            "error": None,
        }
    except Exception as e:
        logger.warning(f"Bitquery whale fetch failed for {symbol}: {e}")
        return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": str(e)}


def fetch_etherscan_whale_activity(contract_address: str, symbol: str = "", min_amount: float = 100000, hours: int = 2) -> dict:
    """Detect whale transactions for an EVM token using Etherscan V2."""
    api_key = os.environ.get("ETHERSCAN_API_KEY", "").strip()
    if not api_key or not contract_address:
        return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": "no_key_or_address"}
    try:
        resp = requests.get(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": 1,
                "module": "account",
                "action": "tokentx",
                "contractaddress": contract_address.lower(),
                "sort": "desc",
                "page": 1,
                "offset": 50,
                "apikey": api_key,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": f"etherscan_{resp.status_code}"}
        data = resp.json()
        if data.get("status") != "1":
            return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": data.get("message", "unknown")}
        txs = data.get("result", [])
        if not isinstance(txs, list):
            return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": "invalid_result"}
        now = datetime.now(timezone.utc).timestamp()
        cutoff = now - (hours * 3600)
        KNOWN_EXCHANGE_ADDRESSES = {
            "0x28c6c06298d514db089934071355e5743bf21d60",
            "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
            "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",
            "0x56eddb7aa87536c09ccc2793473599fd21a8b17f",
            "0xa7efae728d2936e78bda97dc267687568dd593f3",
        }
        whale_moves = []
        unique_buyers = set()
        unique_sellers = set()
        exchange_moves = 0
        for tx in txs:
            if not isinstance(tx, dict):
                continue
            ts = int(tx.get("timeStamp", 0))
            if ts < cutoff:
                continue
            decimals = int(tx.get("tokenDecimal", 18))
            amount = float(tx.get("value", 0)) / (10 ** decimals)
            if amount < min_amount:
                continue
            from_addr = (tx.get("from") or "").lower()
            to_addr = (tx.get("to") or "").lower()
            to_exchange = to_addr in KNOWN_EXCHANGE_ADDRESSES
            if to_exchange:
                exchange_moves += 1
                unique_sellers.add(from_addr)
            else:
                unique_buyers.add(to_addr)
            whale_moves.append({
                "from": from_addr,
                "to": to_addr,
                "amount": round(amount, 2),
                "to_exchange": to_exchange,
                "hash": tx.get("hash", ""),
                "timestamp": ts,
            })
        accumulation_detected = len(unique_buyers) >= 3 and len(unique_buyers) > len(unique_sellers) * 1.5
        dump_risk = exchange_moves >= 2 or len(unique_sellers) >= 3
        whale_score = min(100, len(whale_moves) * 8 + len(unique_buyers) * 5 + (20 if accumulation_detected else 0) - (30 if dump_risk else 0))
        return {
            "whale_moves": whale_moves[:10],
            "whale_score": max(0, whale_score),
            "accumulation_detected": accumulation_detected,
            "dump_risk": dump_risk,
            "unique_buyers": len(unique_buyers),
            "unique_sellers": len(unique_sellers),
            "large_move_count": len(whale_moves),
            "exchange_move_count": exchange_moves,
            "error": None,
        }
    except Exception as e:
        logger.warning(f"Etherscan whale fetch failed for {symbol}: {e}")
        return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": str(e)}


WHALE_ACTIVITY_CACHE: Dict[str, dict] = {}

def fetch_whale_activity(contract_address: str, symbol: str = "", chain: str = "ethereum") -> dict:
    """Universal whale detection — routes to Helius (Solana) or Etherscan (EVM)."""
    if not contract_address:
        return {"whale_moves": [], "whale_score": 0, "accumulation_detected": False, "dump_risk": False, "chain": chain, "error": "no_address"}
    cache_key = f"whale_{contract_address}_{chain}"
    cached = get_memory_cache(WHALE_ACTIVITY_CACHE, cache_key, ttl_seconds=1800)
    if cached is not None:
        return cached
    chain_lower = (chain or "").lower()
    if chain_lower == "solana":
        result = fetch_helius_whale_activity(contract_address, symbol)
    else:
        result = fetch_etherscan_whale_activity(contract_address, symbol)
    result["chain"] = chain_lower
    set_memory_cache(WHALE_ACTIVITY_CACHE, cache_key, result)
    return result


def fetch_reddit_rss_signals(limit: int = 50) -> List[dict]:
    """Fetch Reddit RSS feeds for crypto signals without authentication."""
    import xml.etree.ElementTree as ET
    import re
    subreddits = [
        "CryptoMoonShots",
        "memecoins",
        "SatoshiStreetBets",
        "solana",
        "CryptoCurrency",
        "CryptoMarkets",
        "altcoin",
        "defi",
        "SolanaMemeCoins",
        "pumpfun",
    ]
    bullish_keywords = {"pump", "moon", "bullish", "buy", "long", "breakout", "ath", "gem", "launch", "new", "early", "presale", "airdrop", "listing", "100x", "1000x", "undervalued", "hidden", "sleeping"}
    bearish_keywords = {"dump", "sell", "short", "bearish", "crash", "down", "exit", "selling"}
    rug_keywords = {"rug", "scam", "fake", "honeypot", "fraud", "rugpull", "warning", "avoid", "beware", "ponzi"}
    ticker_pattern = re.compile(r"\$([A-Z]{2,10})")
    word_pattern = re.compile(r"\b([A-Z]{2,10})\b")
    stopwords = {"USD", "BTC", "ETH", "NFT", "DAO", "DeFi", "CEX", "DEX", "ATH", "ATL", "ROI", "APY", "APR", "TVL", "AMA", "IDO", "ICO", "IEO", "USA", "API", "RSS", "AI", "THE", "FOR", "AND", "NOT", "ARE", "BUT", "YOU", "ALL", "CAN", "NEW", "NOW", "GET", "TOP", "HOW", "WHY", "DEFI", "PUMP", "DUMP", "MOON", "JUST", "THIS", "WITH", "FROM", "THAT", "HAVE", "THEY", "WILL", "BEEN", "INTO", "OVER", "MORE", "SOME", "WHAT", "WHEN", "MAKE", "LIKE", "TIME", "YOUR", "ONLY", "THEN", "ALSO", "THEM", "WELL", "MUCH", "VERY"}
    # Blacklist token-uri majore - nu sunt pump candidates
    major_tokens = {"BTC", "ETH", "USDT", "USDC", "BNB", "XRP", "ADA", "DOGE", "SOL", "DOT", "MATIC", "SHIB", "LTC", "AVAX", "UNI", "LINK", "ATOM", "XLM", "ALGO", "VET", "FIL", "TRX", "ETC", "XMR", "NEAR", "HBAR", "ICP", "FTM", "SAND", "MANA", "AXS", "THETA", "EGLD", "HNT", "CAKE", "ONE", "ENJ", "CHZ", "HOT", "IOTA", "ZIL", "BAT", "DASH", "ZEC", "COMP", "MKR", "SNX", "AAVE", "YFI", "SUSHI", "CRV", "1INCH", "HYPE", "SUI", "APT", "OP", "ARB", "BLUR", "GMX", "DYDX", "LDO", "RPL", "PENDLE"}
    results = []
    seen_symbols = set()
    for subreddit in subreddits:
        try:
            resp = requests.get(
                f"https://www.reddit.com/r/{subreddit}/new.rss?limit=30",
                headers={"User-Agent": "PumpRadar/1.0"},
                timeout=10,
            )
            if resp.status_code != 200:
                continue
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            root = ET.fromstring(resp.content)
            entries = root.findall("atom:entry", ns)
            for entry in entries:
                title_el = entry.find("atom:title", ns)
                content_el = entry.find("atom:content", ns)
                title = title_el.text if title_el is not None else ""
                body = content_el.text if content_el is not None else ""
                full_text = f"{title} {body}"
                title_lower = (title or "").lower()
                full_lower = full_text.lower()
                # Cauta $TICKER mai intai
                tickers = ticker_pattern.findall(full_text)
                # Fallback: cuvinte majuscule din titlu care nu sunt stopwords
                if not tickers:
                    tickers = [w for w in word_pattern.findall(title or "") if w not in stopwords and len(w) >= 2]
                if not tickers:
                    continue
                symbol = tickers[0].upper()
                if symbol in seen_symbols:
                    continue
                if symbol in major_tokens or symbol in stopwords:
                    continue
                if len(symbol) < 2:
                    continue
                seen_symbols.add(symbol)
                is_bullish = any(k in full_lower for k in bullish_keywords)
                is_bearish = any(k in full_lower for k in bearish_keywords)
                is_rug = any(k in full_lower for k in rug_keywords)
                if not (is_bullish or is_bearish or is_rug):
                    continue
                direction = "dump" if is_bearish or is_rug else "pump"
                # Score bazat pe semnale
                score = 50
                if is_bullish: score += 20
                if is_rug: score -= 30
                if "$" in full_text and symbol in full_text: score += 15
                if subreddit in ("CryptoMoonShots", "SatoshiStreetBets", "SolanaMemeCoins", "pumpfun"): score += 10
                results.append({
                    "symbol": symbol,
                    "title": title[:200],
                    "subreddit": subreddit,
                    "direction": direction,
                    "is_bullish": is_bullish,
                    "is_bearish": is_bearish,
                    "is_rug": is_rug,
                    "score": max(0, min(100, score)),
                    "source": "reddit_rss",
                })
                if len(results) >= limit:
                    break
        except Exception as e:
            logger.warning(f"Reddit RSS fetch failed for r/{subreddit}: {e}")
    return results[:limit]


def get_fear_greed_index() -> dict:
    """Fear & Greed Index from alternative.me (free, no auth)"""
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        if r.status_code == 200:
            data = r.json().get("data", [{}])[0]
            return {"value": int(data.get("value", 50)), "classification": data.get("value_classification", "Neutral")}
    except Exception:
        pass
    return {"value": 50, "classification": "Neutral"}

def get_coingecko_trending() -> List[str]:
    """Get trending coin symbols from CoinGecko (free)"""
    try:
        r = requests.get("https://api.coingecko.com/api/v3/search/trending", headers=CG_HEADERS, timeout=15)
        if r.status_code == 200:
            coins = r.json().get("coins", [])
            return [c["item"]["symbol"].upper() for c in coins]
    except Exception:
        pass
    return []

def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default

def pick_first_numeric(payload: Optional[dict], *keys: str, default: float = 0.0) -> float:
    payload = payload or {}
    for key in keys:
        value = payload.get(key)
        parsed = safe_float(value)
        if parsed is not None:
            return parsed
    return default

def build_direction_audit(
    *,
    symbol: str,
    price_change_1h: float,
    price_change_24h: float,
    price_change_7d: float,
    volume_24h: float,
    market_cap: float,
    signal_type_hint: Optional[str] = None,
    signal_strength_hint: Optional[float] = None,
    pump_strength: Optional[float] = None,
    dump_strength: Optional[float] = None,
    is_trending: bool = False,
) -> dict:
    pc_1h = _coerce_float(price_change_1h)
    pc_24h = _coerce_float(price_change_24h)
    pc_7d = _coerce_float(price_change_7d)
    volume = max(0.0, _coerce_float(volume_24h))
    mcap = max(0.0, _coerce_float(market_cap))
    volume_ratio = (volume / mcap * 100) if mcap else 0.0
    hinted_strength = max(0.0, _coerce_float(signal_strength_hint))
    pump_strength_value = max(0.0, _coerce_float(pump_strength))
    dump_strength_value = max(0.0, _coerce_float(dump_strength))

    bullish_score = 0.0
    bearish_score = 0.0

    bullish_score += min(18.0, max(pc_1h, 0.0) * 5.0)
    bearish_score += min(18.0, abs(min(pc_1h, 0.0)) * 5.0)

    bullish_score += min(42.0, max(pc_24h, 0.0) * 1.85)
    bearish_score += min(42.0, abs(min(pc_24h, 0.0)) * 1.85)

    bullish_score += min(12.0, max(pc_7d, 0.0) * 0.22)
    bearish_score += min(12.0, abs(min(pc_7d, 0.0)) * 0.22)

    hourly_vs_daily = pc_1h - (pc_24h / 24 if pc_24h else 0.0)
    if pc_1h > 0 and hourly_vs_daily > 0:
        bullish_score += min(10.0, hourly_vs_daily * 3.25)
    if pc_1h < 0 and hourly_vs_daily < 0:
        bearish_score += min(10.0, abs(hourly_vs_daily) * 3.25)

    if volume_ratio >= 18:
        if pc_24h >= 0 or pc_1h >= 0:
            bullish_score += 6.0
        if pc_24h <= 0 or pc_1h <= 0:
            bearish_score += 6.0
    elif volume_ratio >= 10:
        if pc_24h >= 0:
            bullish_score += 3.0
        if pc_24h <= 0:
            bearish_score += 3.0

    if is_trending and pc_1h > 0 and pc_24h > -4:
        bullish_score += 5.0

    bullish_score += min(16.0, pump_strength_value * 0.16)
    bearish_score += min(16.0, dump_strength_value * 0.16)

    if signal_type_hint == "pump":
        bullish_score += min(8.0, hinted_strength * 0.08)
    elif signal_type_hint == "dump":
        bearish_score += min(8.0, hinted_strength * 0.08)

    strong_bullish = pc_1h >= 1.0 and pc_24h >= 8.0
    strong_bearish = pc_1h <= -1.0 and pc_24h <= -8.0
    dominant_bullish_context = (
        pc_24h >= 15.0 and
        pc_1h < 0 and
        abs(pc_1h) <= min(4.5, max(2.5, abs(pc_24h) / 6.0))
    )
    dominant_bearish_context = (
        pc_24h <= -15.0 and
        pc_1h > 0 and
        pc_1h <= min(4.8, max(2.0, abs(pc_24h) / 4.4))
    )
    simple_pullback = (
        (pc_24h >= 12.0 or dominant_bullish_context) and
        pc_1h < 0 and
        abs(pc_1h) <= min(4.2, max(2.8, abs(pc_24h) / 5.0)) and
        (dominant_bullish_context or bullish_score >= bearish_score - 2.0)
    )
    dead_cat_bounce = (
        (pc_24h <= -12.0 or dominant_bearish_context) and
        pc_1h > 0 and
        pc_1h <= min(6.0, max(3.0, abs(pc_24h) / 4.5)) and
        (dominant_bearish_context or bearish_score >= bullish_score - 2.0)
    )
    bullish_reversal_exception = (
        pc_24h <= -10.0 and
        pc_1h >= max(5.0, abs(pc_24h) / 3.0) and
        volume_ratio >= 25.0 and
        bullish_score >= bearish_score + 10.0
    )
    bearish_reversal_exception = (
        pc_24h >= 10.0 and
        pc_1h <= -max(5.0, abs(pc_24h) / 3.0) and
        volume_ratio >= 25.0 and
        bearish_score >= bullish_score + 10.0
    )

    score_gap = round(abs(bullish_score - bearish_score), 2)
    structure_bias = "mixed"
    resolved_direction = "pump"
    transition_state = "reversal_watch"
    narrative_template = "mixed_transition"
    explicit_exception = False

    if strong_bearish and not bullish_reversal_exception:
        resolved_direction = "dump"
        structure_bias = "bearish"
        transition_state = "dead_cat_bounce" if dead_cat_bounce else "bearish_breakdown"
        narrative_template = transition_state
    elif strong_bullish and not bearish_reversal_exception:
        resolved_direction = "pump"
        structure_bias = "bullish"
        transition_state = "bullish_pullback" if simple_pullback else "bullish_continuation"
        narrative_template = transition_state
    elif dead_cat_bounce and not bullish_reversal_exception:
        resolved_direction = "dump"
        structure_bias = "bearish"
        transition_state = "dead_cat_bounce"
        narrative_template = transition_state
    elif simple_pullback and not bearish_reversal_exception:
        resolved_direction = "pump"
        structure_bias = "bullish"
        transition_state = "bullish_pullback"
        narrative_template = transition_state
    else:
        if bullish_score >= bearish_score:
            resolved_direction = "pump"
            transition_state = "bullish_reversal" if pc_24h < 0 < pc_1h else "bullish_continuation"
            narrative_template = transition_state
        else:
            resolved_direction = "dump"
            transition_state = "bearish_reversal" if pc_24h > 0 > pc_1h else "bearish_breakdown"
            narrative_template = transition_state
        if score_gap >= 8:
            structure_bias = "bullish" if resolved_direction == "pump" else "bearish"
        else:
            structure_bias = "mixed"
            transition_state = "reversal_watch"
            narrative_template = transition_state

    if bullish_reversal_exception:
        resolved_direction = "pump"
        structure_bias = "mixed"
        transition_state = "bullish_reversal"
        narrative_template = transition_state
        explicit_exception = True
    elif bearish_reversal_exception:
        resolved_direction = "dump"
        structure_bias = "mixed"
        transition_state = "bearish_reversal"
        narrative_template = transition_state
        explicit_exception = True

    contradiction = bool(signal_type_hint and signal_type_hint != resolved_direction)

    return {
        "symbol": symbol,
        "price_change_1h": round(pc_1h, 2),
        "price_change_24h": round(pc_24h, 2),
        "price_change_7d": round(pc_7d, 2),
        "volume_market_cap_ratio": round(volume_ratio, 2),
        "pump_strength": round(pump_strength_value, 1),
        "dump_strength": round(dump_strength_value, 1),
        "signal_type_hint": signal_type_hint,
        "signal_strength_hint": round(hinted_strength, 1),
        "bullish_score": round(bullish_score, 2),
        "bearish_score": round(bearish_score, 2),
        "score_gap": score_gap,
        "structure_bias": structure_bias,
        "transition_state": transition_state,
        "resolved_direction": resolved_direction,
        "narrative_template": narrative_template,
        "strong_bullish": strong_bullish,
        "strong_bearish": strong_bearish,
        "simple_pullback": simple_pullback,
        "dead_cat_bounce": dead_cat_bounce,
        "explicit_exception": explicit_exception,
        "contradiction_with_hint": contradiction,
    }


def _clamp_score(value: Any, default: float = 0.0) -> int:
    try:
        if value is None or value == "":
            value = default
        return int(max(0, min(100, round(float(value)))))
    except Exception:
        return int(max(0, min(100, round(float(default)))))


def build_signal_v2(
    *,
    symbol: str,
    name: str,
    chain: Optional[str],
    contract_or_mint: Optional[str],
    signal_type: str,
    signal_strength: float,
    confidence: Any,
    risk_level: Any,
    market: dict,
    source_stack: dict,
    manipulation_profile: dict,
    decision_engine: dict,
    rugpull_profile: dict,
    asset_identity: dict,
) -> dict:
    """Signal Schema v2: additive dashboard payload for manipulation intelligence."""
    signal_type = (signal_type or "pump").lower()
    source_stack = source_stack or {}
    manipulation_profile = manipulation_profile or {}
    decision_engine = decision_engine or {}
    rugpull_profile = rugpull_profile or {}
    asset_identity = asset_identity or {}

    signal_strength_score = _clamp_score(signal_strength)
    manipulation_score = _clamp_score(manipulation_profile.get("manipulation_score"), signal_strength_score)
    coordination_score = _clamp_score(
        manipulation_profile.get("coordinated_hype_score")
        or manipulation_profile.get("coordination_score")
        or source_stack.get("telegram_score")
    )
    dump_risk_score = _clamp_score(
        manipulation_profile.get("dump_risk_score")
        or manipulation_profile.get("reversal_risk_score")
        or rugpull_profile.get("rugpull_risk_score")
    )
    social_coordination_score = _clamp_score(
        max(
            float(source_stack.get("telegram_score") or 0),
            float(source_stack.get("lunarcrush_score") or 0),
            float(coordination_score or 0),
        )
    )
    execution_score = _clamp_score(
        decision_engine.get("execution_score")
        or source_stack.get("execution_score")
    )

    source_tier = (source_stack.get("confirmation_tier") or "thin").strip().lower()
    telegram_active = bool(source_stack.get("telegram_active"))
    market_active = bool(source_stack.get("coingecko_market_active"))
    execution_active = bool(source_stack.get("execution_active"))
    lunar_active = bool(source_stack.get("lunarcrush_active"))

    source_count = sum([telegram_active, market_active, execution_active, lunar_active])
    noise_score = 25
    if source_tier in {"thin", "single-source"}:
        noise_score += 20
    if telegram_active and not market_active:
        noise_score += 15
    if market_active and source_count >= 2:
        noise_score -= 10
    if execution_active:
        noise_score -= 5
    noise_score = _clamp_score(noise_score)

    if signal_type == "dump":
        manipulation_setup_score = _clamp_score(manipulation_score * 0.45 + dump_risk_score * 0.35 + social_coordination_score * 0.20)
        pump_coordination_score = _clamp_score(coordination_score * 0.45)
    else:
        manipulation_setup_score = _clamp_score(manipulation_score * 0.45 + signal_strength_score * 0.30 + social_coordination_score * 0.25)
        pump_coordination_score = _clamp_score(coordination_score or social_coordination_score)

    phase = manipulation_profile.get("stage") or manipulation_profile.get("phase")
    if not phase:
        if signal_type == "dump" and dump_risk_score >= 70:
            phase = "dump_distribution"
        elif manipulation_setup_score >= 75 and social_coordination_score >= 55:
            phase = "early_coordinated_push"
        elif manipulation_setup_score >= 65:
            phase = "breakout_active" if signal_type == "pump" else "dump_risk_active"
        elif noise_score >= 65:
            phase = "noise_only"
        else:
            phase = "early_setup"

    timing = manipulation_profile.get("timing")
    if not timing:
        if phase in {"early_coordinated_push", "early_setup"}:
            timing = "early"
        elif phase in {"breakout_active", "dump_risk_active"}:
            timing = "developing"
        elif phase in {"late_chase", "distribution", "dump_distribution"}:
            timing = "late"
        else:
            timing = "watch"

    red_flags = []
    for flag in (rugpull_profile.get("warnings") or []):
        if isinstance(flag, str):
            red_flags.append(flag)
    if dump_risk_score >= 70:
        red_flags.append("dump_distribution_risk")
    if noise_score >= 65:
        red_flags.append("high_noise_risk")
    if source_tier in {"thin", "single-source"}:
        red_flags.append("thin_source_stack")
    if not contract_or_mint:
        red_flags.append("contract_or_mint_not_resolved")

    if signal_type == "dump":
        if dump_risk_score >= 75:
            verdict = "Strong Dump"
            action = "sell_risk"
        elif dump_risk_score >= 60:
            verdict = "Dump Risk"
            action = "watch_high_risk"
        else:
            verdict = "Distribution Risk"
            action = "monitor"
    else:
        if manipulation_setup_score >= 82 and noise_score < 55:
            verdict = "Strong Pump"
            action = "watch_high_risk"
        elif (
            manipulation_setup_score >= 68 and social_coordination_score >= 50
        ) or (
            signal_strength_score >= 76 and source_tier in {"dual-source", "triple-source", "stacked"}
        ):
            verdict = "Coordinated Pump Watch"
            action = "watch_high_risk"
        elif (
            manipulation_setup_score >= 52
            or signal_strength_score >= 65
            or source_tier in {"triple-source", "stacked"}
        ):
            verdict = "Pump Watch"
            action = "watch"
        elif noise_score >= 70:
            verdict = "Noise"
            action = "avoid"
        else:
            verdict = "No Signal"
            action = "monitor"

    trigger_parts = []
    if telegram_active:
        trigger_parts.append("Telegram calls")
    if market_active:
        trigger_parts.append("market anomaly")
    if execution_active:
        trigger_parts.append("verified trading venues")
    if lunar_active:
        trigger_parts.append("social metrics")
    trigger = " + ".join(trigger_parts) if trigger_parts else "thin source stack"

    why_now = []
    if telegram_active:
        why_now.append("Telegram source layer is active for this asset.")
    if market_active:
        why_now.append("Market structure shows abnormal movement or volume/market-cap activity.")
    if execution_active:
        why_now.append("Verified trade routes are available for execution.")
    if dump_risk_score >= 60:
        why_now.append("Dump or distribution risk is elevated.")
    if not why_now:
        why_now.append("Signal is still thin and needs more source confirmation.")

    preferred_venue = decision_engine.get("preferred_venue") or {}
    tradeability = {
        "status": "tradable" if execution_active or preferred_venue else "unknown",
        "primary_venue": preferred_venue.get("name"),
        "venue_count": decision_engine.get("venue_count") or source_stack.get("verified_routes") or 0,
        "liquidity_score": decision_engine.get("liquidity_score"),
        "execution_score": execution_score,
    }

    return {
        "schema_version": "signal_v2",
        "symbol": symbol,
        "name": name,
        "chain": chain,
        "contract_or_mint": contract_or_mint,
        "direction": signal_type,
        "verdict": verdict,
        "action": action,
        "confidence": _clamp_score(signal_strength_score if isinstance(confidence, str) else confidence, signal_strength_score),
        "manipulation_setup_score": manipulation_setup_score,
        "pump_coordination_score": pump_coordination_score,
        "dump_distribution_score": dump_risk_score,
        "noise_score": noise_score,
        "social_coordination_score": social_coordination_score,
        "whale_flow_score": None,
        "smart_money_signal": "not_available",
        "phase": phase,
        "timing": timing,
        "trigger": trigger,
        "why_now": why_now,
        "red_flags": sorted(set(red_flags)),
        "source_stack": {
            "labels": source_stack.get("labels") or [],
            "confirmation_tier": source_tier,
            "primary_driver": source_stack.get("primary_driver"),
            "telegram": ["Telegram"] if telegram_active else [],
            "market": ["CoinGecko/Dex market"] if market_active else [],
            "social": ["LunarCrush"] if lunar_active else [],
            "execution": ["CoinGecko venues"] if execution_active else [],
            "reddit": [],
            "x": [],
            "safety": [],
            "holders": [],
        },
        "tradeability": tradeability,
        "explanation": {
            "short": f"{verdict}: {trigger}.",
            "detail": source_stack.get("summary") or manipulation_profile.get("summary") or "Signal v2 generated from current market, social and execution layers.",
        },
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }


def build_signal_source_stack(
    *,
    symbol: str,
    price_change_1h: float,
    price_change_24h: float,
    vol_mcap_ratio: float,
    is_trending: bool,
    telegram_mentions: int = 0,
    telegram_sources: int = 0,
    bullish_mentions: int = 0,
    bearish_mentions: int = 0,
    telegram_avg_score: float = 0.0,
    social_volume: float = 0.0,
    galaxy_score: float = 0.0,
    sentiment: float = 0.0,
    lunar_mentions: float = 0.0,
    lunar_creators: float = 0.0,
    lunar_interactions: float = 0.0,
    lunar_dominance: float = 0.0,
    venue_count: int = 0,
    preferred_venue: Optional[dict] = None,
) -> dict:
    pc_1h = float(price_change_1h or 0)
    pc_24h = float(price_change_24h or 0)
    volume_ratio = float(vol_mcap_ratio or 0)
    tg_mentions = int(telegram_mentions or 0)
    tg_sources = int(telegram_sources or 0)
    bull_mentions = int(bullish_mentions or 0)
    bear_mentions = int(bearish_mentions or 0)
    tg_avg = float(telegram_avg_score or 0)
    social_volume = float(social_volume or 0)
    galaxy_score = float(galaxy_score or 0)
    sentiment = float(sentiment or 0)
    lunar_mentions = float(lunar_mentions or 0)
    lunar_creators = float(lunar_creators or 0)
    lunar_interactions = float(lunar_interactions or 0)
    lunar_dominance = float(lunar_dominance or 0)
    routes = int(venue_count or 0)

    telegram_score = min(100.0, tg_mentions * 11 + tg_sources * 13 + abs(bull_mentions - bear_mentions) * 8 + max(tg_avg - 45.0, 0.0) * 0.35)
    lunar_score = min(
        100.0,
        social_volume / 18.0 +
        galaxy_score * 0.42 +
        max(sentiment, 0.0) * 0.18 +
        lunar_mentions * 0.45 +
        lunar_creators * 3.5 +
        lunar_interactions / 5000.0 +
        lunar_dominance * 8.0 +
        (10.0 if is_trending else 0.0)
    )
    coingecko_market_score = min(100.0, volume_ratio * 3.5 + abs(pc_1h) * 8.0 + abs(pc_24h) * 1.35 + (10.0 if is_trending else 0.0))
    execution_score = min(100.0, routes * 18.0 + (10.0 if preferred_venue else 0.0))

    telegram_active = tg_mentions >= 1 or tg_sources >= 1 or bull_mentions >= 1 or bear_mentions >= 1
    lunar_active = social_volume >= 12 or galaxy_score >= 20 or lunar_mentions >= 6 or lunar_creators >= 2 or lunar_dominance >= 0.05
    coingecko_market_active = volume_ratio >= 6 or abs(pc_24h) >= 7 or abs(pc_1h) >= 1.5
    execution_active = routes >= 1

    labels: List[str] = []
    if telegram_active:
        labels.append("telegram")
    if lunar_active:
        labels.append("lunarcrush")
    if coingecko_market_active or is_trending:
        labels.append("coingecko_market")
    if execution_active:
        labels.append("coingecko_venues")

    active_layers = len(labels)
    if execution_active and active_layers >= 4:
        confirmation_tier = "stacked"
    elif active_layers >= 3:
        confirmation_tier = "triple-source"
    elif active_layers >= 2:
        confirmation_tier = "dual-source"
    elif active_layers == 1:
        confirmation_tier = "single-source"
    else:
        confirmation_tier = "thin"

    if telegram_score >= max(lunar_score, coingecko_market_score):
        primary_driver = "telegram_rumor_flow"
    elif lunar_score >= max(telegram_score, coingecko_market_score):
        primary_driver = "lunarcrush_social_flow"
    else:
        primary_driver = "coingecko_market_structure"

    if bull_mentions > bear_mentions:
        sentiment_bias = "bullish social bias"
    elif bear_mentions > bull_mentions:
        sentiment_bias = "bearish social bias"
    elif telegram_active or lunar_active:
        sentiment_bias = "mixed social bias"
    else:
        sentiment_bias = "market-led only"

    summary_parts = []
    if telegram_active:
        summary_parts.append(f"Telegram saw {tg_mentions} mentions across {tg_sources} sources")
    if lunar_active:
        summary_parts.append(f"LunarCrush social stack is active at {round(lunar_score)}/100")
    if coingecko_market_active or is_trending:
        summary_parts.append(f"CoinGecko market confirmation is {round(coingecko_market_score)}/100")
    if execution_active:
        preferred_name = ((preferred_venue or {}).get("name") or "preferred venue").strip()
        summary_parts.append(f"{routes} verified trade route{'s' if routes != 1 else ''} are available via {preferred_name}")

    summary = (
        f"{symbol} is {confirmation_tier} across "
        f"{', '.join(labels) if labels else 'thin source coverage'} with {sentiment_bias}."
    )
    if summary_parts:
        summary += " " + ". ".join(summary_parts[:4]) + "."

    return {
        "labels": labels,
        "confirmation_tier": confirmation_tier,
        "primary_driver": primary_driver,
        "sentiment_bias": sentiment_bias,
        "telegram_active": telegram_active,
        "lunarcrush_active": lunar_active,
        "coingecko_market_active": coingecko_market_active,
        "execution_active": execution_active,
        "telegram_score": round(telegram_score),
        "lunarcrush_score": round(lunar_score),
        "coingecko_market_score": round(coingecko_market_score),
        "execution_score": round(execution_score),
        "verified_routes": routes,
        "summary": summary,
    }

def score_market_candidate(candidate: dict, fg_value: float) -> dict:
    vol_mcap = float(candidate.get("vol_mcap_ratio", 0) or 0)
    pc_1h = float(candidate.get("price_change_1h", 0) or 0)
    pc_24h = float(candidate.get("price_change_24h", 0) or 0)
    pc_7d = float(candidate.get("price_change_7d", 0) or 0)
    is_trending = bool(candidate.get("is_trending", False))
    social_volume = float(candidate.get("social_volume", 0) or 0)
    sentiment = float(candidate.get("sentiment", 0) or 0)
    galaxy_score = float(candidate.get("galaxy_score", 0) or 0)
    telegram_mentions = int(candidate.get("telegram_mentions", 0) or 0)
    telegram_sources = int(candidate.get("telegram_sources", 0) or 0)
    bullish_mentions = int(candidate.get("bullish_mentions", 0) or 0)
    bearish_mentions = int(candidate.get("bearish_mentions", 0) or 0)
    telegram_avg_score = float(candidate.get("telegram_avg_score", 0) or 0)
    lunar_mentions = float(candidate.get("lunar_mentions", 0) or 0)
    lunar_creators = float(candidate.get("lunar_creators", 0) or 0)
    lunar_interactions = float(candidate.get("lunar_interactions", 0) or 0)
    lunar_dominance = float(candidate.get("lunar_dominance", 0) or 0)

    vol_score = min(100, (vol_mcap / 20) * 100) if vol_mcap > 5 else vol_mcap * 10

    momentum_1h_normalized = pc_1h * 24
    momentum_divergence = momentum_1h_normalized - pc_24h
    momentum_score = min(100, max(0, 50 + momentum_divergence * 2))

    trend_score = 0.0
    if pc_1h > 0 and pc_24h > 0:
        trend_score += 40
    if pc_1h > pc_24h / 24:
        trend_score += 30
    if is_trending:
        trend_score += 30

    sentiment_boost = 0.0
    if fg_value < 30 and pc_1h > 0:
        sentiment_boost = 20
    elif fg_value > 60 and pc_1h > 2:
        sentiment_boost = 15

    bullish_media_bias = max(0, bullish_mentions - bearish_mentions)
    bearish_media_bias = max(0, bearish_mentions - bullish_mentions)
    media_interest_score = min(100, telegram_mentions * 10 + telegram_sources * 12 + max(telegram_avg_score - 45, 0) * 0.45)
    bullish_media_score = min(100, media_interest_score * 0.55 + bullish_media_bias * 16 + (10 if bullish_mentions and bullish_mentions >= bearish_mentions else 0))
    bearish_media_score = min(100, media_interest_score * 0.55 + bearish_media_bias * 18 + (10 if bearish_mentions and bearish_mentions >= bullish_mentions else 0))

    accumulation_score = 0.0
    if pc_1h > 0:
        accumulation_score += min(26, pc_1h * 7.5)
    if -8 <= pc_24h <= 18:
        accumulation_score += max(0, 18 - abs(pc_24h - 4))
    if vol_mcap >= 6:
        accumulation_score += min(24, vol_mcap * 1.7)
    if social_volume or galaxy_score:
        accumulation_score += min(18, social_volume / 22 + galaxy_score * 0.18 + max(sentiment, 0) * 0.1)
    if bullish_media_bias > 0:
        accumulation_score += min(16, bullish_media_bias * 5 + telegram_sources * 3)
    accumulation_score = min(100, accumulation_score)

    selling_pressure = 0.0
    if pc_1h < 0 and pc_24h < 0:
        selling_pressure = min(100, abs(pc_1h) * 10 + abs(pc_24h) * 2)

    decline_acceleration = 0.0
    if pc_1h < 0 and pc_1h < pc_24h / 24:
        decline_acceleration = min(100, abs(pc_1h - pc_24h / 24) * 15)

    dump_vol_score = vol_score if pc_1h < -2 else 0.0
    dump_narrative_score = 0.0
    if pc_1h < 0:
        dump_narrative_score += min(22, abs(pc_1h) * 8)
    if pc_24h < 0:
        dump_narrative_score += min(24, abs(pc_24h) * 1.1)
    if vol_mcap >= 6:
        dump_narrative_score += min(18, vol_mcap * 1.2)
    if bearish_media_bias > 0:
        dump_narrative_score += min(20, bearish_media_bias * 6 + telegram_sources * 4)
    dump_narrative_score = min(100, dump_narrative_score)

    social_confirmation_score = min(100, social_volume / 18 + galaxy_score * 0.45 + max(sentiment, 0) * 0.2 + (18 if is_trending else 0))

    pump_strength = (
        vol_score * 0.22 +
        momentum_score * 0.24 +
        trend_score * 0.16 +
        sentiment_boost * 0.08 +
        accumulation_score * 0.18 +
        bullish_media_score * 0.12
    )

    dump_strength = (
        dump_vol_score * 0.18 +
        decline_acceleration * 0.24 +
        selling_pressure * 0.18 +
        (15 if fg_value > 70 else 0) * 0.08 +
        dump_narrative_score * 0.20 +
        bearish_media_score * 0.12
    )

    direction_audit = build_direction_audit(
        symbol=candidate.get("symbol", ""),
        price_change_1h=pc_1h,
        price_change_24h=pc_24h,
        price_change_7d=pc_7d,
        volume_24h=candidate.get("volume_24h", 0),
        market_cap=candidate.get("market_cap", 0),
        pump_strength=pump_strength,
        dump_strength=dump_strength,
        is_trending=is_trending,
    )
    source_stack = build_signal_source_stack(
        symbol=candidate.get("symbol", ""),
        price_change_1h=pc_1h,
        price_change_24h=pc_24h,
        vol_mcap_ratio=vol_mcap,
        is_trending=is_trending,
        telegram_mentions=telegram_mentions,
        telegram_sources=telegram_sources,
        bullish_mentions=bullish_mentions,
        bearish_mentions=bearish_mentions,
        telegram_avg_score=telegram_avg_score,
        social_volume=social_volume,
        galaxy_score=galaxy_score,
        sentiment=sentiment,
        lunar_mentions=lunar_mentions,
        lunar_creators=lunar_creators,
        lunar_interactions=lunar_interactions,
        lunar_dominance=lunar_dominance,
    )

    return {
        "pump_strength": round(pump_strength, 1),
        "dump_strength": round(dump_strength, 1),
        "vol_score": round(vol_score, 1),
        "momentum_score": round(momentum_score, 1),
        "accumulation_score": round(accumulation_score, 1),
        "social_confirmation_score": round(social_confirmation_score, 1),
        "bullish_media_score": round(bullish_media_score, 1),
        "bearish_media_score": round(bearish_media_score, 1),
        "direction_audit": direction_audit,
        "source_stack": source_stack,
    }

def build_market_candidate_record(
    coin: dict,
    *,
    lc_lookup: Optional[Dict[str, dict]] = None,
    telegram_stats_map: Optional[Dict[str, dict]] = None,
    trending_symbols: Optional[List[str]] = None,
    origin: str = "coingecko_scan",
    telegram_seed: Optional[dict] = None,
) -> dict:
    lc_lookup = lc_lookup or {}
    telegram_stats_map = telegram_stats_map or {}
    trending_symbols = trending_symbols or []

    sym = (coin.get("symbol") or "").upper()
    lc = lc_lookup.get(sym, {})
    telegram_stats = telegram_stats_map.get(sym, {})
    price = coin.get("current_price") or coin.get("price") or (telegram_seed or {}).get("reference_price") or 0
    vol = coin.get("total_volume") or coin.get("volume_24h") or 0
    mcap = coin.get("market_cap") or coin.get("market_cap_usd") or 0
    pc = coin.get("price_change_percentage_1h_in_currency") or coin.get("price_change_percentage_1h") or coin.get("price_change_1h") or 0
    pc24 = coin.get("price_change_percentage_24h") or coin.get("price_change_24h") or 0
    pc7d = coin.get("price_change_percentage_7d_in_currency") or coin.get("price_change_percentage_7d") or coin.get("price_change_7d") or 0
    vol_mcap_ratio = (vol / mcap * 100) if mcap > 0 else 0
    social_volume = lc.get("social_volume") or lc.get("sv") or 0
    sentiment = lc.get("sentiment") or lc.get("ss") or 0
    galaxy_score = lc.get("galaxy_score") or lc.get("gs") or 0
    lunar_mentions = pick_first_numeric(lc, "posts_active", "posts", "mentions", "social_posts", default=0.0)
    lunar_creators = pick_first_numeric(lc, "contributors_active", "social_contributors", "creators", "contributors", default=0.0)
    lunar_interactions = pick_first_numeric(lc, "interactions", "social_interactions", "engagements", default=0.0)
    lunar_dominance = pick_first_numeric(lc, "social_dominance", "dominance", default=0.0)
    is_trending = sym in trending_symbols
    mentions = int(telegram_stats.get("mentions") or (telegram_seed or {}).get("mentions") or 0)
    unique_sources = int(telegram_stats.get("unique_sources") or (telegram_seed or {}).get("unique_sources") or 0)
    bullish_mentions = int(telegram_stats.get("bullish_mentions") or (telegram_seed or {}).get("bullish_mentions") or 0)
    bearish_mentions = int(telegram_stats.get("bearish_mentions") or (telegram_seed or {}).get("bearish_mentions") or 0)
    avg_score = float(telegram_stats.get("avg_score") or (telegram_seed or {}).get("avg_score") or 0)

    return {
        "id": coin.get("id", "") or (telegram_seed or {}).get("coin_id", ""),
        "symbol": sym,
        "name": coin.get("name", "") or (telegram_seed or {}).get("coin_name") or sym,
        "price": price,
        "market_cap": float(coin.get("market_cap") or coin.get("market_cap_usd") or coin.get("fdv_usd") or coin.get("reserve_usd") or 0),
        "volume_24h": vol,
        "vol_mcap_ratio": round(vol_mcap_ratio, 2),
        "price_change_1h": round(float(pc), 2) if pc else 0,
        "price_change_24h": round(float(pc24), 2) if pc24 else 0,
        "price_change_7d": round(float(pc7d), 2) if pc7d else 0,
        "image": coin.get("image"),
        "is_trending": is_trending,
        "social_volume": social_volume,
        "sentiment": sentiment,
        "galaxy_score": galaxy_score,
        "lunar_mentions": round(lunar_mentions, 1),
        "lunar_creators": round(lunar_creators, 1),
        "lunar_interactions": round(lunar_interactions, 1),
        "lunar_dominance": round(lunar_dominance, 4),
        "telegram_mentions": mentions,
        "telegram_sources": unique_sources,
        "bullish_mentions": bullish_mentions,
        "bearish_mentions": bearish_mentions,
        "telegram_avg_score": round(avg_score, 1),
        "candidate_origin": origin,
    }

def explain_market_candidate_rejection(
    *,
    price: float,
    volume_24h: float,
    market_cap: float,
    vol_mcap_ratio: float,
    is_trending: bool,
    social_volume: float,
    galaxy_score: float,
    telegram_mentions: int,
    telegram_sources: int,
    bullish_mentions: int,
    bearish_mentions: int,
) -> List[str]:
    reasons: List[str] = []
    if price <= 0 or price < 0.0000001:
        reasons.append("price_missing_or_too_small")
    if volume_24h <= 0:
        reasons.append("volume_missing")
    if reasons:
        return reasons

    has_market_activity = volume_24h >= 15_000 or vol_mcap_ratio >= 3
    has_social_activity = social_volume >= 12 or galaxy_score >= 20 or is_trending
    has_telegram_activity = (
        telegram_mentions >= 1 or
        telegram_sources >= 1 or
        bullish_mentions >= 1 or
        bearish_mentions >= 1
    )

    if market_cap > 0 and market_cap >= 100_000:
        return []
    if has_market_activity or has_social_activity or has_telegram_activity:
        return []

    reasons.append("insufficient_market_social_or_telegram_activity")
    return reasons

def should_include_market_candidate(
    *,
    price: float,
    volume_24h: float,
    market_cap: float,
    vol_mcap_ratio: float,
    is_trending: bool,
    social_volume: float,
    galaxy_score: float,
    telegram_mentions: int,
    telegram_sources: int,
    bullish_mentions: int,
    bearish_mentions: int,
) -> bool:
    return len(explain_market_candidate_rejection(
        price=price,
        volume_24h=volume_24h,
        market_cap=market_cap,
        vol_mcap_ratio=vol_mcap_ratio,
        is_trending=is_trending,
        social_volume=social_volume,
        galaxy_score=galaxy_score,
        telegram_mentions=telegram_mentions,
        telegram_sources=telegram_sources,
        bullish_mentions=bullish_mentions,
        bearish_mentions=bearish_mentions,
    )) == 0

def is_true_pump_candidate(candidate: dict) -> bool:
    direction_audit = candidate.get("direction_audit") or {}
    if direction_audit.get("resolved_direction") != "pump":
        return False
    source_stack = candidate.get("source_stack") or {}

    pc_1h = float(candidate.get("price_change_1h", 0) or 0)
    pc_24h = float(candidate.get("price_change_24h", 0) or 0)
    vol_mcap = float(candidate.get("vol_mcap_ratio", 0) or 0)
    pump_strength = float(candidate.get("pump_strength", 0) or 0)
    accumulation_score = float(candidate.get("accumulation_score", 0) or 0)
    bullish_media_score = float(candidate.get("bullish_media_score", 0) or 0)
    social_confirmation_score = float(candidate.get("social_confirmation_score", 0) or 0)
    is_trending = bool(candidate.get("is_trending"))
    telegram_mentions = int(candidate.get("telegram_mentions", 0) or 0)
    bullish_mentions = int(candidate.get("bullish_mentions", 0) or 0)
    telegram_sources = int(candidate.get("telegram_sources", 0) or 0)

    momentum_ok = (pc_24h >= 7 and pc_1h >= 0.6) or pc_1h >= 1.8
    attention_ok = (
        bullish_media_score >= 40 or
        social_confirmation_score >= 28 or
        accumulation_score >= 56 or
        is_trending or
        bullish_mentions >= 1 or
        telegram_sources >= 1 or
        telegram_mentions >= 2
    )
    source_confirmed = (
        bool(source_stack.get("telegram_active")) or
        bool(source_stack.get("lunarcrush_active")) or
        bool(candidate.get("is_trending")) or
        bullish_media_score >= 55
    )
    market_confirmed = bool(source_stack.get("coingecko_market_active")) or bool(source_stack.get("coinpaprika_active")) or vol_mcap >= 6 or (candidate.get("candidate_origin") or "").startswith("geckoterminal")
    return (
        pump_strength >= 45 and
        vol_mcap >= 6 and
        momentum_ok and
        attention_ok and
        (source_confirmed or market_confirmed) and
        market_confirmed
    )

def is_true_dump_candidate(candidate: dict) -> bool:
    direction_audit = candidate.get("direction_audit") or {}
    if direction_audit.get("resolved_direction") != "dump":
        return False
    source_stack = candidate.get("source_stack") or {}

    pc_1h = float(candidate.get("price_change_1h", 0) or 0)
    pc_24h = float(candidate.get("price_change_24h", 0) or 0)
    vol_mcap = float(candidate.get("vol_mcap_ratio", 0) or 0)
    dump_strength = float(candidate.get("dump_strength", 0) or 0)
    bearish_media_score = float(candidate.get("bearish_media_score", 0) or 0)
    social_confirmation_score = float(candidate.get("social_confirmation_score", 0) or 0)
    telegram_mentions = int(candidate.get("telegram_mentions", 0) or 0)
    bearish_mentions = int(candidate.get("bearish_mentions", 0) or 0)
    telegram_sources = int(candidate.get("telegram_sources", 0) or 0)
    is_trending = bool(candidate.get("is_trending"))

    momentum_ok = (pc_24h <= -4 and pc_1h <= -0.5) or pc_1h <= -1.2
    narrative_ok = (
        bearish_media_score >= 28 or
        bearish_mentions >= 1 or
        telegram_sources >= 1 or
        telegram_mentions >= 1 or
        social_confirmation_score >= 22 or
        is_trending
    )
    source_confirmed = (
        bool(source_stack.get("telegram_active")) or
        bool(source_stack.get("lunarcrush_active")) or
        bool(source_stack.get("coingecko_market_active")) or
        bearish_media_score >= 38 or
        vol_mcap >= 6
    )
    market_confirmed = bool(source_stack.get("coingecko_market_active")) or vol_mcap >= 4
    return (
        dump_strength >= 46 and
        vol_mcap >= 4 and
        momentum_ok and
        narrative_ok and
        source_confirmed and
        market_confirmed
    )


def evaluate_snapshot_signal_gate(signal: dict, signal_type: str) -> dict:
    profile = signal.get("manipulation_profile") or {}
    decision = signal.get("decision_engine") or {}
    source_stack = signal.get("signal_sources") or signal.get("source_stack") or {}
    asset_identity = signal.get("asset_identity") or {}
    rugpull_profile = signal.get("rugpull_profile") or {}
    direction = profile.get("resolved_direction") or signal.get("signal_type") or signal_type

    hard_reasons: List[str] = []
    soft_reasons: List[str] = []

    if direction != signal_type:
        hard_reasons.append("direction_mismatch")

    signal_strength = float(signal.get("signal_strength") or 0) if str(signal.get("signal_strength", "0")).replace(".", "").isdigit() else 70
    pc_1h = float(signal.get("price_change_1h", 0) or 0)
    pc_24h = float(signal.get("price_change_24h", 0) or 0)
    volume_ratio = float(profile.get("volume_market_cap_ratio", 0) or decision.get("volume_market_cap_ratio") or 0)
    mentions = int(profile.get("telegram_mentions", 0) or 0)
    bullish_mentions = int(profile.get("bullish_mentions", 0) or 0)
    bearish_mentions = int(profile.get("bearish_mentions", 0) or 0)
    sources = int(profile.get("telegram_sources", 0) or 0)
    stage = (profile.get("stage") or "").strip().lower()
    social_burst = float(profile.get("social_burst_score", 0) or 0)
    coordination = float(profile.get("coordinated_hype_score", 0) or 0)
    dump_risk = float(profile.get("dump_risk_score", 0) or 0)
    execution_score = float(decision.get("execution_score", 0) or 0)
    venue_count = int(decision.get("venue_count", 0) or source_stack.get("verified_routes", 0) or 0)
    source_tier = (source_stack.get("confirmation_tier") or "thin").strip().lower()
    social_confirmed = bool(source_stack.get("telegram_active")) or bool(source_stack.get("lunarcrush_active")) or bool(signal.get("is_trending"))
    telegram_confirmed = bool(source_stack.get("telegram_active"))
    market_confirmed = bool(source_stack.get("coingecko_market_active"))
    identity_classification = (asset_identity.get("classification") or "").strip().lower()
    meme_score = float(asset_identity.get("meme_score", 0) or 0)
    speculative_score = float(asset_identity.get("speculative_score", 0) or 0)
    serious_score = float(asset_identity.get("serious_score", 0) or 0)
    rugpull_score = float(rugpull_profile.get("score", 0) or 0)

    if venue_count < 1:
        hard_reasons.append("no_trade_route")

    if signal_type == "pump":
        strong_override = (
            market_confirmed and
            venue_count >= 1 and
            signal_strength >= 66 and
            volume_ratio >= 3 and
            execution_score >= 40
        )

        market_structure_ok = (
            market_confirmed and
            venue_count >= 1 and
            signal_strength >= 54 and
            volume_ratio >= 2.5 and
            (
                (pc_24h >= 2 and pc_1h >= 0) or
                pc_1h >= 0.4 or
                coordination >= 18
            )
        )

        if signal_strength < 54:
            soft_reasons.append("signal_strength_below_pump_threshold")
        if volume_ratio < 2.5:
            soft_reasons.append("volume_ratio_below_pump_threshold")
        if not (((pc_24h >= 2 and pc_1h >= 0) or pc_1h >= 0.4 or coordination >= 18 or social_burst >= 18)):
            soft_reasons.append("price_momentum_not_confirmed_for_pump")
        if not (
            social_burst >= 8 or coordination >= 8 or bullish_mentions >= 1 or mentions >= 1 or
            sources >= 1 or signal.get("is_trending") or market_structure_ok
        ):
            soft_reasons.append("telegram_or_social_heat_too_thin_for_pump")
        if stage and stage not in {
            "breakout active", "coordinated hype", "extended breakout", "stealth build",
            "pullback continuation", "reversal attempt", "accumulation", "early breakout"
        }:
            soft_reasons.append("stage_not_allowed_for_pump")
        if execution_score < 35:
            soft_reasons.append("execution_score_too_low_for_pump")
        if not (social_confirmed or market_confirmed or market_structure_ok):
            soft_reasons.append("social_confirmation_missing_for_pump")
        if not (
            source_tier in {"stacked", "triple-source", "dual-source", "single-source"} or
            (market_confirmed and venue_count >= 1) or
            market_structure_ok
        ):
            soft_reasons.append("source_stack_too_thin_for_pump")
        if not (
            identity_classification in {"meme", "speculative", "mixed", "unknown"} or
            meme_score >= 28 or speculative_score >= 30 or serious_score <= 75 or market_structure_ok
        ):
            soft_reasons.append("asset_profile_not_suitable_for_pump")

        eligible = len(hard_reasons) == 0 and (strong_override or market_structure_ok or len(soft_reasons) <= 2)
        return {
            "eligible": eligible,
            "reasons": hard_reasons + ([] if eligible and (strong_override or market_structure_ok) else soft_reasons),
        }

    strong_override = (
        market_confirmed and
        venue_count >= 1 and
        signal_strength >= 62 and
        volume_ratio >= 3 and
        (dump_risk >= 55 or rugpull_score >= 35)
    )

    market_structure_ok = (
        market_confirmed and
        venue_count >= 1 and
        signal_strength >= 52 and
        volume_ratio >= 2.5 and
        (
            (pc_24h <= -2 and pc_1h <= 0) or
            pc_1h <= -0.6 or
            dump_risk >= 58
        )
    )

    if signal_strength < 52:
        soft_reasons.append("signal_strength_below_dump_threshold")
    if volume_ratio < 2.5:
        soft_reasons.append("volume_ratio_below_dump_threshold")
    if not (((pc_24h <= -2 and pc_1h <= 0) or pc_1h <= -0.6 or dump_risk >= 58)):
        soft_reasons.append("price_momentum_not_confirmed_for_dump")
    if not (
        bearish_mentions >= 1 or mentions >= 1 or sources >= 1 or social_burst >= 8 or
        coordination >= 8 or dump_risk >= 52 or market_structure_ok
    ):
        soft_reasons.append("telegram_or_social_heat_too_thin_for_dump")
    if stage and stage not in {
        "breakdown pressure", "coordinated unwind", "unwind active",
        "countertrend bounce", "reversal attempt", "distribution", "late breakdown"
    }:
        soft_reasons.append("stage_not_allowed_for_dump")
    if not (social_confirmed or rugpull_score >= 35 or market_confirmed or market_structure_ok):
        soft_reasons.append("social_or_rugpull_confirmation_missing_for_dump")
    if not (
        source_tier in {"stacked", "triple-source", "dual-source", "single-source"} or
        (market_confirmed and venue_count >= 1) or
        market_structure_ok
    ):
        soft_reasons.append("source_stack_too_thin_for_dump")
    if not (
        identity_classification in {"meme", "speculative", "mixed", "unknown"} or
        rugpull_score >= 28 or speculative_score >= 30 or market_structure_ok
    ):
        soft_reasons.append("asset_profile_not_suitable_for_dump")

    eligible = len(hard_reasons) == 0 and (strong_override or market_structure_ok or len(soft_reasons) <= 2)
    return {
        "eligible": eligible,
        "reasons": hard_reasons + ([] if eligible and (strong_override or market_structure_ok) else soft_reasons),
    }

def is_true_snapshot_signal(signal: dict, signal_type: str) -> bool:
    return bool(evaluate_snapshot_signal_gate(signal, signal_type).get("eligible"))

def is_alert_worthy_signal(signal: dict, signal_type: str) -> bool:
    if not is_true_snapshot_signal(signal, signal_type):
        return False
    decision = signal.get("decision_engine") or {}
    profile = signal.get("manipulation_profile") or {}
    signal_strength = float(signal.get("signal_strength") or 0) if str(signal.get("signal_strength", "0")).replace(".", "").isdigit() else 70
    execution_score = float(decision.get("execution_score", 0) or 0)
    volume_ratio = float(profile.get("volume_market_cap_ratio", 0) or decision.get("volume_market_cap_ratio") or 0)
    source_stack = signal.get("signal_sources") or signal.get("source_stack") or {}
    source_tier = (source_stack.get("confirmation_tier") or "thin").strip().lower()
    if signal_type == "pump":
        return signal_strength >= 78 and execution_score >= 58 and volume_ratio >= 12 and source_tier in {"stacked", "triple-source"}
    return signal_strength >= 72 and volume_ratio >= 10 and float(profile.get("dump_risk_score", 0) or 0) >= 70 and source_tier in {"stacked", "triple-source"}

def normalize_stage_for_direction(
    stage: Optional[str],
    resolved_direction: str,
    transition_state: Optional[str],
) -> str:
    current_stage = (stage or "").strip().lower()
    transition = (transition_state or "").strip().lower()
    bullish_only_stages = {"breakout active", "stealth build", "coordinated hype", "extended breakout", "pullback continuation", "reversal attempt", "blow-off risk"}
    bearish_only_stages = {"breakdown pressure", "coordinated unwind", "unwind active", "countertrend bounce"}

    if resolved_direction == "pump":
        if transition == "bullish_pullback":
            return "pullback continuation"
        if transition == "bullish_reversal":
            return "reversal attempt"
        if current_stage in bearish_only_stages or not current_stage:
            return "breakout active"
        return stage or "breakout active"

    if transition == "dead_cat_bounce":
        return "countertrend bounce"
    if current_stage in bullish_only_stages or not current_stage:
        return "breakdown pressure"
    return stage or "breakdown pressure"

def build_fallback_signal_analysis(
    scored_candidates: List[dict],
    fear_greed: Optional[dict] = None,
    trending: Optional[List[str]] = None,
) -> dict:
    fg = fear_greed or {"value": 50, "classification": "Neutral"}
    trending = trending or []

    for candidate in scored_candidates:
        candidate.setdefault(
            "direction_audit",
            build_direction_audit(
                symbol=candidate.get("symbol", ""),
                price_change_1h=candidate.get("price_change_1h", 0),
                price_change_24h=candidate.get("price_change_24h", 0),
                price_change_7d=candidate.get("price_change_7d", 0),
                volume_24h=candidate.get("volume_24h", 0),
                market_cap=candidate.get("market_cap", 0),
                pump_strength=candidate.get("pump_strength", 0),
                dump_strength=candidate.get("dump_strength", 0),
                is_trending=bool(candidate.get("is_trending")),
            ),
        )

        logger.info(f"Pump candidates before filter: {len([c for c in scored_candidates if is_true_pump_candidate(c)])}")
        pump_candidates = sorted(
        [
            c for c in scored_candidates
            if is_true_pump_candidate(c)
        ],
        key=lambda x: x.get("pump_strength", 0),
        reverse=True,
    )[:8]
    dump_candidates = sorted(
        [
            c for c in scored_candidates
            if is_true_dump_candidate(c)
        ],
        key=lambda x: x.get("dump_strength", 0),
        reverse=True,
    )[:4]

    def confidence_for(score: float) -> str:
        if score >= 75:
            return "high"
        if score >= 50:
            return "medium"
        return "low"

    def risk_for(change_1h: float, vol_mcap: float) -> str:
        if abs(change_1h) >= 4 or vol_mcap >= 18:
            return "high"
        if abs(change_1h) >= 2 or vol_mcap >= 10:
            return "medium"
        return "low"

    fallback_pumps = []
    for coin in pump_candidates:
        score = int(round(coin.get("pump_strength", 0)))
        direction_audit = coin.get("direction_audit") or {}
        source_stack = coin.get("source_stack") or {}
        media_tail = ""
        if coin.get("bullish_mentions", 0) or coin.get("telegram_sources", 0):
            media_tail = (
                f" Telegram bias is {int(coin.get('bullish_mentions', 0) or 0)} bullish vs "
                f"{int(coin.get('bearish_mentions', 0) or 0)} bearish mentions across "
                f"{int(coin.get('telegram_sources', 0) or 0)} source(s)."
            )
        source_tail = f" Source stack: {source_stack.get('summary')}" if source_stack.get("summary") else ""
        fallback_pumps.append({
            "symbol": coin["symbol"],
            "signal_strength": score,
            "reason": (
                f"Volume/market-cap ratio at {coin.get('vol_mcap_ratio', 0)}% with 1h move "
                f"{coin.get('price_change_1h', 0):+.2f}% and 24h move {coin.get('price_change_24h', 0):+.2f}%. "
                f"Momentum score {coin.get('momentum_score', 0):.0f} indicates short-term acceleration. "
                f"Resolved structure: {direction_audit.get('transition_state', 'bullish_continuation').replace('_', ' ')}."
                f"{media_tail}{source_tail}"
            ),
            "technical_factors": "Volume anomaly, momentum acceleration, trend alignment",
            "confidence": confidence_for(score),
            "risk_level": risk_for(coin.get("price_change_1h", 0), coin.get("vol_mcap_ratio", 0)),
            "timeframe": "4-12 hours",
            "signal_sources": source_stack,
            "source_summary": source_stack.get("summary"),
        })

    fallback_dumps = []
    for coin in dump_candidates:
        score = int(round(coin.get("dump_strength", 0)))
        direction_audit = coin.get("direction_audit") or {}
        source_stack = coin.get("source_stack") or {}
        media_tail = ""
        if coin.get("bearish_mentions", 0) or coin.get("telegram_sources", 0):
            media_tail = (
                f" Telegram/media bias is {int(coin.get('bearish_mentions', 0) or 0)} bearish vs "
                f"{int(coin.get('bullish_mentions', 0) or 0)} bullish mentions across "
                f"{int(coin.get('telegram_sources', 0) or 0)} source(s)."
            )
        source_tail = f" Source stack: {source_stack.get('summary')}" if source_stack.get("summary") else ""
        fallback_dumps.append({
            "symbol": coin["symbol"],
            "signal_strength": score,
            "reason": (
                f"1h decline {coin.get('price_change_1h', 0):+.2f}% against 24h move {coin.get('price_change_24h', 0):+.2f}% "
                f"with volume/market-cap ratio {coin.get('vol_mcap_ratio', 0)}%. Selling pressure and decline acceleration remain elevated. "
                f"Resolved structure: {direction_audit.get('transition_state', 'bearish_breakdown').replace('_', ' ')}."
                f"{media_tail}{source_tail}"
            ),
            "technical_factors": "Selling pressure, decline acceleration, volume confirmation",
            "confidence": confidence_for(score),
            "risk_level": risk_for(coin.get("price_change_1h", 0), coin.get("vol_mcap_ratio", 0)),
            "timeframe": "2-8 hours",
            "signal_sources": source_stack,
            "source_summary": source_stack.get("summary"),
        })

    sentiment = fg.get("classification", "Neutral")
    trend_text = ", ".join(trending[:5]) if trending else "none"
    stacked_count = len([
        c for c in scored_candidates
        if ((c.get("source_stack") or {}).get("confirmation_tier") in {"stacked", "triple-source"})
    ])
    market_summary = (
        f"Fallback quantitative analysis active. Fear & Greed is {fg.get('value', 50)}/100 ({sentiment}). "
        f"{len(fallback_pumps)} pump candidates and {len(fallback_dumps)} dump candidates passed the scoring filters. "
        f"{stacked_count} assets currently have stacked source confirmation from rumor flow, social breadth, and market structure. "
        f"Trending symbols: {trend_text}."
    )

    return {
        "pump_signals": fallback_pumps,
        "dump_signals": fallback_dumps,
        "market_summary": market_summary,
    }

def build_fallback_chat_reply(
    message: str,
    pump_signals: List[dict],
    dump_signals: List[dict],
    summary: str,
    fear_greed: Optional[dict],
    trending: List[str],
    user_sub: str,
) -> str:
    msg = (message or "").strip().lower()
    normalized_msg = " ".join(re.findall(r"[a-zA-Z0-9ăâîșşțţ']+", msg))
    tokens = set(normalized_msg.split())
    fg = fear_greed or {}
    top_pumps = ", ".join([f"{s.get('symbol')} ({s.get('signal_strength', 0)}%)" for s in pump_signals[:3]]) or "none right now"
    top_dumps = ", ".join([f"{s.get('symbol')} ({s.get('signal_strength', 0)}%)" for s in dump_signals[:3]]) or "none right now"
    abusive_terms = [
        "idiot", "stupid", "dumb", "moron", "fuck you", "fucking", "shit", "bitch", "asshole",
        "prost", "idiotule", "bou", "dobitoc", "muie", "dracu", "mars", "retard",
    ]
    greeting_terms = ["hi", "hello", "hey", "salut", "buna", "bună", "yo"]
    thanks_terms = ["thanks", "thank you", "mersi", "multumesc", "mulțumesc", "thx"]
    capability_terms = ["what can you do", "ce poti", "ce poți", "help", "ajuta", "ajută"]
    identity_terms = ["who are you", "cine esti", "cine ești"]

    def has_term(term: str) -> bool:
        normalized_term = " ".join(re.findall(r"[a-zA-Z0-9ăâîșşțţ']+", term.lower()))
        if not normalized_term:
            return False
        if " " in normalized_term:
            return normalized_term in normalized_msg
        return normalized_term in tokens

    if not msg:
        return "Ask me about PumpRadar signals, a coin from the dashboard, market context, or your subscription."

    if any(has_term(term) for term in abusive_terms):
        return (
            "I can help with PumpRadar and crypto questions, but I will not engage with abusive language. "
            "Ask about a coin, current signals, market context, or your subscription and I will keep it concise."
        )

    if any(has_term(term) for term in identity_terms):
        return (
            "I'm PumpRadar AI, the in-app assistant for this platform. "
            "I help explain signals, summarize market context, and answer questions about coins, features, and subscriptions."
        )

    if any(has_term(term) for term in capability_terms):
        return (
            "I can explain live signals, summarize the current market, discuss coins from the latest snapshot, "
            "and help with PumpRadar features or subscription questions."
        )

    if any(has_term(term) for term in greeting_terms):
        return (
            "Hi. I can help with live PumpRadar signals, explain a coin on the dashboard, summarize market context, or clarify your subscription."
        )

    if any(has_term(term) for term in thanks_terms):
        return "You're welcome. Ask about a coin, the latest signals, market context, or your subscription if you want the next step."

    if "pump" in msg and "signal" in msg:
        return (
            "A PUMP signal means our scoring model sees bullish short-term momentum supported by volume and trend alignment. "
            f"Right now the strongest pump candidates are {top_pumps}. This is not financial advice. Always do your own research."
        )
    if "dump" in msg and "signal" in msg:
        return (
            "A DUMP signal means our model sees elevated downside pressure, usually from accelerating decline plus high relative volume. "
            f"Current dump warnings: {top_dumps}. This is not financial advice. Always do your own research."
        )
    if "plan" in msg or "price" in msg or "subscription" in msg:
        return (
            f"Your current plan is {user_sub}. PumpRadar offers a 7-day free trial, Monthly at $29.99, and Annual at $299.99. "
            "Paid plans unlock full signals and deeper analysis. This is not financial advice. Always do your own research."
        )
    if "fear" in msg or "greed" in msg or "market" in msg:
        return (
            f"Current market context: Fear & Greed is {fg.get('value', 'N/A')}/100 ({fg.get('classification', 'N/A')}). "
            f"{summary or 'Signals are still being processed.'} Trending: {', '.join(trending[:5]) if trending else 'none'}. "
            "This is not financial advice. Always do your own research."
        )
    return (
        "I can help with PumpRadar signals, market context, coins shown in the dashboard, and subscription questions. "
        f"Right now we track {len(pump_signals)} pump candidates and {len(dump_signals)} dump candidates. "
        f"Top pumps: {top_pumps}. Top dumps: {top_dumps}. Ask something specific and I will keep it short."
    )

async def analyze_signals_with_ai(candidates: List[dict], fear_greed: dict = None, trending: List[str] = None) -> dict:
    """
    SCIENTIFIC PUMP/DUMP SIGNAL ANALYSIS
    
    Uses multiple quantitative indicators:
    1. Volume Spike Detection (Abnormal Volume = vol/mcap > 15% indicates institutional interest)
    2. Momentum Analysis (RSI-like: 1h vs 24h price action divergence)
    3. Market Sentiment Alignment (Fear & Greed correlation)
    4. Social Trending Factor (CoinGecko trending = retail interest)
    5. Price Action Patterns (Higher highs, lower lows detection)
    
    Signal Strength Formula:
    PUMP = (vol_score * 0.3) + (momentum_score * 0.35) + (trend_score * 0.2) + (sentiment_score * 0.15)
    DUMP = (vol_score * 0.25) + (decline_score * 0.4) + (selling_pressure * 0.2) + (sentiment_score * 0.15)
    """
    if not candidates:
        return {"pump_signals": [], "dump_signals": [], "market_summary": "No data available"}
    
    try:
        fg = fear_greed or {"value": 50, "classification": "Neutral"}
        trending_str = ", ".join(trending[:10]) if trending else "N/A"
        fg_value = fg.get("value", 50)
        
        # Pre-calculate scientific scores for each coin
        scored_candidates = []
        for c in candidates:
            scored = score_market_candidate(c, fg_value)
            
            scored_candidates.append({
                **c,
                **scored,
            })
        
        # Select top pump candidates (strength > 50)
        logger.info(f"Pump candidates before filter: {len([c for c in scored_candidates if is_true_pump_candidate(c)])}")
        pump_candidates = sorted(
            [
                c for c in scored_candidates
                if is_true_pump_candidate(c)
            ],
            key=lambda x: x["pump_strength"],
            reverse=True
        )[:15]
        
        # Select top dump candidates (strength > 40)
        dump_candidates = sorted(
            [
                c for c in scored_candidates
                if is_true_dump_candidate(c)
            ],
            key=lambda x: x["dump_strength"],
            reverse=True
        )[:10]
        
        system_instruction = """You are PumpRadar AI Judge. Return strict JSON only. English only.
Classify pre-scored crypto setups as early_pump, pump_watch, late_pump, distribution_risk, dump_risk, noise, or avoid.
Prefer safety: late vertical moves, weak confirmation, high noise, or conflicting pump/dump scores should be avoid/watch, not buy."""
        
        pump_data = "; ".join([
            f"{c['symbol']} ps={c['pump_strength']:.0f} ds={c.get('dump_strength',0):.0f} "
            f"1h={c['price_change_1h']:+.2f}% 24h={c['price_change_24h']:+.2f}% "
            f"vm={c['vol_mcap_ratio']:.1f}% mom={c['momentum_score']:.0f} "
            f"tg={c.get('bullish_mentions',0)}/{c.get('bearish_mentions',0)} "
            f"tier={((c.get('source_stack') or {}).get('confirmation_tier') or 'thin')}"
            for c in pump_candidates[:5]
        ]) if pump_candidates else "none"
        
        dump_data = "; ".join([
            f"{c['symbol']} ds={c['dump_strength']:.0f} ps={c.get('pump_strength',0):.0f} "
            f"1h={c['price_change_1h']:+.2f}% 24h={c['price_change_24h']:+.2f}% "
            f"vm={c['vol_mcap_ratio']:.1f}% "
            f"tg={c.get('bullish_mentions',0)}/{c.get('bearish_mentions',0)} "
            f"tier={((c.get('source_stack') or {}).get('confirmation_tier') or 'thin')}"
            for c in dump_candidates[:3]
        ]) if dump_candidates else "none"
        
        prompt = (
            f"FG={fg_value} {fg['classification']}. "
            f"PUMP={pump_data}. DUMP={dump_data}. "
            "Return JSON only with keys: pump_signals, dump_signals, market_summary. "
            "Each signal: symbol, signal_strength (integer 0-100), reason, technical_factors, confidence, risk_level, timeframe. "
            "Use short reasons. Exclude noise/late unsafe setups. If a 24h pump is already extreme and 1h weak, mark dump/distribution risk, not pump."
        )
        
        ai_result = await call_claude_haiku_json(
            system_instruction=system_instruction,
            user_prompt=prompt,
            temperature=0.1,
            max_tokens=1500,
        )

        if ai_result.get("ok") and ai_result.get("json"):
            result = ai_result["json"]
            result["ai_provider"] = ai_result.get("provider")
            result["ai_model"] = ai_result.get("model")
            return result

        logger.warning(
            "Claude Haiku signal analysis failed - using quantitative fallback. provider=%s error=%s",
            ai_result.get("provider"),
            ai_result.get("error"),
        )
        return build_fallback_signal_analysis(scored_candidates, fear_greed, trending)

    except Exception as e:
        logger.error(f"OpenAI/OpenRouter signal analysis error - using quantitative fallback: {e}")
        return build_fallback_signal_analysis(scored_candidates if 'scored_candidates' in locals() else [], fear_greed, trending)

async def fetch_and_store_signals(trigger: str = "scheduler"):
    """Main job: fetch data, analyze with AI, store results"""
    if SIGNAL_SCAN_LOCK.locked():
        logger.info("Skipping signal fetch job because another scan is already running")
        return {
            "started": False,
            "completed": False,
            "scan_status": build_signal_scan_status(),
        }

    logger.info("Starting crypto signal fetch job...")
    async with SIGNAL_SCAN_LOCK:
        started_at = datetime.now(timezone.utc)
        SIGNAL_SCAN_STATE.update({
            "running": True,
            "trigger": trigger,
            "started_at": started_at,
            "finished_at": None,
            "last_error": None,
        })
        job_result = {
            "started": True,
            "completed": False,
            "pump_count": 0,
            "dump_count": 0,
            "coins_analyzed": 0,
            "snapshot_at": None,
        }

        try:
            telegram_stats_map = await get_recent_telegram_signal_map(hours=24)

            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                cg_future = executor.submit(fetch_geckoterminal_markets, 500)
                lc_future = executor.submit(get_lunarcrush_data, 50)
                fg_future = executor.submit(get_fear_greed_index)
                trending_future = executor.submit(get_coingecko_trending)

                cg_data = cg_future.result()
                lc_data = lc_future.result()
                fear_greed = fg_future.result()
                trending_symbols = trending_future.result()

            lc_lookup: Dict[str, dict] = {}
            for coin in lc_data:
                sym = (coin.get("symbol") or coin.get("s") or "").upper()
                if sym:
                    lc_lookup[sym] = coin

            recent_telegram_cutoff = datetime.now(timezone.utc) - timedelta(hours=TELEGRAM_EARLY_SIGNAL_HOURS)
            recent_telegram_signals = await db.telegram_signals.find({
                "posted_at": {"$gte": recent_telegram_cutoff},
                "symbol": {"$exists": True, "$ne": None},
            }).sort("posted_at", -1).limit(400).to_list(length=400)
            telegram_early_signals = build_telegram_early_signal_candidates(
                recent_telegram_signals,
                hours=TELEGRAM_EARLY_SIGNAL_HOURS,
                limit=TELEGRAM_EARLY_SIGNAL_LIMIT,
            )
            # Reddit RSS signals
            try:
                reddit_rss_signals = await asyncio.to_thread(fetch_reddit_rss_signals, 30)
                logger.info(f"Reddit RSS: {len(reddit_rss_signals)} signals fetched")
            except Exception as reddit_err:
                reddit_rss_signals = []
                logger.warning(f"Reddit RSS fetch failed: {reddit_err}")

            telegram_pipeline_records: List[dict] = []

            candidates = []
            candidate_lookup: Dict[str, dict] = {}
            for coin in cg_data:
                candidate = build_market_candidate_record(
                    coin,
                    lc_lookup=lc_lookup,
                    telegram_stats_map=telegram_stats_map,
                    trending_symbols=trending_symbols,
                    origin="coingecko_scan",
                )
                reject_reasons = explain_market_candidate_rejection(
                    price=candidate.get("price", 0) or 0,
                    volume_24h=candidate.get("volume_24h", 0) or 0,
                    market_cap=candidate.get("market_cap", 0) or 0,
                    vol_mcap_ratio=candidate.get("vol_mcap_ratio", 0) or 0,
                    is_trending=bool(candidate.get("is_trending")),
                    social_volume=candidate.get("social_volume", 0) or 0,
                    galaxy_score=candidate.get("galaxy_score", 0) or 0,
                    telegram_mentions=candidate.get("telegram_mentions", 0) or 0,
                    telegram_sources=candidate.get("telegram_sources", 0) or 0,
                    bullish_mentions=candidate.get("bullish_mentions", 0) or 0,
                    bearish_mentions=candidate.get("bearish_mentions", 0) or 0,
                )
                if reject_reasons:
                    if (candidate.get("telegram_mentions", 0) or 0) > 0 or (candidate.get("telegram_sources", 0) or 0) > 0:
                        telegram_pipeline_records.append({
                            "symbol": candidate.get("symbol"),
                            "stage": "candidate_gate",
                            "reasons": reject_reasons,
                            "candidate_origin": candidate.get("candidate_origin"),
                            "telegram_mentions": candidate.get("telegram_mentions", 0),
                            "telegram_sources": candidate.get("telegram_sources", 0),
                            "avg_score": candidate.get("telegram_avg_score", 0),
                        })
                    continue
                candidates.append(candidate)
                candidate_lookup[candidate["symbol"]] = candidate

            promoted_telegram_symbols: List[str] = []
            for early_signal in telegram_early_signals:
                symbol = (early_signal.get("symbol") or "").upper()
                if not symbol:
                    continue
                if symbol in candidate_lookup:
                    candidate_lookup[symbol]["candidate_origin"] = "coingecko_scan+telegram_heat"
                    candidate_lookup[symbol]["telegram_early_seed"] = early_signal
                    promoted_telegram_symbols.append(symbol)
                    telegram_pipeline_records.append({
                        "symbol": symbol,
                        "stage": "telegram_seed_attached",
                        "reasons": ["existing_market_candidate_gained_telegram_confirmation"],
                        "candidate_origin": candidate_lookup[symbol].get("candidate_origin"),
                        "telegram_mentions": early_signal.get("mentions", 0),
                        "telegram_sources": early_signal.get("unique_sources", 0),
                        "avg_score": early_signal.get("avg_score", 0),
                    })
                    continue
                if not early_signal.get("candidate_ready"):
                    telegram_pipeline_records.append({
                        "symbol": symbol,
                        "stage": "telegram_seed_gate",
                        "reasons": ["insufficient_cross_source_confirmation"],
                        "candidate_origin": "telegram_early",
                        "telegram_mentions": early_signal.get("mentions", 0),
                        "telegram_sources": early_signal.get("unique_sources", 0),
                        "avg_score": early_signal.get("avg_score", 0),
                    })
                    continue

                market_row = await asyncio.to_thread(
                    get_coingecko_market_snapshot,
                    symbol,
                    early_signal.get("coin_name"),
                    early_signal.get("coin_id"),
                )
                if not market_row:
                    telegram_pipeline_records.append({
                        "symbol": symbol,
                        "stage": "market_data_lookup",
                        "reasons": ["missing_market_data_for_telegram_seed"],
                        "candidate_origin": "telegram_early",
                        "telegram_mentions": early_signal.get("mentions", 0),
                        "telegram_sources": early_signal.get("unique_sources", 0),
                        "avg_score": early_signal.get("avg_score", 0),
                    })
                    continue

                candidate = build_market_candidate_record(
                    market_row,
                    lc_lookup=lc_lookup,
                    telegram_stats_map=telegram_stats_map,
                    trending_symbols=trending_symbols,
                    origin="telegram_early",
                    telegram_seed=early_signal,
                )
                reject_reasons = explain_market_candidate_rejection(
                    price=candidate.get("price", 0) or 0,
                    volume_24h=candidate.get("volume_24h", 0) or 0,
                    market_cap=candidate.get("market_cap", 0) or 0,
                    vol_mcap_ratio=candidate.get("vol_mcap_ratio", 0) or 0,
                    is_trending=bool(candidate.get("is_trending")),
                    social_volume=candidate.get("social_volume", 0) or 0,
                    galaxy_score=candidate.get("galaxy_score", 0) or 0,
                    telegram_mentions=candidate.get("telegram_mentions", 0) or 0,
                    telegram_sources=candidate.get("telegram_sources", 0) or 0,
                    bullish_mentions=candidate.get("bullish_mentions", 0) or 0,
                    bearish_mentions=candidate.get("bearish_mentions", 0) or 0,
                )
                hard_reject_reasons = [reason for reason in reject_reasons if reason != "insufficient_market_social_or_telegram_activity"]
                if hard_reject_reasons:
                    telegram_pipeline_records.append({
                        "symbol": symbol,
                        "stage": "telegram_candidate_gate",
                        "reasons": hard_reject_reasons,
                        "candidate_origin": "telegram_early",
                        "telegram_mentions": early_signal.get("mentions", 0),
                        "telegram_sources": early_signal.get("unique_sources", 0),
                        "avg_score": early_signal.get("avg_score", 0),
                    })
                    continue

                candidate["candidate_gate_override_reason"] = "telegram_cross_source_seed"
                candidate["telegram_early_seed"] = early_signal
                candidates.append(candidate)
                candidate_lookup[symbol] = candidate
                promoted_telegram_symbols.append(symbol)
                telegram_pipeline_records.append({
                    "symbol": symbol,
                    "stage": "telegram_promoted_candidate",
                    "reasons": ["cross_source_telegram_seed_promoted_into_ai_pipeline"],
                    "candidate_origin": "telegram_early",
                    "telegram_mentions": early_signal.get("mentions", 0),
                    "telegram_sources": early_signal.get("unique_sources", 0),
                    "avg_score": early_signal.get("avg_score", 0),
                })

            logger.info(f"Filtered to {len(candidates)} valid coins from {len(cg_data)} total (%s telegram-promoted)", len(promoted_telegram_symbols))

            # Cross-source correlation: Reddit + Telegram + Whale boost
            reddit_symbols = {(r.get("symbol") or "").upper() for r in reddit_rss_signals if r.get("symbol")}
            telegram_symbols = {(s.get("symbol") or "").upper() for s in telegram_early_signals if s.get("symbol")}
            for candidate in candidates:
                sym = (candidate.get("symbol") or "").upper()
                in_reddit = sym in reddit_symbols
                in_telegram = sym in telegram_symbols
                whale = candidate.get("whale_activity") or {}
                has_whale = bool(whale.get("accumulation_detected") or whale.get("whale_score", 0) > 30)
                boost = 0
                sources = []
                if in_reddit and in_telegram:
                    boost += 15
                    sources.append("reddit+telegram")
                elif in_reddit:
                    boost += 8
                    sources.append("reddit")
                if has_whale and (in_reddit or in_telegram):
                    boost += 10
                    sources.append("whale")
                if boost > 0:
                    candidate["pump_strength"] = min(100, (candidate.get("pump_strength") or 0) + boost)
                    candidate["cross_source_confirmed"] = True
                    candidate["cross_source_tags"] = sources
                    if boost >= 25:
                        candidate["multi_source_badge"] = "MULTI_SOURCE_CONFIRMED"
                    logger.info(f"Cross-source boost {sym}: +{boost} ({', '.join(sources)})")

            ai_result = await analyze_signals_with_ai(candidates, fear_greed, trending_symbols)
            cg_lookup = {c["symbol"].upper(): c for c in candidates}

            def enrich_signal(sig: dict, signal_type: str) -> dict:
                sym = sig.get("symbol", "").upper()
                market = cg_lookup.get(sym, {})
                if not market:
                    return None

                price = market.get("price") or 0
                if price <= 0:
                    return None

                direction_audit = build_direction_audit(
                    symbol=sym,
                    price_change_1h=market.get("price_change_1h") or 0,
                    price_change_24h=market.get("price_change_24h") or 0,
                    price_change_7d=market.get("price_change_7d") or 0,
                    volume_24h=market.get("volume_24h") or 0,
                    market_cap=market.get("market_cap") or 0,
                    signal_type_hint=signal_type,
                    signal_strength_hint=sig.get("signal_strength", 0),
                    pump_strength=sig.get("signal_strength", 0) if signal_type == "pump" else market.get("pump_strength", 0),
                    dump_strength=sig.get("signal_strength", 0) if signal_type == "dump" else market.get("dump_strength", 0),
                    is_trending=market.get("is_trending", False),
                )
                resolved_signal_type = direction_audit.get("resolved_direction", signal_type)
                if resolved_signal_type != signal_type:
                    logger.info(
                        "Direction audit rerouted %s from %s to %s (1h=%+.2f 24h=%+.2f 7d=%+.2f gap=%.2f transition=%s)",
                        sym,
                        signal_type,
                        resolved_signal_type,
                        direction_audit.get("price_change_1h", 0.0),
                        direction_audit.get("price_change_24h", 0.0),
                        direction_audit.get("price_change_7d", 0.0),
                        direction_audit.get("score_gap", 0.0),
                        direction_audit.get("transition_state", "unknown"),
                    )

                market_details = get_coin_extended_details(market.get("id", ""))
                market_platform, market_contract = pick_primary_contract(market_details)
                venues = build_market_venues(sym, market.get("id", ""), market_platform, market_contract)
                asset_identity = build_asset_identity_profile(
                    symbol=sym,
                    coin_id=market.get("id", ""),
                    name=market.get("name", sym),
                    market_cap=market.get("market_cap") or 0,
                    details=market_details,
                    venues=venues,
                )
                tokenomics = build_tokenomics_profile(market_details) if market_details else {
                    "circulating_supply": None,
                    "total_supply": None,
                    "max_supply": None,
                    "fdv_usd": None,
                    "market_cap_usd": market.get("market_cap") or 0,
                    "circulating_ratio_pct": None,
                    "dilution_gap_pct": None,
                    "unlock_risk": "Unknown",
                    "warnings": [],
                    "source": "CoinGecko",
                }
                decision_engine = build_signal_execution_plan(
                    signal_type=resolved_signal_type,
                    symbol=sym,
                    price=price,
                    price_change_1h=market.get("price_change_1h") or 0,
                    price_change_24h=market.get("price_change_24h") or 0,
                    price_change_7d=market.get("price_change_7d") or 0,
                    volume_24h=market.get("volume_24h") or 0,
                    market_cap=market.get("market_cap") or 0,
                    signal_strength=float(sig.get("signal_strength") or 0) if str(sig.get("signal_strength", "0")).replace(".", "").isdigit() else 70,
                    confidence=sig.get("confidence", "medium"),
                    risk_level=sig.get("risk_level", "medium"),
                    venues=venues,
                    direction_audit=direction_audit,
                )
                source_stack = build_signal_source_stack(
                    symbol=sym,
                    price_change_1h=market.get("price_change_1h") or 0,
                    price_change_24h=market.get("price_change_24h") or 0,
                    vol_mcap_ratio=market.get("vol_mcap_ratio") or 0,
                    is_trending=market.get("is_trending", False),
                    telegram_mentions=market.get("telegram_mentions", 0) or 0,
                    telegram_sources=market.get("telegram_sources", 0) or 0,
                    bullish_mentions=market.get("bullish_mentions", 0) or 0,
                    bearish_mentions=market.get("bearish_mentions", 0) or 0,
                    telegram_avg_score=market.get("telegram_avg_score", 0) or 0,
                    social_volume=market.get("social_volume") or 0,
                    galaxy_score=market.get("galaxy_score") or 0,
                    sentiment=market.get("sentiment") or 0,
                    lunar_mentions=market.get("lunar_mentions", 0) or 0,
                    lunar_creators=market.get("lunar_creators", 0) or 0,
                    lunar_interactions=market.get("lunar_interactions", 0) or 0,
                    lunar_dominance=market.get("lunar_dominance", 0) or 0,
                    venue_count=decision_engine.get("venue_count", 0) or len(venues),
                    preferred_venue=decision_engine.get("preferred_venue"),
                )

                holder_distribution = {"available": False}
                goplus_security = {"available": False}
                goplus_rugpull = {"available": False}
                wallet_cluster_intelligence = {
                    "available": False,
                    "cluster_risk_score": None,
                    "combined_insider_pct": None,
                    "top_10_pct": None,
                    "long_tail_pct": None,
                    "warnings": [],
                    "summary": "No verified wallet clustering data yet.",
                }
                contract_risk = {
                    "available": False,
                    "risk_score": None,
                    "risk_level": None,
                    "warnings": [],
                    "source": "GoPlus",
                }
                whale_activity = {"whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": "no_contract"}
                if market_contract:
                    holder_distribution = get_holder_distribution(market_platform, market_contract)
                    goplus_security = get_goplus_security(market_platform, market_contract)
                    goplus_rugpull = get_goplus_rugpull(market_platform, market_contract)
                    wallet_cluster_intelligence = build_wallet_cluster_intelligence(holder_distribution, goplus_security)
                    contract_risk = build_contract_risk_profile(market_platform, market_contract, goplus_security, goplus_rugpull)
                    whale_activity = fetch_whale_activity(market_contract, sym, market_platform)

                manipulation_profile = build_manipulation_profile(
                    signal_type=resolved_signal_type,
                    symbol=sym,
                    price_change_1h=market.get("price_change_1h") or 0,
                    price_change_24h=market.get("price_change_24h") or 0,
                    price_change_7d=market.get("price_change_7d") or 0,
                    volume_24h=market.get("volume_24h") or 0,
                    market_cap=market.get("market_cap") or 0,
                    signal_strength=float(sig.get("signal_strength") or 0) if str(sig.get("signal_strength", "0")).replace(".", "").isdigit() else 70,
                    risk_level=sig.get("risk_level", "medium"),
                    is_trending=market.get("is_trending", False),
                    social_volume=market.get("social_volume") or 0,
                    sentiment=market.get("sentiment") or 0,
                    galaxy_score=market.get("galaxy_score") or 0,
                    decision_engine=decision_engine,
                    telegram_stats=telegram_stats_map.get(sym),
                    direction_audit=direction_audit,
                )
                rugpull_profile = build_rugpull_profile(
                    asset_identity=asset_identity,
                    tokenomics=tokenomics,
                    wallet_cluster_intelligence=wallet_cluster_intelligence,
                    contract_risk=contract_risk,
                    venues=venues,
                    manipulation_profile=manipulation_profile,
                )
                manipulation_timeline = build_manipulation_timeline(
                    symbol=sym,
                    signal_type=resolved_signal_type,
                    manipulation_profile=manipulation_profile,
                    decision_engine=decision_engine,
                    fear_greed=fear_greed,
                    is_trending=market.get("is_trending", False),
                    social_volume=market.get("social_volume") or 0,
                    galaxy_score=market.get("galaxy_score") or 0,
                )
                signal_v2 = build_signal_v2(
                    symbol=sym,
                    name=market.get("name", sym),
                    chain=market_platform,
                    contract_or_mint=market_contract,
                    signal_type=resolved_signal_type,
                    signal_strength=float(sig.get("signal_strength") or 0) if str(sig.get("signal_strength", "0")).replace(".", "").isdigit() else 70,
                    confidence=sig.get("confidence", "medium"),
                    risk_level=sig.get("risk_level", "medium"),
                    market=market,
                    source_stack=source_stack,
                    manipulation_profile=manipulation_profile,
                    decision_engine=decision_engine,
                    rugpull_profile=rugpull_profile,
                    asset_identity=asset_identity,
                )

                return {
                    **sig,
                    "signal_type": resolved_signal_type,
                    "requested_signal_type": signal_type,
                    "candidate_origin": market.get("candidate_origin", "coingecko_scan"),
                    "candidate_gate_override_reason": market.get("candidate_gate_override_reason"),
                    "telegram_early_seed": market.get("telegram_early_seed"),
                    "symbol": sym,
                    "name": market.get("name", sym),
                    "id": market.get("id"),
                    "price": price,
                    "price_change_1h": market.get("price_change_1h"),
                    "price_change_24h": market.get("price_change_24h"),
                    "price_change_7d": market.get("price_change_7d"),
                    "volume_24h": market.get("volume_24h"),
                    "market_cap": market.get("market_cap"),
                    "social_volume": market.get("social_volume"),
                    "sentiment": market.get("sentiment"),
                    "galaxy_score": market.get("galaxy_score"),
                    "image": market.get("image"),
                    "is_trending": market.get("is_trending", False),
                    "direction_audit": direction_audit,
                    "decision_engine": decision_engine,
                    "preferred_venue": decision_engine.get("preferred_venue"),
                    "signal_sources": source_stack,
                    "source_summary": source_stack.get("summary"),
                    "asset_identity": asset_identity,
                    "rugpull_profile": rugpull_profile,
                    "manipulation_profile": manipulation_profile,
                    "whale_activity": whale_activity if market_contract else {"whale_score": 0, "accumulation_detected": False, "dump_risk": False, "error": "no_contract"},
                    "manipulation_timeline": manipulation_timeline,
                    "signal_v2": signal_v2,
                    "x_data": {},
                    "timestamp": datetime.now(timezone.utc),
                }


            all_enriched_signals = [
                s for s in (
                    [enrich_signal(s, "pump") for s in ai_result.get("pump_signals", [])] +
                    [enrich_signal(s, "dump") for s in ai_result.get("dump_signals", [])]
                ) if s is not None
            ]
            deduped_signals: Dict[str, dict] = {}
            for enriched_signal in all_enriched_signals:
                symbol_key = enriched_signal.get("symbol", "").upper()
                if not symbol_key:
                    continue
                existing = deduped_signals.get(symbol_key)
                if not existing:
                    deduped_signals[symbol_key] = enriched_signal
                    continue
                current_rank = (
                    float(existing.get("signal_strength", 0) or 0),
                    float((existing.get("direction_audit") or {}).get("score_gap", 0) or 0),
                    abs(float(existing.get("price_change_24h", 0) or 0)),
                )
                candidate_rank = (
                    float(enriched_signal.get("signal_strength", 0) or 0),
                    float((enriched_signal.get("direction_audit") or {}).get("score_gap", 0) or 0),
                    abs(float(enriched_signal.get("price_change_24h", 0) or 0)),
                )
                if candidate_rank > current_rank:
                    deduped_signals[symbol_key] = enriched_signal

            # Post-processing: reclassify dump -> risk when 24h is large positive (late pump / distribution)
            for sym_key, enriched_signal in deduped_signals.items():
                if enriched_signal.get("signal_type") == "dump":
                    try:
                        ch24 = float(enriched_signal.get("price_change_24h") or 0)
                    except Exception:
                        ch24 = 0.0
                    if ch24 >= 30:
                        enriched_signal["signal_type"] = "risk"
                        enriched_signal["direction"] = "risk"
                        if not enriched_signal.get("verdict") or enriched_signal.get("verdict") == "Dump Risk / Thin Liquidity":
                            enriched_signal["verdict"] = "Late Pump / Distribution Risk"
                            enriched_signal["final_verdict"] = "Late Pump / Distribution Risk"
                        enriched_signal["action"] = enriched_signal.get("action") or "avoid_chasing"
            gated_pump_signals: List[dict] = []
            gated_dump_signals: List[dict] = []
            for enriched_signal in deduped_signals.values():
                resolved_signal_type = enriched_signal.get("signal_type")
                if resolved_signal_type not in {"pump", "dump", "risk"}:
                    continue
                gate = evaluate_snapshot_signal_gate(enriched_signal, resolved_signal_type if resolved_signal_type != "risk" else "dump")
                enriched_signal["snapshot_gate"] = gate
                if gate.get("eligible"):
                    if resolved_signal_type == "pump":
                        gated_pump_signals.append(enriched_signal)
                    elif resolved_signal_type == "risk":
                        gated_dump_signals.append(enriched_signal)
                    else:
                        gated_dump_signals.append(enriched_signal)
                    continue
                manipulation_profile = enriched_signal.get("manipulation_profile") or {}
                if (
                    enriched_signal.get("candidate_origin") == "telegram_early" or
                    int(manipulation_profile.get("telegram_mentions") or 0) > 0 or
                    int(manipulation_profile.get("telegram_sources") or 0) > 0
                ):
                    telegram_pipeline_records.append({
                        "symbol": enriched_signal.get("symbol"),
                        "stage": "snapshot_gate",
                        "reasons": gate.get("reasons", []),
                        "candidate_origin": enriched_signal.get("candidate_origin"),
                        "signal_type": resolved_signal_type,
                        "signal_strength": enriched_signal.get("signal_strength", 0),
                        "telegram_mentions": manipulation_profile.get("telegram_mentions", 0),
                        "telegram_sources": manipulation_profile.get("telegram_sources", 0),
                        "avg_score": ((enriched_signal.get("telegram_early_seed") or {}).get("avg_score") or 0),
                    })

            pump_signals = sorted(
                gated_pump_signals,
                key=lambda item: (
                    item.get("signal_strength", 0),
                    (item.get("direction_audit") or {}).get("score_gap", 0),
                ),
                reverse=True,
            )[:8]
            dump_signals = sorted(
                gated_dump_signals,
                key=lambda item: (
                    item.get("signal_strength", 0),
                    (item.get("direction_audit") or {}).get("score_gap", 0),
                ),
                reverse=True,
            )[:4]

            telegram_payload = api_ok(build_telegram_consensus_payload([], [], 24))
            try:
                dashboard_sources = []
                all_sources = await get_enabled_telegram_sources()
                dashboard_sources = [
                    source for source in all_sources
                    if derive_telegram_source_profile(source).get("quality_badge") in {"High Signal Quality", "Fast but Risky"}
                ]
                source_ids = [str(source["_id"]) for source in dashboard_sources]
                dashboard_signals = []
                if source_ids:
                    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
                    dashboard_signals = await db.telegram_signals.find({
                        "source_id": {"$in": source_ids},
                        "posted_at": {"$gte": cutoff},
                    }).sort("posted_at", -1).limit(300).to_list(length=300)
                telegram_payload = api_ok(build_telegram_consensus_payload(dashboard_signals, dashboard_sources, 24))
            except Exception as e:
                logger.warning(f"Precompute telegram consensus failed: {e}")

            snapshot_at = datetime.now(timezone.utc)
            stage_counts: Dict[str, int] = {}
            for record in telegram_pipeline_records:
                stage_name = record.get("stage") or "unknown"
                stage_counts[stage_name] = stage_counts.get(stage_name, 0) + 1

            geckoterminal_candidates = {
                "generated_at": snapshot_at,
                "solana_trending": fetch_geckoterminal_pool_candidates("solana", mode="trending", limit=12),
                "solana_new": fetch_geckoterminal_pool_candidates("solana", mode="new", limit=12),
                "ethereum_trending": fetch_geckoterminal_pool_candidates("ethereum", mode="trending", limit=8),
            }

            geckoterminal_experimental_signals = []
            for bucket_name in ["solana_trending", "solana_new", "ethereum_trending"]:
                for candidate in geckoterminal_candidates.get(bucket_name, []) or []:
                    try:
                        signal = build_geckoterminal_signal_v2(candidate)
                        signal["candidate_bucket"] = bucket_name
                        geckoterminal_experimental_signals.append(signal)
                    except Exception as exc:
                        logger.warning("Failed to build GeckoTerminal signal_v2 for %s: %s", candidate.get("symbol"), exc)

            def geckoterminal_has_unsafe_name(item: dict) -> bool:
                text = f"{item.get('symbol') or ''} {item.get('name') or ''}".strip().lower()
                unsafe_terms = {
                    "porn", "pornhub", "sex", "xxx", "hentai", "shit", "fuck",
                    "nazi", "hitler", "rape", "cum", "dick", "pussy"
                }
                return any(term in text for term in unsafe_terms)

            for item in geckoterminal_experimental_signals:
                unsafe_name = geckoterminal_has_unsafe_name(item)
                item["unsafe_symbol_name"] = unsafe_name
                if unsafe_name:
                    flags = list(item.get("red_flags") or [])
                    flags.append("unsafe_symbol_name")
                    item["red_flags"] = sorted(set(flags))

            verdict_priority = {
                "High-Risk Pump": 90,
                "Dump Risk": 85,
                "Pump Watch": 80,
                "Distribution": 70,
                "Early DEX Watch": 60,
                "Noise": 10,
            }

            def geckoterminal_dedupe_key(item: dict) -> str:
                return (
                    item.get("contract_or_mint")
                    or item.get("pool_address")
                    or f"{item.get('chain') or 'unknown'}:{item.get('symbol') or 'unknown'}"
                )

            def geckoterminal_rank(item: dict) -> tuple:
                return (
                    verdict_priority.get(item.get("verdict"), 0),
                    float(item.get("manipulation_setup_score") or 0),
                    float(item.get("confidence") or 0),
                    -float(item.get("noise_score") or 0),
                )

            deduped_geckoterminal_signals_by_key = {}
            for item in geckoterminal_experimental_signals:
                key = geckoterminal_dedupe_key(item)
                current = deduped_geckoterminal_signals_by_key.get(key)
                if current is None or geckoterminal_rank(item) > geckoterminal_rank(current):
                    deduped_geckoterminal_signals_by_key[key] = item

            geckoterminal_experimental_signals = list(deduped_geckoterminal_signals_by_key.values())

            geckoterminal_experimental_signals.sort(
                key=lambda item: geckoterminal_rank(item),
                reverse=True,
            )

            actionable_verdicts = {
                "High-Risk Pump",
                "Pump Watch",
                "Early DEX Watch",
                "Dump Risk",
                "Distribution",
            }

            actionable_geckoterminal_candidates = [
                item for item in geckoterminal_experimental_signals
                if item.get("verdict") in actionable_verdicts
                and not item.get("unsafe_symbol_name")
            ]

            unsafe_geckoterminal = [
                item for item in geckoterminal_experimental_signals
                if item.get("unsafe_symbol_name")
            ]

            def geckoterminal_quality_gate(item: dict) -> bool:
                verdict = item.get("verdict")
                confidence = float(item.get("confidence") or 0)
                red_flags = set(item.get("red_flags") or [])

                mc = item.get("market_context") or {}
                pc = mc.get("price_change_pct") or {}
                change_24h = float(pc.get("h24") or 0)
                change_1h = float(pc.get("h1") or 0)
                reserve_usd = float(mc.get("reserve_usd") or 0)
                if reserve_usd < 30000: return False
                if change_24h >= 300 and change_1h < 0: return False
                if change_24h >= 500: return False
                if verdict in {"High-Risk Pump", "Pump Watch"}:
                    return confidence >= 55
                if verdict == "Early DEX Watch":
                    return confidence >= 55
                if verdict == "Dump Risk":
                    return confidence >= 55 or "pump_then_reversal" in red_flags
                if verdict == "Distribution":
                    return confidence >= 55
                return False

            quality_pass_geckoterminal = [
                item for item in actionable_geckoterminal_candidates
                if geckoterminal_quality_gate(item)
            ]

            avoid_geckoterminal = [
                item for item in quality_pass_geckoterminal
                if item.get("action") == "avoid"
            ]

            actionable_geckoterminal_signals = [
                item for item in quality_pass_geckoterminal
                if item.get("action") != "avoid"
            ]

            low_quality_geckoterminal = [
                item for item in actionable_geckoterminal_candidates
                if not geckoterminal_quality_gate(item)
            ]

            rejected_geckoterminal_noise = [
                item for item in geckoterminal_experimental_signals
                if item.get("verdict") == "Noise"
            ]

            experimental_signals_v2 = {
                "generated_at": snapshot_at,
                "geckoterminal": geckoterminal_experimental_signals[:40],
                "actionable_geckoterminal": actionable_geckoterminal_signals[:25],
                "avoid_geckoterminal": avoid_geckoterminal[:25],
                "low_quality_geckoterminal": low_quality_geckoterminal[:25],
                "unsafe_geckoterminal": unsafe_geckoterminal[:25],
                "rejected_geckoterminal_noise": rejected_geckoterminal_noise[:40],
                "summary": {
                    "geckoterminal_total": len(geckoterminal_experimental_signals),
                    "actionable_geckoterminal_count": len(actionable_geckoterminal_signals),
                    "avoid_geckoterminal_count": len(avoid_geckoterminal),
                    "low_quality_geckoterminal_count": len(low_quality_geckoterminal),
                    "unsafe_geckoterminal_count": len(unsafe_geckoterminal),
                    "rejected_geckoterminal_noise_count": len(rejected_geckoterminal_noise),
                    "geckoterminal_deduped_count": len(deduped_geckoterminal_signals_by_key),
                    "unsafe_symbol_name_count": len([item for item in geckoterminal_experimental_signals if item.get("unsafe_symbol_name")]),
                },
            }

            snapshot = {
                "timestamp": snapshot_at,
                "pump_signals": pump_signals,
                "dump_signals": dump_signals,
                "geckoterminal_candidates": geckoterminal_candidates,
                "experimental_signals_v2": experimental_signals_v2,
                "telegram_early_signals": telegram_early_signals,
                "reddit_rss_signals": reddit_rss_signals,
                "telegram_pipeline_audit": {
                    "window_hours": TELEGRAM_EARLY_SIGNAL_HOURS,
                    "early_signal_count": len(telegram_early_signals),
                    "promotion_count": len(promoted_telegram_symbols),
                    "promoted_symbols": promoted_telegram_symbols[:12],
                    "stage_counts": stage_counts,
                    "records": telegram_pipeline_records[:40],
                },
                "market_summary": ai_result.get("market_summary", ""),
                "coins_analyzed": len(candidates),
                "fear_greed": fear_greed,
                "trending": trending_symbols[:10],
                "source_pipeline": {
                    "telegram_enabled": True,
                    "lunarcrush_enabled": bool(lc_data),
                    "coingecko_enabled": bool(cg_data),
                    "x_enabled": bool(X_BEARER_TOKEN and not looks_like_placeholder(X_BEARER_TOKEN, "X_BEARER_TOKEN")),
                },
            }
            telegram_data = telegram_payload.get("data") if isinstance(telegram_payload, dict) else None
            snapshot["telegram_consensus_precomputed"] = telegram_data
            snapshot["cross_platform_consensus_precomputed"] = build_dashboard_cross_platform_consensus(snapshot, telegram_data)
            snapshot["fresh_manipulation_alerts_precomputed"] = build_intelligence_alerts(snapshot, telegram_data)
            await db.signal_snapshots.insert_one(snapshot)
            await persist_telegram_pipeline_audit(telegram_pipeline_records, snapshot_at, trigger)
            calibration_summary = await build_telegram_calibration_summary(72)
            snapshot["telegram_calibration_summary"] = calibration_summary
            await db.signal_snapshots.update_one(
                {"timestamp": snapshot_at},
                {"$set": {"telegram_calibration_summary": calibration_summary}},
            )

            job_result.update({
                "completed": True,
                "pump_count": len(pump_signals),
                "dump_count": len(dump_signals),
                "coins_analyzed": len(candidates),
                "snapshot_at": snapshot.get("timestamp"),
            })

            if pump_signals or dump_signals:
                asyncio.create_task(check_and_send_alerts(pump_signals, dump_signals))

            count = await db.signal_snapshots.count_documents({})
            if count > 48:
                oldest = await db.signal_snapshots.find({}).sort("timestamp", 1).limit(count - 48).to_list(length=100)
                old_ids = [d["_id"] for d in oldest]
                await db.signal_snapshots.delete_many({"_id": {"$in": old_ids}})

            logger.info(f"Signal job complete: {len(pump_signals)} pump, {len(dump_signals)} dump signals")
            return {
                **job_result,
                "scan_status": build_signal_scan_status(),
            }
        except Exception as e:
            SIGNAL_SCAN_STATE["last_error"] = str(e)
            logger.exception(f"Signal fetch job error: {e}")
            return {
                **job_result,
                "completed": False,
                "error": str(e),
                "scan_status": build_signal_scan_status(),
            }
        finally:
            finished_at = datetime.now(timezone.utc)
            SIGNAL_SCAN_STATE.update({
                "running": False,
                "finished_at": finished_at,
                "last_snapshot_at": job_result.get("snapshot_at"),
                "last_result": {
                    "pump_count": job_result.get("pump_count", 0),
                    "dump_count": job_result.get("dump_count", 0),
                    "coins_analyzed": job_result.get("coins_analyzed", 0),
                    "completed": bool(job_result.get("completed")),
                },
            })

# ─────────────────────────────────────────────
# CRYPTO SIGNAL ENDPOINTS
# ─────────────────────────────────────────────
def serialize_signal(s: dict) -> dict:
    """Remove MongoDB _id and serialize datetime"""
    s.pop("_id", None)
    if isinstance(s.get("timestamp"), datetime):
        s["timestamp"] = s["timestamp"].isoformat()
    return s

async def build_dashboard_new_algorithm_signals(snapshot: Optional[dict], limit: int = 6) -> List[dict]:
    if not snapshot:
        return []

    snapshot_ts = snapshot.get("timestamp")
    snapshot_key = snapshot_ts.isoformat() if isinstance(snapshot_ts, datetime) else str(snapshot_ts)
    cache_key = f"new_algorithm::{snapshot_key}"
    cached_rows = get_memory_cache(NEW_ALGORITHM_SIGNALS_CACHE, cache_key, ttl_seconds=300)
    if cached_rows is not None:
        return cached_rows

    candidates: List[dict] = []

    def _legacy_signal_allowed_for_new_algorithm(signal: dict) -> bool:
        """Avoid promoting weak legacy CoinGecko-only signals into the decision table."""
        try:
            strength = float(signal.get("signal_strength") or signal.get("confidence_score") or 0)
        except Exception:
            strength = 0.0
        confidence = str(signal.get("confidence") or "").lower()
        reason = str(signal.get("reason") or "").lower()
        ai_source = signal.get("ai_source")
        source_stack = signal.get("source_stack")

        weak_text = any(x in reason for x in [
            "single-source",
            "thin",
            "low conviction",
            "contradictory",
            "evidence is too thin",
        ])

        if confidence == "low":
            return False
        if strength < 50:
            return False
        if weak_text and not ai_source:
            return False
        if not ai_source and not source_stack and weak_text and (strength < 60 or confidence == "low"):
            return False
        return True

    for signal in (snapshot.get("pump_signals", []) or [])[:3]:
        if not _legacy_signal_allowed_for_new_algorithm(signal):
            continue
        candidates.append({
            "source": "pump_signal",
            "signal_type": "pump",
            "symbol": (signal.get("symbol") or "").upper(),
            "name": signal.get("name") or signal.get("symbol"),
            "coin_id": signal.get("id"),
        })

    for signal in (snapshot.get("dump_signals", []) or [])[:3]:
        if not _legacy_signal_allowed_for_new_algorithm(signal):
            continue
        candidates.append({
            "source": "dump_signal",
            "signal_type": "dump",
            "symbol": (signal.get("symbol") or "").upper(),
            "name": signal.get("name") or signal.get("symbol"),
            "coin_id": signal.get("id"),
        })

    for signal in (snapshot.get("telegram_early_signals", []) or [])[:4]:
        candidates.append({
            "source": "telegram_early",
            "signal_type": signal.get("direction") or "pump",
            "symbol": (signal.get("symbol") or "").upper(),
            "name": signal.get("coin_name") or signal.get("symbol"),
            "coin_id": signal.get("coin_id"),
        })

    deduped_candidates: List[dict] = []
    seen_candidates: set[str] = set()
    for candidate in candidates:
        dedupe_key = f"{candidate.get('coin_id') or ''}::{candidate.get('symbol') or ''}"
        if dedupe_key in seen_candidates:
            continue
        seen_candidates.add(dedupe_key)
        deduped_candidates.append(candidate)

    async def analyze_candidate(candidate: dict) -> Optional[dict]:
        symbol = candidate.get("symbol") or ""
        coin_id = candidate.get("coin_id")
        name = candidate.get("name")

        if not coin_id and symbol:
            coin_id = await asyncio.to_thread(resolve_coingecko_coin_id, symbol, name)
        if not coin_id:
            return None

        details = await asyncio.to_thread(get_coin_extended_details, coin_id)
        platform_id, contract_address = pick_primary_contract(details)
        if not platform_id or not contract_address:
            return None

        pump_engine_payload = await build_coin_pump_engine_payload(platform_id, contract_address)
        analysis = pump_engine_payload.get("analysis") if pump_engine_payload.get("available") else None
        if not analysis:
            return None

        return {
            "symbol": symbol,
            "name": name or symbol,
            "source": candidate.get("source"),
            "signal_type": candidate.get("signal_type"),
            "platform_id": platform_id,
            "contract_address": contract_address,
            "analysis": analysis,
        }

    tasks = [analyze_candidate(candidate) for candidate in deduped_candidates[:limit]]
    rows = [row for row in await asyncio.gather(*tasks) if row]

    filtered_rows = []
    for row in rows:
        analysis = row.get("analysis") or {}
        final_block = analysis.get("final") or {}
        ai_block = analysis.get("ai_judge") or {}
        rule_block = analysis.get("rule_engine") or {}

        final_verdict = (
            final_block.get("verdict")
            or ai_block.get("final_verdict")
            or rule_block.get("verdict")
            or ""
        )
        final_action = (
            final_block.get("action")
            or ai_block.get("final_action")
            or rule_block.get("action")
            or ""
        )
        final_score = (
            final_block.get("confidence")
            or ai_block.get("confidence")
            or rule_block.get("score")
            or 0
        )

        verdict_text = str(final_verdict).strip().lower()
        action_text = str(final_action).strip().upper()

        if verdict_text == "noise":
            continue
        if action_text == "WATCH" and verdict_text in {"likely_noise", "coordinated_noise"}:
            continue

        row["_display_score"] = float(final_score or 0)
        filtered_rows.append(row)

    filtered_rows.sort(key=lambda item: float(item.get("_display_score", 0) or 0), reverse=True)
    return set_memory_cache(NEW_ALGORITHM_SIGNALS_CACHE, cache_key, filtered_rows[:limit])



@app.get("/api/crypto/experimental/geckoterminal/summary")
async def get_experimental_geckoterminal_summary():
    """Return compact summary for experimental GeckoTerminal Signal v2 buckets."""
    snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
    if not snapshot:
        return api_ok({
            "timestamp": None,
            "summary": {},
            "top_actionable": [],
            "top_avoid": [],
            "top_unsafe": [],
        })

    experimental = snapshot.get("experimental_signals_v2") or {}

    def compact_item(item: dict) -> dict:
        solana_safety = item.get("solana_safety") or {}
        solana_dex_context = item.get("solana_dex_context") or {}
        return {
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "chain": item.get("chain"),
            "dex": item.get("dex"),
            "direction": item.get("direction"),
            "verdict": item.get("verdict"),
            "action": item.get("action"),
            "confidence": item.get("confidence"),
            "manipulation_setup_score": item.get("manipulation_setup_score"),
            "noise_score": item.get("noise_score"),
            "unsafe_symbol_name": item.get("unsafe_symbol_name"),
            "red_flags": item.get("red_flags") or [],
            "contract_or_mint": item.get("contract_or_mint"),
            "pool_url": item.get("pool_url"),
            "reserve_usd": ((item.get("tradeability") or {}).get("reserve_usd")),
            "volume_h24": ((item.get("tradeability") or {}).get("volume_h24")),
            "trigger": item.get("trigger"),

            "dex_family": solana_dex_context.get("dex_family"),
            "launch_context": solana_dex_context.get("launch_context"),
            "is_pumpfun_related": solana_dex_context.get("is_pumpfun_related"),
            "is_new_pool": solana_dex_context.get("is_new_pool"),
            "is_trending_pool": solana_dex_context.get("is_trending_pool"),
            "solana_meme_risk_flags": solana_dex_context.get("solana_meme_risk_flags") or [],

            "solana_safety_status": solana_safety.get("solana_safety_status"),
            "solana_safety_available": solana_safety.get("available"),
            "holder_count": solana_safety.get("holder_count"),
            "top_holder_percent": solana_safety.get("top_holder_percent"),
            "safety_red_flags": solana_safety.get("safety_red_flags") or [],
        }

    actionable = experimental.get("actionable_geckoterminal") or []
    avoid = experimental.get("avoid_geckoterminal") or []
    unsafe = experimental.get("unsafe_geckoterminal") or []

    summary = dict(experimental.get("summary") or {})
    all_items = (
        list(experimental.get("actionable_geckoterminal") or [])
        + list(experimental.get("avoid_geckoterminal") or [])
        + list(experimental.get("low_quality_geckoterminal") or [])
        + list(experimental.get("unsafe_geckoterminal") or [])
        + list(experimental.get("rejected_geckoterminal_noise") or [])
    )

    solana_items = [item for item in all_items if item.get("chain") == "solana"]
    solana_safety_available = [
        item for item in solana_items
        if (item.get("solana_safety") or {}).get("available")
    ]
    solana_holder_available = [
        item for item in solana_items
        if (item.get("solana_safety") or {}).get("holder_count") is not None
        or (item.get("solana_safety") or {}).get("top_holder_percent") is not None
    ]
    solana_risk_flags = [
        item for item in solana_items
        if (item.get("solana_safety") or {}).get("safety_red_flags")
    ]

    solana_dex_context_available = [
        item for item in solana_items
        if (item.get("solana_dex_context") or {}).get("available")
    ]
    solana_new_pool = [
        item for item in solana_items
        if (item.get("solana_dex_context") or {}).get("is_new_pool")
    ]
    solana_pumpfun_related = [
        item for item in solana_items
        if (item.get("solana_dex_context") or {}).get("is_pumpfun_related")
    ]

    summary.update({
        "solana_geckoterminal_count": len(solana_items),
        "solana_safety_available_count": len(solana_safety_available),
        "solana_holder_available_count": len(solana_holder_available),
        "solana_safety_risk_flags_count": len(solana_risk_flags),
        "solana_dex_context_available_count": len(solana_dex_context_available),
        "solana_new_pool_count": len(solana_new_pool),
        "solana_pumpfun_related_count": len(solana_pumpfun_related),
    })

    return api_ok({
        "timestamp": snapshot.get("timestamp"),
        "summary": summary,
        "top_actionable": [compact_item(item) for item in actionable[:10]],
        "top_avoid": [compact_item(item) for item in avoid[:10]],
        "top_unsafe": [compact_item(item) for item in unsafe[:10]],
    })



@app.get("/api/crypto/experimental/geckoterminal")
async def get_experimental_geckoterminal_signals():
    """Return latest experimental GeckoTerminal Signal v2 buckets.

    Internal/testing endpoint only.
    Does not affect the main dashboard, pump_signals, or dump_signals.
    """
    snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
    if not snapshot:
        return api_ok({
            "timestamp": None,
            "summary": {},
            "actionable_geckoterminal": [],
            "avoid_geckoterminal": [],
            "low_quality_geckoterminal": [],
            "unsafe_geckoterminal": [],
            "rejected_geckoterminal_noise": [],
            "geckoterminal": [],
        })

    experimental = snapshot.get("experimental_signals_v2") or {}

    return api_ok({
        "timestamp": snapshot.get("timestamp"),
        "summary": experimental.get("summary") or {},
        "actionable_geckoterminal": experimental.get("actionable_geckoterminal") or [],
        "avoid_geckoterminal": experimental.get("avoid_geckoterminal") or [],
        "low_quality_geckoterminal": experimental.get("low_quality_geckoterminal") or [],
        "unsafe_geckoterminal": experimental.get("unsafe_geckoterminal") or [],
        "rejected_geckoterminal_noise": experimental.get("rejected_geckoterminal_noise") or [],
        "geckoterminal": experimental.get("geckoterminal") or [],
    })



def _to_float_safe(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default




async def call_local_qwen_json(
    *,
    system_instruction: str,
    user_prompt: str,
    temperature: float = 0.0,
    max_tokens: int = 80,
) -> dict:
    """Call local llama.cpp Qwen server and parse JSON."""
    import json as json_lib

    url = "http://127.0.0.1:8088/v1/chat/completions"
    payload = {
        "model": "local-qwen",
        "messages": [
            {"role": "system", "content": system_instruction or ""},
            {"role": "user", "content": user_prompt or ""},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    try:
        def _request():
            return requests.post(url, json=payload, timeout=35)

        resp = await asyncio.to_thread(_request)
        if resp.status_code >= 400:
            return {
                "ok": False,
                "provider": "local_qwen",
                "model": "qwen2.5-1.5b-instruct-q4_k_m",
                "status_code": resp.status_code,
                "error": resp.text[:500],
                "json": None,
                "text": "",
            }

        data = resp.json() or {}
        choices = data.get("choices") or []
        text = ""
        if choices:
            text = ((choices[0].get("message") or {}).get("content") or "").strip()

        cleaned = text
        if cleaned.startswith("```"):
            cleaned = "\n".join(
                line for line in cleaned.splitlines()
                if not line.strip().startswith("```")
            ).strip()

        parsed = json_lib.loads(cleaned)
        return {
            "ok": True,
            "provider": "local_qwen",
            "model": "qwen2.5-1.5b-instruct-q4_k_m",
            "json": parsed,
            "text": text,
            "raw": data,
        }

    except Exception as exc:
        return {
            "ok": False,
            "provider": "local_qwen",
            "model": "qwen2.5-1.5b-instruct-q4_k_m",
            "error": str(exc)[:500],
            "json": None,
            "text": "",
        }



async def apply_ai_judge_to_decision_signals(decision_signals: List[dict]) -> List[dict]:
    """
    Compact live AI judge over already-filtered decision signals.
    Local rules decide candidates first; AI only confirms/downgrades the final short list.
    """
    import json as json_lib

    if not decision_signals:
        return decision_signals

    compact = []
    for row in decision_signals[:6]:
        mc = row.get("market_context") or {}
        pc = mc.get("price_change_pct") or {}
        vol = mc.get("volume_usd") or {}
        tx = mc.get("transactions") or {}

        compact.append({
            "symbol": row.get("symbol"),
            "direction": row.get("direction"),
            "verdict": row.get("verdict"),
            "action": row.get("action"),
            "timing": row.get("timing"),
            "confidence": row.get("confidence"),
            "strength": row.get("signal_strength"),
            "h1": pc.get("h1"),
            "h24": pc.get("h24"),
            "reserve": mc.get("reserve_usd"),
            "volume_h24": vol.get("h24"),
            "buy_sell_h1": tx.get("h1_buy_sell_ratio"),
            "red_flags": row.get("red_flags_list") or row.get("red_flags") or [],
        })

    system_instruction = (
        "You are PumpRadar AI Judge. Be conservative. "
        "Confirm only plausible pump/dump/risk signals. "
        "Return strict JSON only. No markdown."
    )

    prompt = (
        'Classify each crypto signal. Return JSON only with shape '
        '{"signals":[{"symbol":"...","ai_verdict_code":"confirm_pump_watch|confirm_dump_risk|confirm_distribution_risk|downgrade_to_noise|reject_thin_liquidity","ai_reason_short":"max 18 words"}]}. '
        "Rules: huge 24h pump WITH NEGATIVE 1h (reversal) = confirm_distribution_risk; huge 24h pump WITH POSITIVE 1h = confirm_pump_watch. "
        "clean moderate rise with liquidity = confirm_pump_watch. "
        "thin/dust/no volume = reject_thin_liquidity. "
        "weak contradictory signal = downgrade_to_noise. "
        "Signals: "
        + json_lib.dumps(compact, separators=(",", ":"), default=str)[:4500]
    )

    try:
        ai_result = await call_claude_haiku_json(
            system_instruction=system_instruction,
            user_prompt=prompt,
            temperature=0.1,
            max_tokens=120,
        )

        if not ai_result.get("ok") or not ai_result.get("json"):
            external_error_text = str(ai_result.get("error") or ai_result.get("status_code") or "external_ai_failed")[:300]

            # External AI failed. Try local Qwen before falling back to deterministic rules.
            local_result = await call_local_qwen_json(
                system_instruction=system_instruction,
                user_prompt=prompt,
                temperature=0.0,
                max_tokens=80,
            )

            if local_result.get("ok") and local_result.get("json"):
                ai_result = local_result
            else:
                local_error_text = str(local_result.get("error") or local_result.get("status_code") or "local_ai_failed")[:300]
                for row in decision_signals:
                    row["ai_live_used"] = False
                    row["ai_fallback_used"] = True
                    row["ai_provider"] = "local_rules"
                    row["ai_model"] = None
                    row["ai_error"] = f"external={external_error_text}; local={local_error_text}"
                    row["ai_verdict_code"] = row.get("ai_judge_code")
                    row["ai_reason_short"] = "Local deterministic fallback used because external and local AI failed."
                return decision_signals

        parsed = ai_result.get("json") or {}
        ai_rows = parsed.get("signals") or []
        ai_map = {
            str(item.get("symbol") or "").upper(): item
            for item in ai_rows
            if item.get("symbol")
        }

        for row in decision_signals:
            symbol = str(row.get("symbol") or "").upper()
            ai = ai_map.get(symbol) or {}

            row["ai_live_used"] = bool(ai)
            row["ai_fallback_used"] = not bool(ai)
            row["ai_provider"] = ai_result.get("provider")
            row["ai_model"] = ai_result.get("model")
            row["ai_verdict_code"] = ai.get("ai_verdict_code") or row.get("ai_judge_code")
            row["ai_reason_short"] = ai.get("ai_reason_short") or "Live AI did not return a specific reason for this symbol."

            # AI can downgrade/reject, but cannot turn unsafe/avoid into buy.
            code = str(row.get("ai_verdict_code") or "").lower()
            whale = row.get("whale_activity") or {}
            whale_score = int(whale.get("whale_score") or 0)
            whale_accum = bool(whale.get("accumulation_detected"))
            whale_dump = bool(whale.get("dump_risk"))

            if code in {"downgrade_to_noise", "reject_thin_liquidity"}:
                row["verdict"] = "Noise / Rejected by AI" if code == "downgrade_to_noise" else "Rejected Thin Liquidity"
                row["final_verdict"] = row["verdict"]
                row["action"] = "avoid"
                row["direction"] = "risk"
                row["signal_type"] = "risk"
                row["timing"] = "avoid"
                row["risk_level"] = "high"
            elif code == "confirm_whale_dump" or whale_dump:
                row["verdict"] = "DUMP IMMINENT"
                row["final_verdict"] = "DUMP IMMINENT"
                row["action"] = "avoid"
                row["signal_type"] = "dump"
                row["direction"] = "dump"
                row["timing"] = "urgent"
                row["risk_level"] = "high"
            elif code == "confirm_whale_accumulation" or (whale_accum and whale_score >= 60):
                row["verdict"] = "PUMP IMMINENT"
                row["final_verdict"] = "PUMP IMMINENT"
                row["action"] = "watch"
                row["signal_type"] = "pump"
                row["direction"] = "pump"
                row["timing"] = "urgent"
                row["risk_level"] = "medium"
            elif code == "confirm_pump_watch":
                sources = (row.get("source_stack") or {})
                telegram_active = bool(sources.get("telegram") and len(sources.get("telegram", [])) > 0)
                if telegram_active and whale_score >= 30:
                    row["verdict"] = "PUMP IMMINENT"
                    row["final_verdict"] = "PUMP IMMINENT"
                    row["signal_type"] = "pump"
                    row["direction"] = "pump"
                    row["timing"] = "urgent"
                else:
                    row["verdict"] = "WATCH THIS — Pump Signal"
                    row["final_verdict"] = "WATCH THIS — Pump Signal"
                    row["signal_type"] = "pump"
                    row["direction"] = "pump"
                    row["timing"] = "watch"
            elif code in {"confirm_dump_risk", "confirm_distribution_risk"}:
                row["verdict"] = "DUMP IMMINENT" if code == "confirm_dump_risk" else "WATCH THIS — Distribution Risk"
                row["final_verdict"] = "DUMP IMMINENT" if code == "confirm_dump_risk" else "WATCH THIS"
                row["signal_type"] = "dump" if code == "confirm_dump_risk" else "risk"
                row["direction"] = "dump" if code == "confirm_dump_risk" else "risk"
                row["timing"] = "urgent" if code == "confirm_dump_risk" else "watch"

        return decision_signals

    except Exception as exc:
        for row in decision_signals:
            row["ai_live_used"] = False
            row["ai_fallback_used"] = True
            row["ai_provider"] = "openrouter"
            row["ai_error"] = str(exc)[:300]
            row["ai_verdict_code"] = row.get("ai_judge_code")
            row["ai_reason_short"] = "Local decision fallback used because AI judge raised an exception."
        return decision_signals




async def call_claude_haiku_json(
    *,
    system_instruction: str,
    user_prompt: str,
    temperature: float = 0.0,
    max_tokens: int = 80,
) -> dict:
    """Call Claude Haiku API and parse JSON response."""
    import json as json_lib
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return {"ok": False, "provider": "claude_haiku", "error": "no_api_key", "json": None, "text": ""}
    try:
        def _request():
            return requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "system": system_instruction or "",
                    "messages": [{"role": "user", "content": user_prompt or ""}],
                },
                timeout=30,
            )
        resp = await asyncio.to_thread(_request)
        if resp.status_code >= 400:
            return {"ok": False, "provider": "claude_haiku", "error": resp.text[:200], "json": None, "text": ""}
        data = resp.json() or {}
        text = ((data.get("content") or [{}])[0].get("text") or "").strip()
        cleaned = text
        if cleaned.startswith("```"):
            cleaned = chr(10).join(line for line in cleaned.splitlines() if not line.strip().startswith("```")).strip()
        parsed = json_lib.loads(cleaned)
        return {"ok": True, "provider": "claude_haiku", "model": "claude-haiku-4-5-20251001", "json": parsed, "text": text}
    except Exception as exc:
        return {"ok": False, "provider": "claude_haiku", "error": str(exc), "json": None, "text": ""}


async def call_claude_haiku_text(
    *,
    system_instruction: str,
    user_prompt: str,
    temperature: float = 0.3,
    max_tokens: int = 900,
) -> dict:
    """Call Claude Haiku API and return plain text (chat replies)."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return {"ok": False, "provider": "claude_haiku", "error": "no_api_key", "text": ""}
    try:
        def _request():
            return requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "system": system_instruction or "",
                    "messages": [{"role": "user", "content": user_prompt or ""}],
                },
                timeout=45,
            )
        resp = await asyncio.to_thread(_request)
        if resp.status_code >= 400:
            return {"ok": False, "provider": "claude_haiku", "status_code": resp.status_code, "error": resp.text[:300], "text": ""}
        data = resp.json() or {}
        text = ((data.get("content") or [{}])[0].get("text") or "").strip()
        return {"ok": bool(text), "provider": "claude_haiku", "model": "claude-haiku-4-5-20251001", "text": text}
    except Exception as exc:
        return {"ok": False, "provider": "claude_haiku", "error": str(exc), "text": ""}


async def apply_local_qwen_to_decision_signals(decision_signals: List[dict]) -> List[dict]:
    """
    Local-first AI judge using Qwen/llama.cpp.
    One signal per request is more reliable than batch JSON for small local models.
    """
    if not decision_signals:
        return decision_signals

    allowed_codes = {
        "confirm_pump_watch",
        "confirm_dump_risk",
        "confirm_distribution_risk",
        "confirm_whale_accumulation",
        "confirm_whale_dump",
        "downgrade_to_noise",
        "reject_thin_liquidity",
    }

    system_instruction = (
        "You are PumpRadar AI Judge. Return JSON only. No markdown. "
        "Choose code only from: confirm_pump_watch, confirm_dump_risk, "
        "confirm_distribution_risk, confirm_whale_accumulation, confirm_whale_dump, "
        "downgrade_to_noise, reject_thin_liquidity. "
        "If whale_score>=60 and whale_accumulation=True use confirm_whale_accumulation. "
        "If whale_dump_risk=True use confirm_whale_dump."
    )

    async def _process_signal(row: dict) -> tuple:
        mc = row.get("market_context") or {}
        pc = mc.get("price_change_pct") or {}
        vol = mc.get("volume_usd") or {}
        tx = mc.get("transactions") or {}
        flags = row.get("red_flags_list") or row.get("red_flags") or []
        prompt = (
            "Classify this PumpRadar signal. "
            "Return exactly JSON: {\"code\":\"...\",\"reason\":\"max 12 words\"}. "
            "Codes: confirm_pump_watch, confirm_dump_risk, confirm_distribution_risk, "
            "downgrade_to_noise, reject_thin_liquidity. "
            f"Signal: symbol={row.get('symbol')}, direction={row.get('direction')}, "
            f"h1={((row.get('market_context') or {}).get('price_change_pct') or {}).get('h1')}, "
            f"h24={((row.get('market_context') or {}).get('price_change_pct') or {}).get('h24')}, "
            f"flags={row.get('red_flags_list') or row.get('red_flags') or []}."
        )
        result = await call_claude_haiku_json(
            system_instruction=system_instruction,
            user_prompt=prompt,
            temperature=0.0,
            max_tokens=50,
        )
        if not result.get("ok") or not result.get("json"):
            result = await call_local_qwen_json(
                system_instruction=system_instruction,
                user_prompt=prompt,
                temperature=0.0,
                max_tokens=50,
            )
        return row, result

    qwen_results = await asyncio.gather(*[_process_signal(row) for row in decision_signals], return_exceptions=True)
    for item in qwen_results:
        if isinstance(item, Exception):
            continue
        row, result = item
        mc = row.get("market_context") or {}
        pc = mc.get("price_change_pct") or {}
        vol = mc.get("volume_usd") or {}
        tx = mc.get("transactions") or {}
        flags = row.get("red_flags_list") or row.get("red_flags") or []

        prompt = (
            "Classify this PumpRadar signal. "
            "Return exactly JSON: {\"code\":\"...\",\"reason\":\"max 12 words\"}. "
            "Codes: confirm_pump_watch, confirm_dump_risk, confirm_distribution_risk, "
            "downgrade_to_noise, reject_thin_liquidity. "
            "Rules: huge 24h pump WITH NEGATIVE 1h (reversal/dump) = confirm_distribution_risk; huge 24h pump WITH POSITIVE 1h (still rising) = confirm_pump_watch; "
            "moderate rise with liquidity and buy pressure = confirm_pump_watch; "
            "clear short-term fall with volume = confirm_dump_risk; "
            "thin/dust/new pool = reject_thin_liquidity; contradictory weak signal = downgrade_to_noise. "
            f"Signal: symbol={row.get('symbol')}, local_verdict={row.get('verdict')}, "
            f"direction={row.get('direction')}, action={row.get('action')}, "
            f"h1={pc.get('h1')}, h24={pc.get('h24')}, "
            f"reserve={mc.get('reserve_usd')}, volume_h24={vol.get('h24')}, "
            f"buy_sell_h1={tx.get('h1_buy_sell_ratio')}, flags={flags}, "
            f"whale_score={(row.get('whale_activity') or {}).get('whale_score', 0)}, "
            f"whale_accumulation={(row.get('whale_activity') or {}).get('accumulation_detected', False)}, "
            f"whale_dump_risk={(row.get('whale_activity') or {}).get('dump_risk', False)}."
        )

        # Try Claude Haiku first, fallback to Qwen
        result = await call_claude_haiku_json(
            system_instruction=system_instruction,
            user_prompt=prompt,
            temperature=0.0,
            max_tokens=50,
        )
        if not result.get("ok") or not result.get("json"):
            result = await call_local_qwen_json(
                system_instruction=system_instruction,
                user_prompt=prompt,
                temperature=0.0,
                max_tokens=50,
            )

        if not result.get("ok") or not result.get("json"):
            row["ai_live_used"] = False
            row["ai_fallback_used"] = True
            row["ai_provider"] = "local_rules"
            row["ai_model"] = None
            row["ai_error"] = str(result.get("error") or result.get("status_code") or "ai_failed")[:300]
            row["ai_verdict_code"] = row.get("ai_judge_code")
            row["ai_reason_short"] = "Local deterministic fallback used because AI failed."
            continue

        parsed = result.get("json") or {}
        code = str(parsed.get("code") or parsed.get("ai_verdict_code") or "").strip()
        reason = str(parsed.get("reason") or parsed.get("ai_reason_short") or "").strip()

        if code not in allowed_codes:
            code = row.get("ai_judge_code") or "downgrade_to_noise"
            reason = reason or "Local Qwen returned an unknown code; fallback code kept."

        row["ai_live_used"] = True
        row["ai_fallback_used"] = False
        row["ai_provider"] = result.get("provider") or "local_qwen"
        row["ai_model"] = result.get("model") or "qwen2.5-1.5b-instruct-q4_k_m"
        row["ai_error"] = None
        row["ai_verdict_code"] = code

        # Small local models are useful for compact verdict codes, but their free-text
        # explanation can be generic. Use factual market evidence instead.
        row["ai_reason_short"] = (
            f"AI confirmed {code}; evidence: "
            f"{row.get('why_now') or row.get('reason') or row.get('trigger') or 'market/DEX context'}"
        )[:280]

        # Qwen can downgrade or reject, but cannot upgrade unsafe signals into buys.
        # If AI confirms a pump but holder concentration is high, keep it as watch,
        # but mark it explicitly high-risk instead of a normal early pump.
        flag_text_after_ai = " ".join(str(x).lower() for x in (row.get("red_flags_list") or row.get("red_flags") or []))
        if code == "confirm_pump_watch" and "top_holder_high_concentration" in flag_text_after_ai:
            row["verdict"] = "High-Risk Pump Watch"
            row["final_verdict"] = row["verdict"]
            row["action"] = "monitor_high_risk"
            row["risk_level"] = "high"
            row["signal_quality"] = "Suspect"

        whale = row.get("whale_activity") or {}
        whale_score = int(whale.get("whale_score") or 0)
        whale_accum = bool(whale.get("accumulation_detected"))
        whale_dump = bool(whale.get("dump_risk"))

        if code == "downgrade_to_noise":
            row["verdict"] = "Noise / Downgraded by Local AI"
            row["final_verdict"] = row["verdict"]
            row["action"] = "avoid"
            row["direction"] = "risk"
            row["signal_type"] = "risk"
            row["timing"] = "avoid"
            row["risk_level"] = "high"
        elif code == "reject_thin_liquidity":
            row["verdict"] = "Rejected Thin Liquidity"
            row["final_verdict"] = row["verdict"]
            row["action"] = "avoid"
            row["direction"] = "risk"
            row["signal_type"] = "risk"
            row["timing"] = "avoid"
            row["risk_level"] = "high"
        elif code == "confirm_whale_dump" or whale_dump:
            row["verdict"] = "DUMP IMMINENT"
            row["final_verdict"] = "DUMP IMMINENT"
            row["action"] = "avoid"
            row["signal_type"] = "dump"
            row["direction"] = "dump"
            row["timing"] = "urgent"
            row["risk_level"] = "high"
        elif code == "confirm_whale_accumulation" or (whale_accum and whale_score >= 60):
            row["verdict"] = "PUMP IMMINENT"
            row["final_verdict"] = "PUMP IMMINENT"
            row["action"] = "watch"
            row["signal_type"] = "pump"
            row["direction"] = "pump"
            row["timing"] = "urgent"
            row["risk_level"] = "medium"
        elif code == "confirm_pump_watch":
            sources = (row.get("source_stack") or {})
            telegram_active = bool(sources.get("telegram") and len(sources.get("telegram", [])) > 0)
            if telegram_active and whale_score >= 30:
                row["verdict"] = "PUMP IMMINENT"
                row["final_verdict"] = "PUMP IMMINENT"
                row["signal_type"] = "pump"
                row["direction"] = "pump"
                row["timing"] = "urgent"
            else:
                row["verdict"] = "WATCH THIS — Pump Signal"
                row["final_verdict"] = "WATCH THIS — Pump Signal"
                row["signal_type"] = "pump"
                row["direction"] = "pump"
                row["timing"] = "watch"
        elif code in {"confirm_dump_risk", "confirm_distribution_risk"}:
            row["verdict"] = "DUMP IMMINENT" if code == "confirm_dump_risk" else "WATCH THIS — Distribution Risk"
            row["final_verdict"] = "DUMP IMMINENT" if code == "confirm_dump_risk" else "WATCH THIS — Distribution Risk"
            row["signal_type"] = "dump" if code == "confirm_dump_risk" else "risk"
            row["direction"] = "dump" if code == "confirm_dump_risk" else "risk"
            row["timing"] = "urgent" if code == "confirm_dump_risk" else "watch"

    # Remove entries rejected/downgraded by local Qwen from the final decision table.
    # They can later be exposed in an audit/rejected endpoint, but should not appear as live signals.
    decision_signals = [
        row for row in decision_signals
        if str(row.get("ai_verdict_code") or "").lower() not in {"downgrade_to_noise", "reject_thin_liquidity"}
    ]

    return decision_signals



def build_decision_signals(snapshot: Optional[dict], limit: int = 12) -> List[dict]:
    """
    Final PumpRadar decision layer.
    This is the single backend contract for the dashboard decision table.
    Sources are fused here, but the output is one normalized verdict list.
    """
    if not snapshot:
        return []

    rows: List[dict] = []
    experimental = snapshot.get("experimental_signals_v2") or {}

    # Use both final actionable items and raw GeckoTerminal v2 items.
    # Actionable alone is too narrow and can miss clean market-led pump watches.
    gecko_items: List[dict] = []
    seen_symbols = set()

    for source_key in ("actionable_geckoterminal", "geckoterminal"):
        for item in list(experimental.get(source_key) or []):
            symbol_key = str(item.get("symbol") or item.get("base_symbol") or "").upper()
            if not symbol_key or symbol_key in seen_symbols:
                continue
            seen_symbols.add(symbol_key)
            item["_decision_source_key"] = source_key
            gecko_items.append(item)

    for item in gecko_items[:40]:
        symbol = (item.get("symbol") or item.get("base_symbol") or "UNKNOWN").upper()
        market_context = item.get("market_context") or {}
        price_change_pct = market_context.get("price_change_pct") or {}
        volume_usd = market_context.get("volume_usd") or {}
        transactions = market_context.get("transactions") or {}

        red_flags = item.get("red_flags") or []
        red_flags_list = [str(x) for x in red_flags] if isinstance(red_flags, list) else [str(red_flags)]
        flag_text = " ".join(red_flags_list).lower()

        direction_raw = (item.get("direction") or item.get("direction_hint") or "pump").lower()
        pump_score = _to_float_safe(item.get("pump_coordination_score"))
        dump_score = _to_float_safe(item.get("dump_distribution_score"))
        noise_score = _to_float_safe(item.get("noise_score"))
        risk_score = _to_float_safe(item.get("manipulation_setup_score"))
        confidence = _to_float_safe(item.get("confidence"))
        reserve_usd = _to_float_safe(market_context.get("reserve_usd") or ((item.get("tradeability") or {}).get("reserve_usd")))
        volume_h1 = _to_float_safe(volume_usd.get("h1"))
        volume_h24 = _to_float_safe(volume_usd.get("h24") or ((item.get("tradeability") or {}).get("volume_h24")))
        change_5m = _to_float_safe(price_change_pct.get("m5"))
        change_15m = _to_float_safe(price_change_pct.get("m15"))
        change_1h = _to_float_safe(price_change_pct.get("h1"))
        change_24h = _to_float_safe(price_change_pct.get("h24"))
        buy_sell_h1 = _to_float_safe(transactions.get("h1_buy_sell_ratio"))

        hard_thin = any(x in flag_text for x in [
            "very_thin_solana_liquidity",
            "very_thin_liquidity",
            "new_pool_low_liquidity",
            "thin_solana_liquidity",
            "thin_liquidity",
        ])

        # Invalid, dust, or very thin pools must not reach the final decision layer.
        if reserve_usd <= 0 or volume_h24 <= 0:
            continue

        if hard_thin and (reserve_usd < 10000 or volume_h24 < 10000):
            continue

        top_holder_high = "top_holder_high_concentration" in flag_text
        new_pool = "solana_new_pool" in flag_text or "pumpfun_new_launch_context" in flag_text

        verdict = "Noise"
        action = "exclude"
        direction = "noise"
        timing = item.get("timing") or "watch"
        quality = "Suspect"
        ai_judge_code = "noise_excluded"
        include = False

        # Late vertical moves are not clean dumps; they are distribution/exit-liquidity risk.
        if change_24h >= 300 and (change_1h < 0 or change_15m < 0 or dump_score >= pump_score or top_holder_high):
            verdict = "Late Pump / Distribution Risk"
            action = "avoid_chasing"
            direction = "risk"
            timing = "late"
            ai_judge_code = "avoid_distribution_late"
            include = True

        # Real breakdowns can be risk signals, but thin liquidity should be marked as avoid.
        elif direction_raw == "dump" and (change_1h <= -3 or change_24h <= -20):
            direction = "risk"
            timing = "watch"
            if hard_thin or reserve_usd < 10000:
                verdict = "Dump Risk / Thin Liquidity"
                action = "avoid"
                ai_judge_code = "avoid_dump_thin"
            else:
                verdict = "Dump Risk"
                action = "monitor"
                ai_judge_code = "dump_risk_monitor"
            include = True

        # Early pump watch: not a buy signal, only a monitored setup.
        elif (
            direction_raw == "pump"
            and not hard_thin
            and reserve_usd >= 50000
            and volume_h24 >= 25000
            and buy_sell_h1 >= 1.25
            and 10 <= change_24h <= 150
            and pump_score >= 35
            and dump_score <= 20
            and noise_score <= 50
        ):
            verdict = "Early Pump Watch"
            action = "monitor"
            direction = "pump"
            timing = "developing"
            quality = "Suspect" if new_pool or "holders_not_connected" in flag_text or "dex_only_signal" in flag_text else "Clean"
            ai_judge_code = "early_pump_watch_liquidity_ok"
            include = True

        # Market-led pump watch: allows clean market structure even when the older
        # pump_score is under-calibrated, but blocks thin/new-pool junk.
        elif (
            direction_raw == "pump"
            and not hard_thin
            and reserve_usd >= 100000
            and volume_h24 >= 500000
            and 3 <= change_1h <= 20
            and 8 <= change_24h <= 80
            and buy_sell_h1 >= 1.15
            and dump_score <= 25
            and noise_score <= 40
            and "top_holder_high_concentration" not in flag_text
        ):
            verdict = "Market-led Pump Watch"
            action = "monitor"
            direction = "pump"
            timing = "developing"
            quality = "Suspect" if "dex_only_signal" in flag_text or "pumpfun_related" in flag_text else "Clean"
            ai_judge_code = "market_led_pump_watch"
            include = True

        if not include:
            continue

        # Do not promote very weak risk candidates to the final decision table.
        # Keep true distribution/late-pump risks, but reject low-confidence generic dips.
        if direction == "risk":
            effective_strength = max(confidence, dump_score, risk_score)
            is_distribution_late = ai_judge_code == "avoid_distribution_late"
            has_holder_concentration = "top_holder_high_concentration" in flag_text or "top_holder_elevated_concentration" in flag_text
            if effective_strength < 35 and not is_distribution_late and not has_holder_concentration:
                continue

        reason_parts = []
        if change_24h:
            reason_parts.append(f"24h {change_24h:+.2f}%")
        if change_1h:
            reason_parts.append(f"1h {change_1h:+.2f}%")
        if reserve_usd:
            reason_parts.append(f"reserve ${reserve_usd:,.0f}")
        if volume_h24:
            reason_parts.append(f"24h volume ${volume_h24:,.0f}")
        if buy_sell_h1:
            reason_parts.append(f"B/S h1 {buy_sell_h1:.2f}")
        if red_flags_list:
            reason_parts.append("flags: " + ", ".join(red_flags_list[:4]))

        rows.append({
            "symbol": symbol,
            "name": item.get("name") or item.get("pool_name") or symbol,
            "chain": item.get("chain") or item.get("network"),
            "contract_address": item.get("contract_or_mint") or item.get("token_address"),
            "pool_address": item.get("pool_address"),
            "pool_url": item.get("pool_url"),
            "dex": item.get("dex"),
            "direction": direction,
            "signal_type": direction,
            "verdict": verdict,
            "final_verdict": verdict,
            "action": action,
            "timing": timing,
            "signal_quality": quality,
            "confidence": int(max(0, min(100, confidence or max(pump_score, dump_score, risk_score)))),
            "signal_strength": int(max(0, min(100, pump_score if direction == "pump" else max(dump_score, risk_score, confidence)))),
            "risk_level": "high" if action.startswith("avoid") or "Risk" in verdict else "medium",
            "trigger": item.get("trigger"),
            "why_now": "; ".join(reason_parts) or item.get("trigger") or "Decision signal generated from DEX context.",
            "reason": "; ".join(reason_parts) or item.get("trigger") or "",
            "red_flags": ", ".join(red_flags_list),
            "red_flags_list": red_flags_list,
            "source_stack": item.get("source_stack") or item.get("source_stack_hint") or {"dex": ["GeckoTerminal"]},
            "tradeability": item.get("tradeability"),
            "market_context": market_context,
            "ai_source": "Signal Schema v2 decision layer",
            "ai_judge_code": ai_judge_code,
            "source": "geckoterminal_decision",
        })

    # Add CoinGecko pump/dump signals into decision layer
    cg_seen = {row.get("symbol") for row in rows}
    for sig in list(snapshot.get("pump_signals", []) or []) + list(snapshot.get("dump_signals", []) or []):
        symbol = (sig.get("symbol") or "").upper()
        if not symbol or symbol in cg_seen:
            continue
        cg_seen.add(symbol)
        sig_type = sig.get("signal_type", "pump")
        confidence_str = sig.get("confidence", "medium")
        confidence_val = 75 if confidence_str == "high" else 55 if confidence_str == "medium" else 35
        whale = sig.get("whale_activity") or {}
        whale_score = int(whale.get("whale_score") or 0)
        whale_accum = bool(whale.get("accumulation_detected"))
        whale_dump = bool(whale.get("dump_risk"))
        if whale_accum and whale_score >= 60:
            verdict = "Whale Accumulation Pump Watch"
            action = "watch"
            ai_judge_code = "confirm_whale_accumulation"
        elif whale_dump:
            verdict = "Whale Dump Risk"
            action = "avoid"
            ai_judge_code = "confirm_whale_dump"
        elif sig_type == "pump":
            verdict = sig.get("signal_v2", {}).get("verdict") or "Pump Watch"
            action = "watch"
            ai_judge_code = "confirm_pump_watch"
        else:
            verdict = "Dump Risk"
            action = "avoid"
            ai_judge_code = "confirm_dump_risk"
        rows.append({
            "symbol": symbol,
            "name": sig.get("name", symbol),
            "direction": sig_type,
            "signal_type": sig_type,
            "verdict": verdict,
            "action": action,
            "signal_strength": int(sig.get("signal_strength") or confidence_val),
            "confidence": confidence_str,
            "signal_quality": "Clean" if confidence_val >= 70 else "Medium" if confidence_val >= 50 else "Suspect",
            "timing": "watch",
            "risk_level": "high" if whale_dump else "medium",
            "trigger": sig.get("reason") or sig.get("technical_factors") or "",
            "why_now": sig.get("reason") or "",
            "reason": sig.get("reason") or "",
            "red_flags": "",
            "red_flags_list": [],
            "source_stack": {"market": ["CoinGecko"], "dex": [], "social": [], "safety": [], "holders": []},
            "tradeability": sig.get("preferred_venue"),
            "market_context": {
                "price_usd": sig.get("price"),
                "price_change_pct": {"h1": sig.get("price_change_1h"), "h24": sig.get("price_change_24h")},
                "volume_usd": {"h24": sig.get("volume_24h")},
                "market_cap_usd": sig.get("market_cap"),
            },
            "whale_activity": whale,
            "ai_source": "CoinGecko + Whale Layer",
            "ai_judge_code": ai_judge_code,
            "source": "coingecko_decision",
        })

    # Reddit RSS signals into decision layer
    reddit_signals = snapshot.get("reddit_rss_signals") or []
    reddit_seen = {row.get("symbol") for row in rows}
    for sig in reddit_signals:
        symbol = (sig.get("symbol") or "").upper()
        if not symbol or symbol in reddit_seen:
            continue
        reddit_seen.add(symbol)
        direction = sig.get("direction", "pump")
        is_rug = sig.get("is_rug", False)
        rows.append({
            "symbol": symbol,
            "name": symbol,
            "direction": direction,
            "signal_type": direction,
            "verdict": "Rug Warning" if is_rug else ("Pump Watch" if direction == "pump" else "Dump Risk"),
            "action": "avoid" if is_rug else "watch",
            "signal_strength": 35,
            "confidence": "low",
            "signal_quality": "Suspect",
            "timing": "watch",
            "risk_level": "high" if is_rug else "medium",
            "trigger": sig.get("title", ""),
            "why_now": f"Reddit r/{sig.get('subreddit','')}: {sig.get('title','')[:80]}",
            "reason": sig.get("title", ""),
            "red_flags": "rug_warning" if is_rug else "",
            "red_flags_list": ["rug_warning"] if is_rug else [],
            "source_stack": {"social": ["Reddit RSS"], "market": [], "dex": [], "safety": [], "holders": []},
            "tradeability": None,
            "market_context": {},
            "whale_activity": {"whale_score": 0, "accumulation_detected": False, "dump_risk": False},
            "ai_source": "Reddit RSS",
            "ai_judge_code": "reddit_signal",
            "source": "reddit_decision",
        })

    def sort_key(row: dict):
        direction_rank = 0 if row.get("direction") == "pump" else 1
        action_penalty = 50 if str(row.get("action")).startswith("avoid") else 0
        whale_bonus = -int((row.get("whale_activity") or {}).get("whale_score") or 0)
        return (direction_rank, action_penalty, whale_bonus, -float(row.get("signal_strength") or 0))

    rows.sort(key=sort_key)
    return rows[:limit]



@app.get("/api/crypto/signals")
async def get_signals(user=Depends(get_optional_user)):
    """Get latest pump/dump signals"""
    # Get latest snapshot
    snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
    
    if not snapshot:
        # Return empty if no data yet
        return api_ok({
            "pump_signals": [],
            "dump_signals": [],
            "telegram_early_signals": [],
            "telegram_pipeline_audit": {"window_hours": TELEGRAM_EARLY_SIGNAL_HOURS, "early_signal_count": 0, "promotion_count": 0, "promoted_symbols": [], "stage_counts": {}, "records": []},
            "telegram_calibration_summary": None,
            "new_algorithm_signals": [],
            "decision_signals": [],
            "market_summary": "Signals are being processed. Please check back in a few minutes.",
            "last_updated": None,
            "coins_analyzed": 0,
        })
    
    # Check subscription for full access
    has_access = True  # Default to true for unauthenticated users (limited view)
    subscription_block_reason = None
    
    if user:
        sub = user.get("subscription", "free")
        sub_expiry = user.get("subscription_expiry")
        
        if sub in ("monthly", "annual"):
            has_access = True
        elif sub == "trial":
            if sub_expiry:
                # Handle timezone
                if isinstance(sub_expiry, str):
                    sub_expiry = datetime.fromisoformat(sub_expiry.replace("Z", "+00:00"))
                if hasattr(sub_expiry, 'tzinfo') and sub_expiry.tzinfo is None:
                    sub_expiry = sub_expiry.replace(tzinfo=timezone.utc)
                
                if datetime.now(timezone.utc) < sub_expiry:
                    has_access = True
                else:
                    has_access = False
                    subscription_block_reason = "trial_expired"
            else:
                has_access = False
                subscription_block_reason = "trial_expired"
        else:
            has_access = False
            subscription_block_reason = "subscription_required"
    
    if subscription_block_reason and user:
        if subscription_block_reason == "trial_expired":
            raise HTTPException(
                status_code=402,
                detail=api_err("Your free trial has expired. Please subscribe to continue.", "SUBSCRIPTION_EXPIRED")
            )
        raise HTTPException(
            status_code=402,
            detail=api_err("Start your free 7-day trial to unlock the full PumpRadar signal feed.", "SUBSCRIPTION_REQUIRED")
        )
    
    snapshot_ts = snapshot.get("timestamp")
    snapshot_key = snapshot_ts.isoformat() if isinstance(snapshot_ts, datetime) else str(snapshot_ts)
    cache_key = f"{snapshot_key}::{int(has_access)}"
    cached_payload = get_memory_cache(DASHBOARD_PAYLOAD_CACHE, cache_key, ttl_seconds=300)
    if cached_payload is not None:
        return api_ok({
            **cached_payload,
            "scan_status": build_signal_scan_status(),
        })

    pump = [serialize_signal(dict(s)) for s in snapshot.get("pump_signals", [])]
    dump = [serialize_signal(dict(s)) for s in snapshot.get("dump_signals", [])]

    # Fallback: expose Signal Schema v2 GeckoTerminal actionable candidates to the main dashboard
    # when the legacy pump/dump arrays are empty. This keeps the UI unchanged and only maps
    # already-computed backend signals into the existing response contract.
    if not pump and not dump:
        experimental_v2 = snapshot.get("experimental_signals_v2") or {}
        actionable_gecko = experimental_v2.get("actionable_geckoterminal") or []

        mapped_pump = []
        mapped_dump = []

        for item in actionable_gecko[:12]:
            direction = (item.get("direction") or item.get("direction_hint") or "pump").lower()
            verdict = item.get("verdict") or ("Pump Watch" if direction == "pump" else "Dump Risk")
            confidence_value = item.get("confidence", 0)
            try:
                signal_strength = int(float(item.get("pump_coordination_score") if direction == "pump" else item.get("dump_distribution_score") or item.get("manipulation_setup_score") or item.get("dex_candidate_score") or confidence_value or 0))
            except Exception:
                signal_strength = 0

            raw_reason = item.get("why_now") or item.get("trigger") or item.get("reason") or "Actionable GeckoTerminal Signal Schema v2 candidate."
            if isinstance(raw_reason, list):
                reason_text = "; ".join(str(x) for x in raw_reason[:6])
            elif isinstance(raw_reason, dict):
                reason_text = "; ".join(f"{k}: {v}" for k, v in list(raw_reason.items())[:6])
            else:
                reason_text = str(raw_reason)

            raw_why_now = item.get("why_now") or item.get("trigger") or reason_text
            if isinstance(raw_why_now, list):
                why_now_text = "; ".join(str(x) for x in raw_why_now[:6])
            elif isinstance(raw_why_now, dict):
                why_now_text = "; ".join(f"{k}: {v}" for k, v in list(raw_why_now.items())[:6])
            else:
                why_now_text = str(raw_why_now)

            raw_red_flags = item.get("red_flags") or []
            if isinstance(raw_red_flags, list):
                red_flags_list = [str(x) for x in raw_red_flags[:8]]
                red_flags_text = ", ".join(red_flags_list)
            else:
                red_flags_text = str(raw_red_flags)
                red_flags_list = [red_flags_text] if red_flags_text else []

            market_context = item.get("market_context") or {}
            price_change_pct = market_context.get("price_change_pct") or {}
            transactions = market_context.get("transactions") or {}

            try:
                pump_score = float(item.get("pump_coordination_score") or 0)
            except Exception:
                pump_score = 0.0
            try:
                dump_score = float(item.get("dump_distribution_score") or 0)
            except Exception:
                dump_score = 0.0
            try:
                noise_score = float(item.get("noise_score") or 0)
            except Exception:
                noise_score = 0.0
            try:
                risk_score = float(item.get("manipulation_setup_score") or 0)
            except Exception:
                risk_score = 0.0
            try:
                change_24h = float(price_change_pct.get("h24") or 0)
            except Exception:
                change_24h = 0.0
            try:
                change_1h = float(price_change_pct.get("h1") or 0)
            except Exception:
                change_1h = 0.0
            try:
                change_15m = float(price_change_pct.get("m15") or 0)
            except Exception:
                change_15m = 0.0
            try:
                buy_sell_h1 = float(transactions.get("h1_buy_sell_ratio") or 0)
            except Exception:
                buy_sell_h1 = 0.0

            timing_raw = str(item.get("timing") or "").lower()
            flag_set = set(red_flags_list)

            ai_judge_code = "pass_watch"
            final_direction = direction
            final_verdict = verdict
            final_action = item.get("action") or "watch"
            final_timing = item.get("timing")

            hard_avoid_flags = {
                "very_thin_solana_liquidity",
                "very_thin_liquidity",
                "new_pool_low_liquidity",
            }

            if flag_set.intersection(hard_avoid_flags):
                ai_judge_code = "avoid_thin_or_new_pool"
                final_verdict = "Avoid / Thin Liquidity"
                final_action = "avoid"
                final_timing = final_timing or "compromised"
            elif "top_holder_high_concentration" in flag_set and change_24h > 300:
                ai_judge_code = "avoid_distribution_late"
                final_direction = "dump"
                final_verdict = "Distribution Risk"
                final_action = "avoid"
                final_timing = "late"
            elif direction == "pump" and dump_score >= pump_score:
                ai_judge_code = "avoid_distribution_late"
                final_direction = "dump"
                final_verdict = "Distribution Risk"
                final_action = "avoid"
                final_timing = "late"
            elif direction == "pump" and (change_24h > 300 and (change_1h <= 2 or change_15m < 0 or buy_sell_h1 < 1)):
                ai_judge_code = "avoid_distribution_late"
                final_direction = "dump"
                final_verdict = "Late Pump / Distribution Risk"
                final_action = "avoid"
                final_timing = "late"
            elif direction == "pump" and (noise_score >= 60 or timing_raw == "late"):
                ai_judge_code = "avoid_late_or_noisy_pump"
                final_verdict = "Avoid / Noisy Late Pump"
                final_action = "avoid"
                final_timing = "late"
            elif direction == "dump" and (noise_score >= 60 or "thin_liquidity" in flag_set or "thin_solana_liquidity" in flag_set):
                ai_judge_code = "avoid_dump_thin"
                final_verdict = "Dump Risk / Thin Liquidity"
                final_action = "avoid"
                final_timing = final_timing or "watch"

            # Main dashboard should not show avoid-classified Gecko items as pump candidates.
            if direction == "pump" and str(final_action).lower() == "avoid":
                direction = "risk"
            else:
                direction = final_direction

            verdict = final_verdict

            mapped = {
                "symbol": item.get("symbol") or item.get("base_symbol") or "UNKNOWN",
                "name": item.get("name") or item.get("pool_name") or item.get("symbol") or "Unknown",
                "chain": item.get("chain") or item.get("network"),
                "contract_address": item.get("contract_or_mint") or item.get("token_address"),
                "pool_address": item.get("pool_address"),
                "pool_url": item.get("pool_url"),
                "dex": item.get("dex"),
                "signal_type": direction,
                "direction": direction,
                "final_verdict": verdict,
                "verdict": verdict,
                "action": final_action,
                "signal_strength": max(0, min(100, signal_strength)),
                "confidence": item.get("confidence_label") or ("high" if float(confidence_value or 0) >= 70 else "medium"),
                "confidence_score": confidence_value,
                "risk_level": item.get("risk") or item.get("risk_level") or "high",
                "timeframe": item.get("timeframe") or final_timing or "watch now",
                "timing": final_timing,
                "trigger": str(item.get("trigger") or reason_text),
                "reason": reason_text,
                "why_now": why_now_text,
                "technical_factors": str(item.get("technical_factors") or "GeckoTerminal DEX activity, volume/liquidity anomaly, short-term market structure"),
                "red_flags": red_flags_text,
                "red_flags_list": red_flags_list,
                "source_stack": item.get("source_stack") or item.get("source_stack_hint") or {"dex": ["GeckoTerminal"]},
                "tradeability": item.get("tradeability"),
                "market_context": item.get("market_context"),
                "timestamp": snapshot_ts.isoformat() if isinstance(snapshot_ts, datetime) else snapshot_ts,
                "ai_source": "Signal Schema v2 local judge",
                "ai_judge_code": ai_judge_code,
            }

            if direction == "dump":
                mapped_dump.append(mapped)
            else:
                mapped_pump.append(mapped)

        if not pump:
            pump = mapped_pump
        if not dump:
            dump = mapped_dump
    def _dashboard_signal_quality_gate(sig: dict, side: str) -> bool:
        """Final dashboard gate: keep only decision-grade signals in the main table."""
        symbol = sig.get("symbol")
        try:
            strength = float(sig.get("signal_strength") or sig.get("confidence_score") or 0)
        except Exception:
            strength = 0.0

        confidence = str(sig.get("confidence") or "").lower()
        reason = str(sig.get("reason") or sig.get("trigger") or "").lower()
        verdict = str(sig.get("verdict") or sig.get("final_verdict") or "").lower()
        action = str(sig.get("action") or "").lower()
        ai_source = sig.get("ai_source")
        source_stack = sig.get("source_stack")

        weak_text = any(x in reason for x in [
            "single-source",
            "thin",
            "low conviction",
            "contradictory",
            "evidence is too thin",
        ])

        ai_judge_code = str(sig.get("ai_judge_code") or "").lower()
        red_flags_raw = sig.get("red_flags_list") or sig.get("red_flags") or []
        if isinstance(red_flags_raw, str):
            flag_text = red_flags_raw.lower()
        else:
            flag_text = " ".join(str(x).lower() for x in red_flags_raw)

        hard_thin_signal = any(x in flag_text for x in [
            "very_thin_solana_liquidity",
            "very_thin_liquidity",
            "new_pool_low_liquidity",
            "thin_solana_liquidity",
            "thin_liquidity",
        ])

        # Do not keep dust/new-pool thin liquidity as main dump/risk signal.
        if ai_judge_code in {"avoid_thin_or_new_pool", "avoid_dump_thin"} and hard_thin_signal:
            return False

        # AI/local-judge risk signals may remain in dump/risk side when they are tradeable enough.
        is_judged_risk = bool(ai_source) and side == "dump" and action == "avoid"
        if is_judged_risk:
            return True

        market_context = sig.get("market_context") or {}
        price_change_pct = market_context.get("price_change_pct") or {}
        volume_usd = market_context.get("volume_usd") or {}
        transactions = market_context.get("transactions") or {}
        red_flags_raw = sig.get("red_flags_list") or sig.get("red_flags") or []
        if isinstance(red_flags_raw, str):
            flag_text = red_flags_raw.lower()
        else:
            flag_text = " ".join(str(x).lower() for x in red_flags_raw)

        try:
            reserve_usd = float(market_context.get("reserve_usd") or 0)
        except Exception:
            reserve_usd = 0.0
        try:
            volume_h1 = float(volume_usd.get("h1") or 0)
        except Exception:
            volume_h1 = 0.0
        try:
            volume_h24 = float(volume_usd.get("h24") or sig.get("volume_24h") or 0)
        except Exception:
            volume_h24 = 0.0
        try:
            change_24h = float(price_change_pct.get("h24") or 0)
        except Exception:
            change_24h = 0.0
        try:
            buy_sell_h1 = float(transactions.get("h1_buy_sell_ratio") or 0)
        except Exception:
            buy_sell_h1 = 0.0

        hard_thin = any(x in flag_text for x in [
            "very_thin_solana_liquidity",
            "very_thin_liquidity",
            "new_pool_low_liquidity",
            "thin_solana_liquidity",
            "thin_liquidity",
        ])

        # Allow early pump watch with decent liquidity even if score is still below 50.
        # This keeps SAOS-style early setups visible, but rejects JEWLON/DIP-style thin launches.
        early_pump_watch = (
            side == "pump"
            and action not in ("avoid", "reject")
            and not hard_thin
            and reserve_usd >= 50000
            and volume_h24 >= 25000
            and buy_sell_h1 >= 1.25
            and 10 <= change_24h <= 150
            and strength >= 35
        )
        if early_pump_watch:
            sig["verdict"] = sig.get("verdict") or sig.get("final_verdict") or "Early Pump Watch"
            sig["final_verdict"] = sig.get("final_verdict") or sig.get("verdict") or "Early Pump Watch"
            sig["action"] = sig.get("action") or "monitor"
            sig["timing"] = sig.get("timing") or "developing"
            sig["ai_judge_code"] = sig.get("ai_judge_code") or "early_pump_watch_liquidity_ok"
            return True

        # Never promote weak/low-confidence legacy signals as main decisions.
        if confidence == "low":
            return False
        if strength < 50:
            return False
        if weak_text and not ai_source:
            return False
        if not ai_source and not source_stack and weak_text and (strength < 60 or confidence == "low"):
            return False

        # Avoid-classified items should not be presented as opportunities.
        if action == "avoid" and side == "pump":
            return False

        # Late/distribution belongs to risk board, not pump board.
        if side == "pump" and ("distribution" in verdict or "late" in verdict):
            return False

        return True

    pump = [x for x in pump if _dashboard_signal_quality_gate(x, "pump")]
    dump = [x for x in dump if _dashboard_signal_quality_gate(x, "dump")]

    # Inject final_verdict from decision_signals into pump/dump
    _decision_signals_local = decision_signals if "decision_signals" in dir() else []
    decision_by_symbol = {s.get("symbol", "").upper(): s for s in _decision_signals_local}
    for sig in pump + dump:
        sym = (sig.get("symbol") or "").upper()
        if sym in decision_by_symbol:
            sig["final_verdict"] = decision_by_symbol[sym].get("final_verdict", "")

    telegram_data = snapshot.get("telegram_consensus_precomputed")
    cross_platform_consensus = snapshot.get("cross_platform_consensus_precomputed")
    fresh_manipulation_alerts = snapshot.get("fresh_manipulation_alerts_precomputed")
    if has_access and telegram_data is None:
        telegram_payload = await telegram_consensus(hours=24, user=user)
        telegram_data = telegram_payload.get("data") if isinstance(telegram_payload, dict) else None
    elif not has_access:
        telegram_data = build_telegram_consensus_payload([], [], 24)
    if cross_platform_consensus is None:
        cross_platform_consensus = build_dashboard_cross_platform_consensus(snapshot, telegram_data)
    if fresh_manipulation_alerts is None:
        fresh_manipulation_alerts = build_intelligence_alerts(snapshot, telegram_data)
    telegram_calibration_summary = snapshot.get("telegram_calibration_summary")
    if telegram_calibration_summary is None and has_access:
        telegram_calibration_summary = await build_telegram_calibration_summary(72)
    new_algorithm_signals = await build_dashboard_new_algorithm_signals(snapshot)
    decision_signals = build_decision_signals(snapshot)
    cached_ai_doc = await db.qwen_decision_cache.find_one({"snapshot_key": snapshot_key})
    if cached_ai_doc and cached_ai_doc.get("items"):
        decision_signals = cached_ai_doc["items"]
    else:
        decision_signals = await apply_local_qwen_to_decision_signals(decision_signals)

    # Inject final_verdict din decision_signals in pump/dump
    decision_by_symbol = {s.get("symbol", "").upper(): s for s in decision_signals}
    for sig in pump + dump:
        sym = (sig.get("symbol") or "").upper()
        if sym in decision_by_symbol:
            sig["final_verdict"] = decision_by_symbol[sym].get("final_verdict", "")

        await db.qwen_decision_cache.replace_one(
            {"snapshot_key": snapshot_key},
            {"snapshot_key": snapshot_key, "items": decision_signals, "updated_at": datetime.now(timezone.utc)},
            upsert=True
        )
        await db.qwen_decision_cache.delete_many({"snapshot_key": {"$ne": snapshot_key}})
    AI_DECISION_SIGNALS_CACHE["latest"] = {
        "snapshot_key": snapshot_key,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "items": decision_signals,
    }
    
    payload = {
        "pump_signals": pump,
        "dump_signals": dump,
        "decision_signals": decision_signals,
        "telegram_early_signals": snapshot.get("telegram_early_signals", []),
        "telegram_pipeline_audit": snapshot.get("telegram_pipeline_audit", {"window_hours": TELEGRAM_EARLY_SIGNAL_HOURS, "early_signal_count": 0, "promotion_count": 0, "promoted_symbols": [], "stage_counts": {}, "records": []}),
        "telegram_calibration_summary": telegram_calibration_summary if has_access else None,
        "new_algorithm_signals": new_algorithm_signals,
        "market_summary": snapshot.get("market_summary", ""),
        "last_updated": snapshot_ts.isoformat() if isinstance(snapshot_ts, datetime) else snapshot_ts,
        "coins_analyzed": snapshot.get("coins_analyzed", 0),
        "has_full_access": has_access,
        "fear_greed": snapshot.get("fear_greed"),
        "trending": snapshot.get("trending", []),
        "telegram_consensus": telegram_data,
        "cross_platform_consensus": cross_platform_consensus,
        "fresh_manipulation_alerts": fresh_manipulation_alerts,
        "scan_status": build_signal_scan_status(),
    }

    return api_ok(set_memory_cache(DASHBOARD_PAYLOAD_CACHE, cache_key, payload))

@app.post("/api/admin/trigger-scan")
async def trigger_scan_manual(request: Request):
    """Manually trigger a signal scan — localhost only"""
    client_host = request.client.host if request.client else ""
    if client_host not in ("127.0.0.1", "::1", "localhost"):
        return api_err("Forbidden", "FORBIDDEN")
    if SIGNAL_SCAN_LOCK.locked():
        return api_err("Scan already running", "SCAN_RUNNING")
    asyncio.create_task(run_full_scan(db))
    return api_ok({"started": True, "message": "Scan triggered (v2)"})


@app.get("/api/crypto/dex-signals")
async def get_dex_signals(user=Depends(get_optional_user)):
    """Get GeckoTerminal DEX signals from latest snapshot"""
    snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
    if not snapshot:
        return api_ok({"dex_signals": [], "last_updated": None, "coins_analyzed": 0})
    experimental = snapshot.get("experimental_signals_v2") or {}
    actionable = experimental.get("actionable_geckoterminal") or []
    avoid = experimental.get("avoid_geckoterminal") or []
    summary = experimental.get("summary") or {}
    all_dex = actionable + avoid
    all_dex.sort(key=lambda x: float(x.get("confidence") or 0), reverse=True)
    return api_ok({
        "dex_signals": all_dex[:30],
        "summary": summary,
        "last_updated": snapshot.get("timestamp"),
        "coins_analyzed": snapshot.get("coins_analyzed", 0),
    })


@app.get("/api/intelligence/alerts")
async def intelligence_alerts(user=Depends(require_active_subscription)):
    snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
    if not snapshot:
        return api_ok({"alerts": [], "last_updated": None})
    alerts = snapshot.get("fresh_manipulation_alerts_precomputed")
    if alerts is None:
        telegram_payload = await telegram_consensus(hours=24, user=user)
        alerts = build_intelligence_alerts(snapshot, telegram_payload.get("data") if isinstance(telegram_payload, dict) else None)
    return api_ok({
        "alerts": alerts,
        "last_updated": serialize_datetime(snapshot.get("timestamp")) if snapshot else None,
    })

@app.get("/api/intelligence/cross-platform")
async def cross_platform_consensus(user=Depends(require_active_subscription)):
    snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
    if not snapshot:
        return api_ok({"cards": [], "last_updated": None})
    snapshot_ts = snapshot.get("timestamp")
    snapshot_key = snapshot_ts.isoformat() if isinstance(snapshot_ts, datetime) else str(snapshot_ts)
    cache_key = f"cross_platform::{snapshot_key}"
    cached_cards = get_memory_cache(CROSS_PLATFORM_CACHE, cache_key, ttl_seconds=180)
    if cached_cards is not None:
        return api_ok({
            "cards": cached_cards,
            "last_updated": serialize_datetime(snapshot.get("timestamp")),
        })
    cards = snapshot.get("cross_platform_consensus_precomputed")
    if cards is None:
        telegram_payload = await telegram_consensus(hours=24, user=user)
        cards = build_dashboard_cross_platform_consensus(snapshot, telegram_payload.get("data") if isinstance(telegram_payload, dict) else None)
    cards = set_memory_cache(CROSS_PLATFORM_CACHE, cache_key, cards)
    return api_ok({
        "cards": cards,
        "last_updated": serialize_datetime(snapshot.get("timestamp")) if snapshot else None,
    })

@app.get("/api/crypto/history")
async def get_history(limit: int = 24, user=Depends(require_active_subscription)):
    """Get historical signals (last N snapshots)"""
    snapshots = await db.signal_snapshots.find({}).sort("timestamp", -1).limit(limit).to_list(length=limit)
    
    result = []
    for snap in snapshots:
        result.append({
            "timestamp": snap["timestamp"].isoformat() if isinstance(snap.get("timestamp"), datetime) else snap.get("timestamp"),
            "pump_count": len(snap.get("pump_signals", [])),
            "dump_count": len(snap.get("dump_signals", [])),
            "market_summary": snap.get("market_summary", ""),
            "coins_analyzed": snap.get("coins_analyzed", 0),
        })
    
    return api_ok({"history": result})

@app.get("/api/crypto/snapshots")
async def get_snapshots(limit: int = 24, user=Depends(require_active_subscription)):
    """Get detailed signal snapshots for timeline view"""
    snapshots = await db.signal_snapshots.find({}).sort("timestamp", -1).limit(limit).to_list(length=limit)
    
    result = []
    for snap in snapshots:
        snap.pop("_id", None)
        if isinstance(snap.get("timestamp"), datetime):
            snap["timestamp"] = snap["timestamp"].isoformat()
        # Serialize signals
        for s in snap.get("pump_signals", []):
            if isinstance(s.get("timestamp"), datetime):
                s["timestamp"] = s["timestamp"].isoformat()
        for s in snap.get("dump_signals", []):
            if isinstance(s.get("timestamp"), datetime):
                s["timestamp"] = s["timestamp"].isoformat()
        result.append(snap)
    
    return api_ok({"snapshots": result})

@app.get("/api/crypto/replays")
async def get_replays(limit: int = 36, user=Depends(require_active_subscription)):
    replay = await build_recent_case_replays(limit=limit)
    return api_ok({"replays": replay})

# ─────────────────────────────────────────────
# WATCHLIST & ALERTS
# ─────────────────────────────────────────────
class WatchlistItem(BaseModel):
    symbol: str
    alertEnabled: bool = False
    alertThreshold: int = 80

class WatchlistUpdate(BaseModel):
    items: List[WatchlistItem]

@app.get("/api/user/watchlist")
async def get_watchlist(user=Depends(get_current_user)):
    """Get user's watchlist"""
    watchlist = user.get("watchlist", [])
    return api_ok({"watchlist": watchlist})

@app.post("/api/user/watchlist")
async def update_watchlist(data: WatchlistUpdate, user=Depends(get_current_user)):
    """Update user's watchlist"""
    watchlist_data = [item.dict() for item in data.items]
    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"watchlist": watchlist_data}}
    )
    return api_ok({"message": "Watchlist updated", "watchlist": watchlist_data})

@app.post("/api/user/watchlist/add")
async def add_to_watchlist(item: WatchlistItem, user=Depends(get_current_user)):
    """Add coin to watchlist"""
    watchlist = user.get("watchlist", [])
    # Check if already exists
    if any(w.get("symbol") == item.symbol.upper() for w in watchlist):
        return api_ok({"message": "Already in watchlist", "watchlist": watchlist})
    
    new_item = {
        "symbol": item.symbol.upper(),
        "alertEnabled": item.alertEnabled,
        "alertThreshold": item.alertThreshold,
        "addedAt": datetime.now(timezone.utc).isoformat(),
    }
    watchlist.append(new_item)
    
    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"watchlist": watchlist}}
    )
    return api_ok({"message": f"Added {item.symbol} to watchlist", "watchlist": watchlist})

@app.delete("/api/user/watchlist/{symbol}")
async def remove_from_watchlist(symbol: str, user=Depends(get_current_user)):
    """Remove coin from watchlist"""
    watchlist = user.get("watchlist", [])
    watchlist = [w for w in watchlist if w.get("symbol") != symbol.upper()]
    
    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"watchlist": watchlist}}
    )
    return api_ok({"message": f"Removed {symbol} from watchlist", "watchlist": watchlist})

# ─────────────────────────────────────────────
# EMAIL ALERTS FOR SIGNAL CATEGORIES
# ─────────────────────────────────────────────
def build_signal_route_label(signal: dict) -> str:
    preferred_venue = (
        signal.get("preferred_venue")
        or ((signal.get("decision_engine") or {}).get("preferred_venue"))
        or {}
    )
    if not preferred_venue:
        return ""
    route_name = (preferred_venue.get("name") or "").strip()
    route_pair = (preferred_venue.get("pair") or "").strip()
    route_type = (preferred_venue.get("type") or "").strip().upper()
    parts = [part for part in [route_name, route_pair, route_type] if part]
    return " · ".join(parts)

def classify_signal_alert(signal: dict, signal_type: str) -> Optional[dict]:
    if not is_true_snapshot_signal(signal, signal_type):
        return None

    profile = signal.get("manipulation_profile") or {}
    decision = signal.get("decision_engine") or {}
    source_stack = signal.get("signal_sources") or signal.get("source_stack") or {}
    asset_identity = signal.get("asset_identity") or {}
    rugpull_profile = signal.get("rugpull_profile") or {}

    signal_strength = float(signal.get("signal_strength") or 0) if str(signal.get("signal_strength", "0")).replace(".", "").isdigit() else 70
    volume_ratio = float(profile.get("volume_market_cap_ratio", 0) or decision.get("volume_market_cap_ratio") or 0)
    execution_score = float(decision.get("execution_score", 0) or 0)
    venue_count = int(decision.get("venue_count", 0) or source_stack.get("verified_routes", 0) or 0)
    source_tier = (source_stack.get("confirmation_tier") or "thin").strip().lower()
    telegram_active = bool(source_stack.get("telegram_active"))
    market_confirmed = bool(source_stack.get("coingecko_market_active"))
    bullish_mentions = int(profile.get("bullish_mentions", 0) or 0)
    bearish_mentions = int(profile.get("bearish_mentions", 0) or 0)
    telegram_sources = int(profile.get("telegram_sources", 0) or 0)
    dump_risk_score = float(profile.get("dump_risk_score", 0) or 0)
    identity_classification = (asset_identity.get("classification") or "").strip().lower()
    meme_score = float(asset_identity.get("meme_score", 0) or 0)
    speculative_score = float(asset_identity.get("speculative_score", 0) or 0)
    serious_score = float(asset_identity.get("serious_score", 0) or 0)
    rugpull_score = float(rugpull_profile.get("score", 0) or 0)
    preferred_route = build_signal_route_label(signal)

    if signal_type == "pump":
        if is_alert_worthy_signal(signal, signal_type):
            return {
                "kind": "confirmed_pump",
                "title": "Confirmed Pump",
                "subject_prefix": "🚀 Confirmed Pump",
                "accent": "#10b981",
                "intro": "A high-conviction pump signal passed the strict confirmation stack.",
                "priority": 3,
                "route_label": preferred_route,
            }

        new_meme_candidate_ok = (
            identity_classification in {"meme", "speculative"} and
            serious_score < 42 and
            venue_count >= 1 and
            signal_strength >= 68 and
            volume_ratio >= 8 and
            market_confirmed and
            (telegram_active or bullish_mentions >= 2 or telegram_sources >= 2 or source_tier in {"dual-source", "triple-source", "stacked"}) and
            (meme_score >= 46 or speculative_score >= 52)
        )
        if new_meme_candidate_ok:
            return {
                "kind": "new_meme_candidate",
                "title": "New Meme Candidate",
                "subject_prefix": "🧪 New Meme Candidate",
                "accent": "#38bdf8",
                "intro": "A fresh meme/speculative setup has reached the site before a full confirmed-pump threshold.",
                "priority": 2,
                "route_label": preferred_route,
            }
        return None

    rugpull_dump_ok = (
        identity_classification in {"meme", "speculative"} and
        signal_strength >= 64 and
        volume_ratio >= 6 and
        venue_count >= 1 and
        market_confirmed and
        (rugpull_score >= 58 or dump_risk_score >= 78) and
        (bearish_mentions >= 2 or telegram_sources >= 2 or source_tier in {"dual-source", "triple-source", "stacked"})
    )
    if rugpull_dump_ok:
        return {
            "kind": "rugpull_dump",
            "title": "Rugpull Dump",
            "subject_prefix": "🧨 Rugpull Dump",
            "accent": "#ef4444",
            "intro": "A bearish meme/speculative unwind is showing rugpull-style distribution risk.",
            "priority": 3,
            "route_label": preferred_route,
        }
    return None

async def send_signal_alert_email(email: str, name: str, signal: dict, signal_type: str, alert_meta: dict):
    """Send email alert for categorized signal events."""
    source_summary = signal.get("source_summary") or ((signal.get("signal_sources") or {}).get("summary")) or ""
    route_label = alert_meta.get("route_label") or build_signal_route_label(signal)
    subject_prefix = alert_meta.get("subject_prefix") or ("🚀 Pump" if signal_type == "pump" else "📉 Dump")
    title = alert_meta.get("title") or signal_type.upper()
    intro = alert_meta.get("intro") or f"A new {signal_type} event was detected."
    accent = alert_meta.get("accent") or ("#10b981" if signal_type == "pump" else "#ef4444")
    rugpull_summary = ((signal.get("rugpull_profile") or {}).get("summary") or "")[:220]
    cta_label = "Open Coin Page"
    html = f"""
    <div style="font-family:sans-serif;max-width:600px;margin:0 auto;padding:24px;background:#0f172a;color:#fff;border-radius:12px">
      <div style="text-align:center;margin-bottom:20px">
        <img src="{LOGO_URL}" alt="PumpRadar" style="width:48px;height:48px;border-radius:10px" />
      </div>
      <h2 style="color:{accent};margin-bottom:16px;text-align:center">{title}: {signal.get('symbol')}</h2>
      <p>Hi {name},</p>
      <p>{intro}</p>
      <div style="background:#1e293b;padding:16px;border-radius:8px;margin:16px 0">
        <p style="margin:0 0 8px 0"><strong>Coin:</strong> {signal.get('symbol')} ({signal.get('name', '')})</p>
        <p style="margin:0 0 8px 0"><strong>Signal Strength:</strong> <span style="color:{accent};font-size:18px">{signal.get('signal_strength', 0)}%</span></p>
        <p style="margin:0 0 8px 0"><strong>Price:</strong> ${signal.get('price', 0)}</p>
        <p style="margin:0 0 8px 0"><strong>1h Change:</strong> {signal.get('price_change_1h', 0):+.2f}%</p>
        {f'<p style="margin:0 0 8px 0"><strong>Where to trade:</strong> {route_label}</p>' if route_label else ''}
        {f'<p style="margin:0 0 8px 0"><strong>Source Stack:</strong> {source_summary[:220]}</p>' if source_summary else ''}
        {f'<p style="margin:0 0 8px 0"><strong>Rugpull Read:</strong> {rugpull_summary}</p>' if alert_meta.get("kind") == "rugpull_dump" and rugpull_summary else ''}
        <p style="margin:0"><strong>Reason:</strong> {signal.get('reason', '')[:220]}</p>
      </div>
      <p style="color:#94a3b8;font-size:12px">This is not financial advice. Always do your own research.</p>
      <a href="{APP_URL}/coin/{signal.get('symbol')}?type={signal_type}"
         style="display:inline-block;background:{accent};color:#fff;padding:12px 24px;border-radius:8px;text-decoration:none;margin-top:16px">
        {cta_label}
      </a>
    </div>"""
    try:
        await asyncio.to_thread(resend.Emails.send, {
            "from": f"PumpRadar Alerts <{SENDER_EMAIL}>",
            "to": [email],
            "subject": f"{subject_prefix}: {signal.get('symbol')} ({signal.get('signal_strength')}%)",
            "html": html,
        })
        logger.info("Alert email sent to %s for %s (%s)", email, signal.get("symbol"), alert_meta.get("kind"))
    except Exception as e:
        logger.error(f"Alert email error: {e}")

async def mark_signal_alert_sent(user_id: str, signal: dict, signal_type: str, alert_kind: str) -> bool:
    symbol = (signal.get("symbol") or "").upper()
    snapshot_ts = signal.get("timestamp")
    if isinstance(snapshot_ts, datetime):
        snapshot_key = snapshot_ts.isoformat()
    else:
        snapshot_key = str(snapshot_ts or "")
    if not user_id or not symbol or not snapshot_key or not alert_kind:
        return False
    try:
        result = await db.signal_alert_events.update_one(
            {
                "user_id": user_id,
                "symbol": symbol,
                "signal_type": signal_type,
                "alert_kind": alert_kind,
                "snapshot_key": snapshot_key,
            },
            {
                "$setOnInsert": {
                    "created_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )
        return bool(getattr(result, "upserted_id", None))
    except Exception:
        return False

async def check_and_send_alerts(pump_signals: List[dict], dump_signals: List[dict]):
    """Check for categorized signals and send alerts to users with enabled notifications."""
    pro_users = await db.users.find({
        "subscription": {"$in": ["monthly", "annual"]},
        "email_alerts_enabled": True,
    }).to_list(length=1000)

    if not pro_users:
        return

    alert_candidates: List[dict] = []
    for signal in pump_signals:
        alert_meta = classify_signal_alert(signal, "pump")
        if alert_meta:
            alert_candidates.append({
                "signal_type": "pump",
                "signal": signal,
                "meta": alert_meta,
            })
    for signal in dump_signals:
        alert_meta = classify_signal_alert(signal, "dump")
        if alert_meta:
            alert_candidates.append({
                "signal_type": "dump",
                "signal": signal,
                "meta": alert_meta,
            })

    if not alert_candidates:
        return

    global_caps = {
        "new_meme_candidate": 2,
        "confirmed_pump": 3,
        "rugpull_dump": 2,
    }

    def rank_alert(item: dict) -> tuple:
        signal = item["signal"]
        meta = item["meta"]
        rugpull_score = float(((signal.get("rugpull_profile") or {}).get("score") or 0))
        score_gap = float(((signal.get("direction_audit") or {}).get("score_gap") or 0))
        return (
            int(meta.get("priority", 0)),
            float(signal.get("signal_strength", 0) or 0),
            rugpull_score,
            score_gap,
        )

    ranked_candidates = sorted(alert_candidates, key=rank_alert, reverse=True)

    for user in pro_users:
        user_id = str(user.get("_id") or "")
        email = user.get("email")
        name = user.get("name", "Trader")
        watchlist = user.get("watchlist", [])

        for item in watchlist:
            if not item.get("alertEnabled"):
                continue
            symbol = item.get("symbol", "").upper()
            threshold = item.get("alertThreshold", 80)
            for alert_item in ranked_candidates:
                signal = alert_item["signal"]
                signal_type = alert_item["signal_type"]
                meta = alert_item["meta"]
                if (
                    signal.get("symbol", "").upper() == symbol and
                    signal.get("signal_strength", 0) >= threshold and
                    await mark_signal_alert_sent(user_id, signal, signal_type, meta["kind"])
                ):
                    asyncio.create_task(send_signal_alert_email(email, name, signal, signal_type, meta))

        if user.get("global_alerts_enabled"):
            sent_counts: Dict[str, int] = {}
            for alert_item in ranked_candidates:
                signal = alert_item["signal"]
                signal_type = alert_item["signal_type"]
                meta = alert_item["meta"]
                kind = meta["kind"]
                if sent_counts.get(kind, 0) >= global_caps.get(kind, 1):
                    continue
                if await mark_signal_alert_sent(user_id, signal, signal_type, kind):
                    sent_counts[kind] = sent_counts.get(kind, 0) + 1
                    asyncio.create_task(send_signal_alert_email(email, name, signal, signal_type, meta))

# Alert settings endpoints
class AlertSettings(BaseModel):
    email_alerts_enabled: bool = False
    global_alerts_enabled: bool = False

@app.get("/api/user/alerts")
async def get_alert_settings(user=Depends(get_current_user)):
    """Get user's alert settings"""
    return api_ok({
        "email_alerts_enabled": user.get("email_alerts_enabled", False),
        "global_alerts_enabled": user.get("global_alerts_enabled", False),
    })

@app.post("/api/user/alerts")
async def update_alert_settings(settings: AlertSettings, user=Depends(get_current_user)):
    """Update user's alert settings"""
    # Only Pro users can enable alerts
    sub = user.get("subscription", "free")
    if sub not in ("monthly", "annual"):
        raise HTTPException(status_code=402, detail=api_err("Pro subscription required for email alerts", "SUBSCRIPTION_REQUIRED"))
    
    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {
            "email_alerts_enabled": settings.email_alerts_enabled,
            "global_alerts_enabled": settings.global_alerts_enabled,
        }}
    )
    return api_ok({"message": "Alert settings updated"})

# ─────────────────────────────────────────────
# SIGNAL ACCURACY TRACKER
# ─────────────────────────────────────────────
async def track_signal_accuracy():
    """
    Track if PUMP/DUMP predictions came true after 1h, 4h, 24h.
    Called hourly by scheduler.
    """
    try:
        # Get signals that need accuracy check
        now = datetime.now(timezone.utc)
        
        # Find predictions from 1h, 4h, 24h ago that haven't been verified
        time_windows = [
            {"hours": 1, "field": "accuracy_1h"},
            {"hours": 4, "field": "accuracy_4h"},
            {"hours": 24, "field": "accuracy_24h"},
        ]
        
        for window in time_windows:
            target_time = now - timedelta(hours=window["hours"])
            field = window["field"]
            
            # Find signals from that time window that don't have this accuracy field
            snapshots = await db.signal_snapshots.find({
                "timestamp": {
                    "$gte": target_time - timedelta(minutes=30),
                    "$lte": target_time + timedelta(minutes=30)
                },
                field: {"$exists": False}
            }).to_list(length=10)
            
            for snapshot in snapshots:
                pump_signals = snapshot.get("pump_signals", [])
                dump_signals = snapshot.get("dump_signals", [])
                
                # Get current prices for these coins
                correct_pumps = 0
                total_pumps = len(pump_signals)
                correct_dumps = 0
                total_dumps = len(dump_signals)
                
                for signal in pump_signals:
                    symbol = signal.get("symbol", "").lower()
                    original_price = signal.get("price", 0)
                    
                    # Fetch current price
                    try:
                        resp = requests.get(
                            f"https://api.coingecko.com/api/v3/simple/price",
                            params={"ids": signal.get("coin_id", symbol), "vs_currencies": "usd"},
                            timeout=5
                        )
                        if resp.status_code == 200:
                            data = resp.json()
                            current_price = list(data.values())[0].get("usd", 0) if data else 0
                            
                            # PUMP is correct if price increased
                            if current_price > original_price:
                                correct_pumps += 1
                    except:
                        pass
                
                for signal in dump_signals:
                    symbol = signal.get("symbol", "").lower()
                    original_price = signal.get("price", 0)
                    
                    try:
                        resp = requests.get(
                            f"https://api.coingecko.com/api/v3/simple/price",
                            params={"ids": signal.get("coin_id", symbol), "vs_currencies": "usd"},
                            timeout=5
                        )
                        if resp.status_code == 200:
                            data = resp.json()
                            current_price = list(data.values())[0].get("usd", 0) if data else 0
                            
                            # DUMP is correct if price decreased
                            if current_price < original_price:
                                correct_dumps += 1
                    except:
                        pass
                
                # Calculate accuracy
                pump_accuracy = (correct_pumps / total_pumps * 100) if total_pumps > 0 else 0
                dump_accuracy = (correct_dumps / total_dumps * 100) if total_dumps > 0 else 0
                overall_accuracy = ((correct_pumps + correct_dumps) / (total_pumps + total_dumps) * 100) if (total_pumps + total_dumps) > 0 else 0
                
                # Store accuracy data
                await db.signal_snapshots.update_one(
                    {"_id": snapshot["_id"]},
                    {"$set": {
                        field: {
                            "pump_accuracy": round(pump_accuracy, 1),
                            "dump_accuracy": round(dump_accuracy, 1),
                            "overall_accuracy": round(overall_accuracy, 1),
                            "correct_pumps": correct_pumps,
                            "total_pumps": total_pumps,
                            "correct_dumps": correct_dumps,
                            "total_dumps": total_dumps,
                            "verified_at": now.isoformat(),
                        }
                    }}
                )
                
                logger.info(f"Accuracy tracked for {window['hours']}h: PUMP {pump_accuracy:.1f}%, DUMP {dump_accuracy:.1f}%")
                
    except Exception as e:
        logger.error(f"Accuracy tracking error: {e}")

@app.get("/api/crypto/accuracy")
async def get_signal_accuracy(user=Depends(get_optional_user)):
    """Get signal accuracy statistics"""
    # Get last 24 snapshots with accuracy data
    snapshots = await db.signal_snapshots.find({
        "$or": [
            {"accuracy_1h": {"$exists": True}},
            {"accuracy_4h": {"$exists": True}},
            {"accuracy_24h": {"$exists": True}},
        ]
    }).sort("timestamp", -1).limit(48).to_list(length=48)
    
    # Aggregate accuracy stats
    stats_1h = {"pump": [], "dump": [], "overall": []}
    stats_4h = {"pump": [], "dump": [], "overall": []}
    stats_24h = {"pump": [], "dump": [], "overall": []}
    
    for snap in snapshots:
        if snap.get("accuracy_1h"):
            acc = snap["accuracy_1h"]
            stats_1h["pump"].append(acc.get("pump_accuracy", 0))
            stats_1h["dump"].append(acc.get("dump_accuracy", 0))
            stats_1h["overall"].append(acc.get("overall_accuracy", 0))
        
        if snap.get("accuracy_4h"):
            acc = snap["accuracy_4h"]
            stats_4h["pump"].append(acc.get("pump_accuracy", 0))
            stats_4h["dump"].append(acc.get("dump_accuracy", 0))
            stats_4h["overall"].append(acc.get("overall_accuracy", 0))
        
        if snap.get("accuracy_24h"):
            acc = snap["accuracy_24h"]
            stats_24h["pump"].append(acc.get("pump_accuracy", 0))
            stats_24h["dump"].append(acc.get("dump_accuracy", 0))
            stats_24h["overall"].append(acc.get("overall_accuracy", 0))
    
    def avg(lst):
        return round(sum(lst) / len(lst), 1) if lst else 0
    
    return api_ok({
        "accuracy_1h": {
            "pump": avg(stats_1h["pump"]),
            "dump": avg(stats_1h["dump"]),
            "overall": avg(stats_1h["overall"]),
            "samples": len(stats_1h["overall"]),
        },
        "accuracy_4h": {
            "pump": avg(stats_4h["pump"]),
            "dump": avg(stats_4h["dump"]),
            "overall": avg(stats_4h["overall"]),
            "samples": len(stats_4h["overall"]),
        },
        "accuracy_24h": {
            "pump": avg(stats_24h["pump"]),
            "dump": avg(stats_24h["dump"]),
            "overall": avg(stats_24h["overall"]),
            "samples": len(stats_24h["overall"]),
        },
        "last_updated": snapshots[0]["timestamp"].isoformat() if snapshots else None,
    })

# ─────────────────────────────────────────────
# DAILY MARKET OPEN EMAILS (PRO FEATURE)
# ─────────────────────────────────────────────
async def send_market_open_email(market: str):
    """
    Send best signal candidates email at market open.
    Called by scheduler at:
    - London: 08:00 UTC (LSE opens)
    - New York: 14:30 UTC (NYSE opens at 9:30 AM EST)
    """
    try:
        # Get latest signals
        snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
        if not snapshot:
            logger.warning(f"No signals available for {market} market open email")
            return
        
        pump_signals = snapshot.get("pump_signals", [])[:5]  # Top 5 pumps
        dump_signals = snapshot.get("dump_signals", [])[:3]  # Top 3 dumps
        fear_greed = snapshot.get("fear_greed", {})
        market_summary = snapshot.get("market_summary", "")
        
        if not pump_signals and not dump_signals:
            return
        
        # Get all Pro subscribers with daily emails enabled
        pro_users = await db.users.find({
            "subscription": {"$in": ["monthly", "annual"]},
            "daily_market_emails": {"$ne": False},  # Default to True if not set
        }).to_list(length=1000)
        
        if not pro_users:
            return
        
        market_name = "London Stock Exchange" if market == "london" else "New York Stock Exchange"
        market_emoji = "🇬🇧" if market == "london" else "🇺🇸"
        
        for user in pro_users:
            email = user.get("email")
            name = user.get("name", "Trader")
            
            # Build email content
            pump_html = ""
            for i, s in enumerate(pump_signals, 1):
                pump_html += f"""
                <tr>
                  <td style="padding:8px;border-bottom:1px solid #334155">{i}. <strong>{s.get('symbol')}</strong></td>
                  <td style="padding:8px;border-bottom:1px solid #334155;color:#10b981">{s.get('signal_strength', 0)}%</td>
                  <td style="padding:8px;border-bottom:1px solid #334155">${s.get('price', 0):.6f}</td>
                  <td style="padding:8px;border-bottom:1px solid #334155;color:#10b981">+{s.get('price_change_1h', 0):.2f}%</td>
                </tr>"""
            
            dump_html = ""
            for i, s in enumerate(dump_signals, 1):
                dump_html += f"""
                <tr>
                  <td style="padding:8px;border-bottom:1px solid #334155">{i}. <strong>{s.get('symbol')}</strong></td>
                  <td style="padding:8px;border-bottom:1px solid #334155;color:#ef4444">{s.get('signal_strength', 0)}%</td>
                  <td style="padding:8px;border-bottom:1px solid #334155">${s.get('price', 0):.6f}</td>
                  <td style="padding:8px;border-bottom:1px solid #334155;color:#ef4444">{s.get('price_change_1h', 0):.2f}%</td>
                </tr>"""
            
            html = f"""
            <div style="font-family:sans-serif;max-width:650px;margin:0 auto;padding:24px;background:#0f172a;color:#fff;border-radius:12px">
              <div style="text-align:center;margin-bottom:24px">
                <img src="{LOGO_URL}" alt="PumpRadar" style="width:56px;height:56px;border-radius:12px;margin-bottom:12px" />
                <h1 style="color:#fff;margin:0">{market_emoji} {market_name} Opens</h1>
                <p style="color:#94a3b8;margin:8px 0 0 0">Daily Signal Report for {name}</p>
              </div>
              
              <div style="background:#1e293b;padding:16px;border-radius:8px;margin-bottom:20px">
                <div style="display:flex;justify-content:space-between;align-items:center">
                  <span style="color:#94a3b8">Fear & Greed Index</span>
                  <span style="font-size:24px;font-weight:bold;color:{'#ef4444' if fear_greed.get('value', 50) < 30 else '#f59e0b' if fear_greed.get('value', 50) < 50 else '#10b981'}">{fear_greed.get('value', 'N/A')}/100</span>
                </div>
                <p style="color:#cbd5e1;margin:8px 0 0 0;font-size:14px">{fear_greed.get('classification', 'N/A')}</p>
              </div>
              
              <h2 style="color:#10b981;margin:24px 0 12px 0;font-size:18px">🚀 Top PUMP Candidates</h2>
              <table style="width:100%;border-collapse:collapse;background:#1e293b;border-radius:8px;overflow:hidden">
                <thead>
                  <tr style="background:#334155">
                    <th style="padding:10px;text-align:left;color:#94a3b8">Coin</th>
                    <th style="padding:10px;text-align:left;color:#94a3b8">Signal</th>
                    <th style="padding:10px;text-align:left;color:#94a3b8">Price</th>
                    <th style="padding:10px;text-align:left;color:#94a3b8">1h</th>
                  </tr>
                </thead>
                <tbody>{pump_html}</tbody>
              </table>
              
              <h2 style="color:#ef4444;margin:24px 0 12px 0;font-size:18px">📉 DUMP Warnings</h2>
              <table style="width:100%;border-collapse:collapse;background:#1e293b;border-radius:8px;overflow:hidden">
                <thead>
                  <tr style="background:#334155">
                    <th style="padding:10px;text-align:left;color:#94a3b8">Coin</th>
                    <th style="padding:10px;text-align:left;color:#94a3b8">Signal</th>
                    <th style="padding:10px;text-align:left;color:#94a3b8">Price</th>
                    <th style="padding:10px;text-align:left;color:#94a3b8">1h</th>
                  </tr>
                </thead>
                <tbody>{dump_html}</tbody>
              </table>
              
              <div style="background:#1e293b;padding:16px;border-radius:8px;margin-top:20px">
                <h3 style="color:#6366f1;margin:0 0 8px 0;font-size:14px">🤖 AI Market Summary</h3>
                <p style="color:#cbd5e1;margin:0;font-size:14px;line-height:1.5">{market_summary[:400]}...</p>
              </div>
              
              <div style="text-align:center;margin-top:24px">
                <a href="{APP_URL}/dashboard" 
                   style="display:inline-block;background:linear-gradient(135deg,#10b981,#059669);color:#fff;padding:14px 32px;border-radius:8px;text-decoration:none;font-weight:bold">
                  View Full Dashboard →
                </a>
              </div>
              
              <p style="color:#64748b;font-size:11px;text-align:center;margin-top:24px;border-top:1px solid #334155;padding-top:16px">
                This is not financial advice. Always do your own research.<br>
                <a href="{APP_URL}/settings" style="color:#6366f1">Manage email preferences</a>
              </p>
            </div>"""
            
            try:
                await asyncio.to_thread(resend.Emails.send, {
                    "from": f"PumpRadar <{SENDER_EMAIL}>",
                    "to": [email],
                    "subject": f"{market_emoji} {market_name} Opens - Top Signals for Today",
                    "html": html,
                })
                logger.info(f"Market open email sent to {email} for {market}")
            except Exception as e:
                logger.error(f"Market open email error for {email}: {e}")
        
        logger.info(f"{market.upper()} market open emails sent to {len(pro_users)} Pro users")
        
    except Exception as e:
        logger.exception(f"Market open email job error: {e}")

async def send_london_market_email():
    """Wrapper for London market open email"""
    await send_market_open_email("london")

async def send_nyse_market_email():
    """Wrapper for NYSE market open email"""
    await send_market_open_email("nyse")

async def send_trial_reminder_emails():
    """Send one reminder shortly before the card-backed trial converts to paid."""
    now = datetime.now(timezone.utc)
    reminder_cutoff_start = now + timedelta(hours=23)
    reminder_cutoff_end = now + timedelta(hours=25)
    users = await db.users.find({
        "subscription": "trial",
        "subscription_expiry": {"$gte": reminder_cutoff_start, "$lte": reminder_cutoff_end},
        "trial_reminder_sent_at": {"$exists": False},
        "pending_plan": {"$in": ["monthly", "annual"]},
    }).to_list(length=500)

    for user in users:
        trial_end = normalize_datetime(user.get("subscription_expiry"))
        if not trial_end:
            continue
        await send_trial_reminder_email(
            user["email"],
            user.get("name", ""),
            SUBSCRIPTION_PLANS.get(user.get("pending_plan"), SUBSCRIPTION_PLANS["monthly"])["name"],
            trial_end,
        )
        await db.users.update_one(
            {"_id": user["_id"]},
            {"$set": {"trial_reminder_sent_at": now}},
        )

@app.post("/api/crypto/refresh")
async def manual_refresh(user=Depends(require_active_subscription)):
    """Manual trigger for a real signal refresh"""
    result = await fetch_and_store_signals(trigger="manual")
    scan_status = build_signal_scan_status()
    if not result.get("started") and scan_status.get("running"):
        return api_ok({
            "message": "A signal scan is already running.",
            "started": False,
            "completed": False,
            "scan_status": scan_status,
        })
    if not result.get("completed"):
        raise HTTPException(
            status_code=500,
            detail=api_err("Signal scan failed. Please try again.", "SIGNAL_REFRESH_FAILED"),
        )
    return api_ok({
        "message": "Signal scan finished successfully.",
        "started": True,
        "completed": True,
        "pump_count": result.get("pump_count", 0),
        "dump_count": result.get("dump_count", 0),
        "coins_analyzed": result.get("coins_analyzed", 0),
        "snapshot_at": serialize_datetime(result.get("snapshot_at")),
        "scan_status": build_signal_scan_status(),
    })

# ─────────────────────────────────────────────
# SUBSCRIPTION / STRIPE
# ─────────────────────────────────────────────
SUBSCRIPTION_PLANS = {
    "trial": {"name": "Trial 7d", "price": 0.0, "currency": "usd", "duration_days": 7},
    "monthly": {"name": "Monthly", "price": 29.99, "currency": "usd", "duration_days": 30, "interval": "month", "interval_count": 1},
    "annual": {"name": "Annual", "price": 299.99, "currency": "usd", "duration_days": 365, "interval": "year", "interval_count": 1},
}

class CheckoutRequest(BaseModel):
    plan: str
    origin_url: str

class BillingPortalRequest(BaseModel):
    origin_url: str

def normalize_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None

async def apply_stripe_subscription_state(
    *,
    tx: dict,
    subscription_id: str,
    checkout_session_id: Optional[str] = None,
    trigger_email: bool = True,
) -> dict:
    subscription = stripe.Subscription.retrieve(subscription_id)
    plan_name = tx.get("plan", "monthly")
    status = subscription.get("status")
    trial_end_raw = subscription.get("trial_end")
    current_period_end_raw = subscription.get("current_period_end")
    trial_end = datetime.fromtimestamp(trial_end_raw, tz=timezone.utc) if trial_end_raw else None
    period_end = datetime.fromtimestamp(current_period_end_raw, tz=timezone.utc) if current_period_end_raw else None

    user_update = {
        "stripe_customer_id": subscription.get("customer"),
        "stripe_subscription_id": subscription_id,
        "stripe_subscription_status": status,
        "pending_plan": plan_name if status == "trialing" else None,
    }
    tx_update = {
        "stripe_subscription_id": subscription_id,
        "stripe_customer_id": subscription.get("customer"),
        "subscription_status": status,
        "updated_at": datetime.now(timezone.utc),
    }
    if checkout_session_id:
        tx_update["session_id"] = checkout_session_id

    if status == "trialing":
        user_update.update({
            "subscription": "trial",
            "subscription_expiry": trial_end,
            "trial_plan": plan_name,
            "trial_started_at": datetime.now(timezone.utc),
            "trial_reminder_sent_at": None,
        })
        tx_update.update({
            "payment_status": "trialing",
            "status": "trialing",
            "trial_end": trial_end,
        })
        if trigger_email:
            asyncio.create_task(send_trial_started_email(tx["user_email"], tx.get("user_name", ""), SUBSCRIPTION_PLANS[plan_name]["name"], trial_end))
    elif status in {"active", "past_due"}:
        expiry = period_end or (datetime.now(timezone.utc) + timedelta(days=SUBSCRIPTION_PLANS[plan_name]["duration_days"]))
        user_update.update({
            "subscription": plan_name,
            "subscription_expiry": expiry,
            "trial_plan": None,
            "pending_plan": None,
            "trial_started_at": None,
        })
        tx_update.update({
            "payment_status": "paid",
            "status": "completed",
            "completed_at": datetime.now(timezone.utc),
        })
        if trigger_email:
            asyncio.create_task(send_subscription_activated_email(tx["user_email"], tx.get("user_name", ""), SUBSCRIPTION_PLANS[plan_name]["name"], expiry))

    await db.users.update_one({"_id": ObjectId(tx["user_id"])}, {"$set": user_update})
    await db.payment_transactions.update_one({"_id": tx["_id"]}, {"$set": tx_update})
    return {"subscription": subscription, "status": status, "trial_end": trial_end, "period_end": period_end}

@app.post("/api/payments/checkout")
async def create_checkout(req: CheckoutRequest, request: Request, user=Depends(get_current_user)):
    if req.plan not in SUBSCRIPTION_PLANS or req.plan == "trial":
        raise HTTPException(status_code=400, detail="Invalid plan")

    if looks_like_placeholder(STRIPE_API_KEY, "STRIPE_API_KEY"):
        raise HTTPException(
            status_code=503,
            detail=api_err("Stripe payments are not configured yet", "STRIPE_NOT_CONFIGURED"),
        )
    
    plan = SUBSCRIPTION_PLANS[req.plan]
    
    success_url = f"{req.origin_url}/subscription/success?session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = f"{req.origin_url}/subscription"
    
    # Create Stripe checkout session for card-backed trial
    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{
                "price_data": {
                    "currency": plan["currency"],
                    "product_data": {
                        "name": f"PumpRadar {req.plan.title()} Plan",
                        "description": "Includes a 7-day free trial before billing starts",
                    },
                    "unit_amount": int(plan["price"] * 100),
                    "recurring": {
                        "interval": plan["interval"],
                        "interval_count": plan["interval_count"],
                    },
                },
                "quantity": 1,
            }],
            mode="subscription",
            success_url=success_url,
            cancel_url=cancel_url,
            customer_email=user["email"],
            billing_address_collection="required",
            phone_number_collection={"enabled": True},
            tax_id_collection={"enabled": True},
            allow_promotion_codes=True,
            subscription_data={
                "trial_period_days": 7,
                "metadata": {
                    "user_id": str(user["_id"]),
                    "user_email": user["email"],
                    "plan": req.plan,
                },
            },
            metadata={
                "user_id": str(user["_id"]),
                "user_email": user["email"],
                "plan": req.plan,
            }
        )
    except stripe.error.AuthenticationError as e:
        logger.error(f"Stripe authentication error: {e}")
        raise HTTPException(
            status_code=503,
            detail=api_err("Stripe payments are not configured correctly", "STRIPE_NOT_CONFIGURED"),
        )
    except stripe.error.StripeError as e:
        logger.error(f"Stripe checkout error: {e}")
        raise HTTPException(
            status_code=502,
            detail=api_err("Stripe checkout is temporarily unavailable", "STRIPE_CHECKOUT_FAILED"),
        )
    
    # Store pending transaction
    await db.payment_transactions.insert_one({
        "session_id": session.id,
        "user_id": str(user["_id"]),
        "user_email": user["email"],
        "user_name": user.get("name", ""),
        "plan": req.plan,
        "amount": plan["price"],
        "currency": plan["currency"],
        "checkout_mode": "subscription",
        "payment_status": "pending",
        "status": "initiated",
        "created_at": datetime.now(timezone.utc),
    })
    
    return api_ok({"url": session.url, "session_id": session.id})

@app.get("/api/payments/status/{session_id}")
async def check_payment_status(session_id: str, user=Depends(get_current_user)):
    if looks_like_placeholder(STRIPE_API_KEY, "STRIPE_API_KEY"):
        raise HTTPException(
            status_code=503,
            detail=api_err("Stripe payments are not configured yet", "STRIPE_NOT_CONFIGURED"),
        )

    # Get Stripe session status
    try:
        session = stripe.checkout.Session.retrieve(session_id)
    except stripe.error.AuthenticationError as e:
        logger.error(f"Stripe authentication error: {e}")
        raise HTTPException(
            status_code=503,
            detail=api_err("Stripe payments are not configured correctly", "STRIPE_NOT_CONFIGURED"),
        )
    except stripe.error.StripeError as e:
        logger.error(f"Stripe status error: {e}")
        raise HTTPException(
            status_code=502,
            detail=api_err("Stripe status check is temporarily unavailable", "STRIPE_STATUS_FAILED"),
        )
    
    payment_status = "pending"
    subscription_status = None
    subscription_id = session.subscription if hasattr(session, "subscription") else None
    if subscription_id:
        subscription = stripe.Subscription.retrieve(subscription_id)
        subscription_status = subscription.status
        if subscription_status == "trialing":
            payment_status = "trialing"
        elif subscription_status in {"active", "past_due"}:
            payment_status = "paid"
    elif session.payment_status == "paid":
        payment_status = "paid"
    elif session.status == "expired":
        payment_status = "expired"
    
    # Update transaction if subscription/trial moved
    tx = await db.payment_transactions.find_one({"session_id": session_id})
    
    if tx and subscription_id and payment_status in {"trialing", "paid"}:
        await apply_stripe_subscription_state(
            tx=tx,
            subscription_id=subscription_id,
            checkout_session_id=session_id,
            trigger_email=False,
        )
    
    return api_ok({
        "status": session.status,
        "payment_status": payment_status,
        "subscription_status": subscription_status,
        "session_id": session_id,
    })

@app.post("/api/payments/portal")
async def create_billing_portal(req: BillingPortalRequest, user=Depends(get_current_user)):
    if looks_like_placeholder(STRIPE_API_KEY, "STRIPE_API_KEY"):
        raise HTTPException(
            status_code=503,
            detail=api_err("Stripe payments are not configured yet", "STRIPE_NOT_CONFIGURED"),
        )
    customer_id = user.get("stripe_customer_id")
    if not customer_id:
        raise HTTPException(
            status_code=400,
            detail=api_err("No Stripe billing profile exists for this account yet.", "BILLING_PROFILE_MISSING"),
        )
    try:
        session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=f"{req.origin_url}/subscription",
        )
    except stripe.error.StripeError as e:
        logger.error(f"Stripe billing portal error: {e}")
        raise HTTPException(
            status_code=502,
            detail=api_err("Stripe billing portal is temporarily unavailable", "STRIPE_PORTAL_FAILED"),
        )
    return api_ok({"url": session.url})

@app.post("/api/webhook/stripe")
async def stripe_webhook(request: Request):
    body = await request.body()
    sig = request.headers.get("Stripe-Signature", "")
    endpoint_secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
    
    try:
        # Verify webhook signature if secret is configured
        if endpoint_secret:
            event = stripe.Webhook.construct_event(body, sig, endpoint_secret)
        else:
            # Without secret, just parse the event
            import json as json_lib
            event = json_lib.loads(body)
        
        event_type = event.get("type") if isinstance(event, dict) else event.type
        
        if event_type == "checkout.session.completed":
            session = event.get("data", {}).get("object", {}) if isinstance(event, dict) else event.data.object
            session_id = session.get("id") if isinstance(session, dict) else session.id
            subscription_id = session.get("subscription") if isinstance(session, dict) else session.subscription
            tx = await db.payment_transactions.find_one({"session_id": session_id})
            if tx and subscription_id:
                await apply_stripe_subscription_state(
                    tx=tx,
                    subscription_id=subscription_id,
                    checkout_session_id=session_id,
                    trigger_email=True,
                )

        if event_type == "invoice.payment_succeeded":
            invoice = event.get("data", {}).get("object", {}) if isinstance(event, dict) else event.data.object
            subscription_id = invoice.get("subscription") if isinstance(invoice, dict) else invoice.subscription
            if subscription_id:
                tx = await db.payment_transactions.find_one({"stripe_subscription_id": subscription_id})
                if tx:
                    await apply_stripe_subscription_state(
                        tx=tx,
                        subscription_id=subscription_id,
                        trigger_email=True,
                    )

        if event_type == "customer.subscription.deleted":
            subscription = event.get("data", {}).get("object", {}) if isinstance(event, dict) else event.data.object
            subscription_id = subscription.get("id") if isinstance(subscription, dict) else subscription.id
            tx = await db.payment_transactions.find_one({"stripe_subscription_id": subscription_id})
            if tx:
                await db.users.update_one(
                    {"_id": ObjectId(tx["user_id"])},
                    {"$set": {
                        "subscription": "free",
                        "subscription_expiry": None,
                        "stripe_subscription_status": "canceled",
                        "pending_plan": None,
                        "trial_plan": None,
                    }}
                )
                await db.payment_transactions.update_one(
                    {"_id": tx["_id"]},
                    {"$set": {"status": "canceled", "payment_status": "canceled", "updated_at": datetime.now(timezone.utc)}}
                )
    except Exception as e:
        logger.error(f"Webhook error: {e}")
    
    return {"status": "ok"}

@app.get("/api/user/subscription")
async def get_subscription(user=Depends(get_current_user)):
    sub = user.get("subscription", "free")
    expiry = user.get("subscription_expiry")
    stripe_status = user.get("stripe_subscription_status")
    pending_plan = user.get("pending_plan") or user.get("trial_plan")
    
    # Normalize expiry to timezone-aware datetime
    if expiry:
        if isinstance(expiry, str):
            expiry = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
        if isinstance(expiry, datetime) and expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
    
    is_active = False
    if sub in ("monthly", "annual"):
        if expiry:
            is_active = datetime.now(timezone.utc) < expiry
        else:
            is_active = True
    elif sub == "trial":
        if expiry:
            is_active = datetime.now(timezone.utc) < expiry
    
    return api_ok({
        "subscription": sub,
        "is_active": is_active,
        "expiry": expiry.isoformat() if isinstance(expiry, datetime) else expiry,
        "stripe_status": stripe_status,
        "pending_plan": pending_plan,
        "next_billing_at": expiry.isoformat() if isinstance(expiry, datetime) else expiry,
        "plans": SUBSCRIPTION_PLANS,
    })

# ─────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler(timezone="UTC")

@app.on_event("startup")
async def startup_event():
    # Create indexes
    await db.users.create_index("email", unique=True)
    await db.signal_snapshots.create_index("timestamp")
    await db.signal_alert_events.create_index(
        [("user_id", 1), ("symbol", 1), ("signal_type", 1), ("snapshot_key", 1)],
        unique=True,
    )
    await db.payment_transactions.create_index("session_id", unique=True)
    await db.telegram_sources.create_index("source_key", unique=True)
    await db.telegram_signals.create_index([("posted_at", -1)])
    await db.telegram_signals.create_index([("source_id", 1), ("posted_at", -1)])
    await db.telegram_signals.create_index([("cluster_key", 1), ("posted_at", -1)])
    
    # Start scheduler - hourly job for signals
    _main_loop = asyncio.get_running_loop()
    scheduler.add_job(
        lambda: asyncio.run_coroutine_threadsafe(run_full_scan(db), _main_loop),
        'interval', hours=1, id='crypto_signals', replace_existing=True
    )
    
    # Accuracy tracking - runs hourly, 30 min offset
    scheduler.add_job(track_signal_accuracy, 'interval', hours=2, id='accuracy_tracker', replace_existing=True)
    scheduler.add_job(send_trial_reminder_emails, 'interval', hours=1, id='trial_reminder_emails', replace_existing=True)
    scheduler.add_job(evaluate_pending_telegram_signals, 'interval', minutes=15, id='telegram_signal_verifier', replace_existing=True)
    
    # Market open emails (PRO feature)
    # London Stock Exchange opens at 08:00 UTC
    scheduler.add_job(send_london_market_email, 'cron', hour=8, minute=0, id='london_market_email', replace_existing=True)
    
    # New York Stock Exchange opens at 14:30 UTC (9:30 AM EST)
    scheduler.add_job(send_nyse_market_email, 'cron', hour=14, minute=30, id='nyse_market_email', replace_existing=True)
    
    scheduler.start()
    
    # Initial fetch with delay to avoid rate limiting on hot-reload
    async def delayed_fetch():
        await asyncio.sleep(30)  # Wait 30s before first fetch
        await run_full_scan(db)
    
    asyncio.create_task(delayed_fetch())  # re-enabled: first scan ~30s after startup

    # Pre-load Qwen cache from MongoDB on startup
    async def preload_qwen_cache():
        try:
            cached_doc = await db.qwen_decision_cache.find_one({})
            if cached_doc and cached_doc.get("items"):
                logger.info(f"Pre-loaded Qwen cache from MongoDB: {len(cached_doc['items'])} items")
            snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
            if snapshot:
                logger.info(f"Last snapshot found: {snapshot.get('timestamp')} - serving from DB until fresh scan")
        except Exception as e:
            logger.warning(f"Startup cache preload failed: {e}")

    asyncio.create_task(preload_qwen_cache())

    # Pre-load Qwen cache from MongoDB on startup
    async def preload_qwen_cache():
        try:
            cached_doc = await db.qwen_decision_cache.find_one({})
            if cached_doc and cached_doc.get("items"):
                logger.info(f"Pre-loaded Qwen cache from MongoDB: {len(cached_doc['items'])} items")
            # Also pre-load last snapshot into dashboard cache
            snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
            if snapshot:
                logger.info(f"Last snapshot found: {snapshot.get('timestamp')} - serving from DB until fresh scan")
        except Exception as e:
            logger.warning(f"Startup cache preload failed: {e}")

    asyncio.create_task(preload_qwen_cache())
    asyncio.create_task(start_telegram_listener())
    logger.info("PumpRadar backend started - scheduler running hourly, first fetch in 30s")

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()
    global telegram_client, telegram_listener_task
    if telegram_listener_task and not telegram_listener_task.done():
        telegram_listener_task.cancel()
    if telegram_client:
        try:
            await telegram_client.disconnect()
        except Exception:
            pass
    client.close()

# ─────────────────────────────────────────────
# AI CHAT CUSTOMER SERVICE
# ─────────────────────────────────────────────
class ChatRequest(BaseModel):
    message: str
    history: Optional[List[dict]] = []

@app.post("/api/ai/chat")
async def ai_chat(req: ChatRequest, user=Depends(require_active_subscription)):
    """AI customer service chat powered by Gemini - Smart & Helpful"""
    try:
        # Get latest signal context with details
        snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
        pump_signals = snapshot.get("pump_signals", []) if snapshot else []
        dump_signals = snapshot.get("dump_signals", []) if snapshot else []
        pump_count = len(pump_signals)
        dump_count = len(dump_signals)
        summary = snapshot.get("market_summary", "") if snapshot else ""
        fear_greed = snapshot.get("fear_greed", {}) if snapshot else {}
        trending = snapshot.get("trending", []) if snapshot else []
        
        # Build detailed signal context
        top_pumps = ", ".join([f"{s.get('symbol')} ({s.get('signal_strength', 0)}%)" for s in pump_signals[:5]]) or "None"
        top_dumps = ", ".join([f"{s.get('symbol')} ({s.get('signal_strength', 0)}%)" for s in dump_signals[:3]]) or "None"
        
        sub = user.get("subscription", "trial")
        user_sub = "Monthly" if sub == "monthly" else "Annual" if sub == "annual" else "Free Trial"
        user_name = user.get("name", "User")
        
        # On-chain radar context (read-only, last 24h)
        try:
            from datetime import datetime as _dt, timedelta as _td, timezone as _tz
            _since = _dt.now(_tz.utc) - _td(hours=24)
            _oc = db["onchain_events"]
            oc_watch = await _oc.count_documents({"recommendation.verdict": "WATCH", "block_time": {"$gte": _since}})
            oc_caution = await _oc.count_documents({"recommendation.verdict": "CAUTION", "block_time": {"$gte": _since}})
            oc_avoid = await _oc.count_documents({"recommendation.verdict": {"$in": ["AVOID", "HONEYPOT"]}, "block_time": {"$gte": _since}})
            _top = await _oc.find(
                {"recommendation.verdict": "WATCH", "block_time": {"$gte": _since}},
                {"_id": 0, "token_symbol": 1, "chain": 1, "scores.early.score": 1},
            ).sort("scores.early.score", -1).limit(5).to_list(5)
            oc_top = ", ".join(
                f"{(t.get('token_symbol') or '?')} [{t.get('chain','').upper()}] ({(t.get('scores',{}).get('early',{}) or {}).get('score','?')})"
                for t in _top
            ) or "None"
        except Exception as _oce:
            logger.warning("ai_chat onchain context failed: %s", _oce)
            oc_watch = oc_caution = oc_avoid = 0
            oc_top = "N/A"

        system_instruction = f"""You are PumpRadar AI - an intelligent crypto market assistant. You're knowledgeable, helpful, and precise.

PLATFORM OVERVIEW:
PumpRadar uses quantitative analysis + AI (Gemini) to detect cryptocurrency pump and dump signals. Data sources: CoinGecko (price/volume), Fear & Greed Index, social trending.

CURRENT LIVE DATA:
- Active PUMP signals: {pump_count} coins showing bullish momentum
- Active DUMP signals: {dump_count} coins showing bearish pressure
- Top PUMP candidates: {top_pumps}
- Top DUMP warnings: {top_dumps}
- Fear & Greed Index: {fear_greed.get('value', 'N/A')}/100 ({fear_greed.get('classification', 'N/A')})
- Trending on CoinGecko: {', '.join(trending[:5]) if trending else 'N/A'}
- Market Summary: {summary}


ON-CHAIN RADAR (newly launched tokens, last 24h, ETH/BSC/Solana, auto-scored):
- WATCH (clean, worth watching): {oc_watch}
- CAUTION (has flags): {oc_caution}
- AVOID/HONEYPOT (likely scam): {oc_avoid}
- Top early plays: {oc_top}
The radar detects brand-new DEX pairs within seconds and scores them by Early (upside) and Threat (danger). Point users to the On-Chain Radar page for live cards.

USER CONTEXT:
- Name: {user_name}
- Subscription: {user_sub}
- Access level: {'Full signal access' if user_sub != 'Free Trial' else 'Limited preview (upgrade for full access)'}

SUBSCRIPTION PLANS:
- Free Trial: 24 hours of full access (automatically granted on signup)
- Monthly: $29.99/month - unlimited signals, AI analysis, coin details
- Annual: $299.99/year - all Pro features and 2 months saved versus monthly

YOUR CAPABILITIES:
1. Explain current market signals with specific data
2. Describe how our quantitative scoring algorithm works (volume/mcap ratio, momentum divergence, trend alignment)
3. Help users understand pump/dump mechanics
4. Guide users through platform features
5. Answer general crypto questions
6. Explain subscription benefits

RESPONSE GUIDELINES:
- ALWAYS respond in English only
- NEVER respond in Romanian or any other language, even if the user writes in another language
- Be concise but informative (2-4 sentences for simple questions, more for complex ones)
- Use specific numbers from live data when relevant
- When discussing signals, mention the actual coins and their scores
- For price predictions: explain we provide probability-based signals, not guarantees
- Always include: "This is not financial advice. Always do your own research."
- Be friendly and professional
- If the user is abusive, insulting, or trolling, set a brief boundary and redirect them to a useful PumpRadar or crypto question
- If the user greets you, thanks you, or asks what you can do, answer naturally instead of forcing a market summary

If asked about a specific coin, check if it's in our current signals and provide details."""
        
        ai_result = await call_claude_haiku_text(
            system_instruction=system_instruction,
            user_prompt=req.message,
            temperature=0.3,
            max_tokens=900,
        )

        if ai_result.get("ok") and ai_result.get("text"):
            return api_ok({
                "reply": ai_result.get("text"),
                "ai_provider": ai_result.get("provider"),
                "ai_model": ai_result.get("model"),
            })

        logger.warning(
            "OpenAI/OpenRouter chat failed - using local fallback. provider=%s error=%s",
            ai_result.get("provider"),
            ai_result.get("error"),
        )
        return api_ok({
            "reply": build_fallback_chat_reply(
                req.message,
                pump_signals,
                dump_signals,
                summary,
                fear_greed,
                trending,
                user_sub,
            ),
            "ai_provider": "local_fallback",
        })
    except Exception as e:
        logger.error(f"OpenAI/OpenRouter chat error - using local fallback: {e}")
        return api_ok({
            "reply": build_fallback_chat_reply(
                req.message,
                pump_signals,
                dump_signals,
                summary,
                fear_greed,
                trending,
                user_sub,
            ),
            "ai_provider": "local_fallback",
        })

# ─────────────────────────────────────────────
# COIN DETAIL
# ─────────────────────────────────────────────
def get_coin_chart_data(coin_id: str, days: int = 1) -> List[dict]:
    """Get hourly price + volume data from CoinGecko"""
    cache_key = f"{coin_id}::{days}"
    cached = get_memory_cache(COIN_CHART_CACHE, cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
        params = {"vs_currency": "usd", "days": days, "interval": "hourly"}
        r = requests.get(url, params=params, headers=CG_HEADERS, timeout=15)
        if r.status_code != 200:
            return []
        data = r.json()
        prices = data.get("prices", [])
        volumes = data.get("total_volumes", [])
        result = []
        for i, (ts, price) in enumerate(prices[-24:]):
            vol = volumes[i][1] if i < len(volumes) else 0
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            result.append({
                "time": dt.strftime("%H:%M"),
                "price": round(price, 6),
                "volume": round(vol),
                "open": round(price * 0.998, 6),
                "high": round(price * 1.005, 6),
                "low": round(price * 0.994, 6),
                "close": round(price, 6),
            })
        return set_memory_cache(COIN_CHART_CACHE, cache_key, result)
    except Exception as e:
        logger.error(f"Chart data error: {e}")
        return []

def format_currency_compact(value: float) -> str:
    value = float(value or 0)
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"${value / 1_000:.1f}K"
    if abs_value >= 1:
        return f"${value:.2f}"
    return f"${value:.6f}"

def format_pct(value: float) -> str:
    value = float(value or 0)
    return f"{value:+.2f}%"

def classify_trading_venue(name: str) -> str:
    venue = (name or "").lower()
    swap_markets = [
        "swap", "uniswap", "pancakeswap", "sushiswap", "raydium", "jupiter",
        "orca", "meteora", "curve", "balancer", "camelot", "thruster",
    ]
    dex_markets = [
        "dydx", "hyperliquid", "gmx", "vertex", "drift", "aevo",
        "perpetual protocol", "kwenta",
    ]
    if any(keyword in venue for keyword in swap_markets):
        return "swap"
    if any(keyword in venue for keyword in dex_markets):
        return "dex"
    return "cex"

KNOWN_QUOTE_SYMBOLS = {
    "tether": "USDT",
    "bridged-usdt": "USDT",
    "binance-bridged-usdt-bnb-smart-chain": "USDT",
    "usd-coin": "USDC",
    "bridged-usdc": "USDC",
    "usd-coin-bridged": "USDC",
    "wrapped-bitcoin": "WBTC",
    "weth": "WETH",
    "wrapped-ether": "WETH",
    "ethereum": "ETH",
    "bitcoin": "BTC",
    "wrapped-bnb": "WBNB",
    "wbnb": "WBNB",
    "solana": "SOL",
}

def is_contract_like_symbol(value: Optional[str]) -> bool:
    token = (value or "").strip()
    return token.lower().startswith("0x") and len(token) >= 20

def normalize_quote_symbol(raw_target: Optional[str], target_coin_id: Optional[str]) -> str:
    target = (raw_target or "").strip().upper()
    if target and not is_contract_like_symbol(target):
        return target

    normalized_target_coin = (target_coin_id or "").strip().lower()
    if normalized_target_coin:
        for key, symbol in KNOWN_QUOTE_SYMBOLS.items():
            if key in normalized_target_coin:
                return symbol

    return target or "QUOTE"

def build_route_pair_label(
    *,
    symbol: str,
    base: Optional[str],
    target: Optional[str],
    target_coin_id: Optional[str] = None,
) -> str:
    base_symbol = (symbol or "").strip().upper() or (base or "").strip().upper() or "BASE"
    if base and not is_contract_like_symbol(base):
        base_symbol = (base or "").strip().upper() or base_symbol
    quote_symbol = normalize_quote_symbol(target, target_coin_id)
    return f"{base_symbol}/{quote_symbol}"

def score_trust_level(trust_score: str) -> int:
    score = (trust_score or "").lower()
    if score == "green":
        return 100
    if score == "yellow":
        return 65
    if score == "red":
        return 25
    if score in {"high", "strong"}:
        return 85
    if score in {"medium", "ok"}:
        return 60
    if score in {"low", "weak"}:
        return 35
    return 45

def fetch_coin_tickers(coin_id: str) -> List[dict]:
    if not coin_id:
        return []
    if coin_id in COIN_TICKERS_CACHE:
        return COIN_TICKERS_CACHE[coin_id]
    try:
        tickers_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/tickers"
        resp = requests.get(
            tickers_url,
            params={"page": 1, "order": "trust_score_desc"},
            headers=CG_HEADERS,
            timeout=15,
        )
        if resp.status_code == 200:
            tickers = resp.json().get("tickers", []) or []
            COIN_TICKERS_CACHE[coin_id] = tickers
            return tickers
    except Exception as e:
        logger.error(f"Coin tickers error for {coin_id}: {e}")
    COIN_TICKERS_CACHE[coin_id] = []
    return []

def resolve_coingecko_coin_id(symbol: str, preferred_name: Optional[str] = None) -> str:
    symbol = (symbol or "").upper().strip()
    preferred_name_normalized = (preferred_name or "").strip().lower()
    if not symbol:
        return ""

    try:
        search_url = f"https://api.coingecko.com/api/v3/search?query={symbol}"
        sr = requests.get(search_url, headers=CG_HEADERS, timeout=10)
        if sr.status_code != 200:
            return symbol.lower()

        coins = sr.json().get("coins", []) or []
        exact_symbol_matches = [coin for coin in coins if (coin.get("symbol") or "").upper() == symbol]
        if not exact_symbol_matches:
            return symbol.lower()

        if preferred_name_normalized:
            for coin in exact_symbol_matches:
                coin_name = (coin.get("name") or "").strip().lower()
                if coin_name == preferred_name_normalized:
                    return coin.get("id") or symbol.lower()

        if len(exact_symbol_matches) == 1:
            return exact_symbol_matches[0].get("id") or symbol.lower()

        candidate_ids = [coin.get("id") for coin in exact_symbol_matches if coin.get("id")]
        if not candidate_ids:
            return symbol.lower()

        mr = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "ids": ",".join(candidate_ids[:10]),
                "price_change_percentage": "1h,24h,7d",
            },
            headers=CG_HEADERS,
            timeout=15,
        )
        if mr.status_code == 200:
            markets = mr.json() or []
            if preferred_name_normalized:
                for market in markets:
                    market_name = (market.get("name") or "").strip().lower()
                    if market_name == preferred_name_normalized:
                        return market.get("id") or symbol.lower()
            markets.sort(key=lambda item: item.get("market_cap") or 0, reverse=True)
            if markets:
                return markets[0].get("id") or symbol.lower()
    except Exception as e:
        logger.error(f"CoinGecko coin resolution error for {symbol}: {e}")

    return symbol.lower()

def get_coin_extended_details(coin_id: str) -> dict:
    if not coin_id:
        return {}
    cached = get_memory_cache(COIN_EXTENDED_DETAILS_CACHE, coin_id, ttl_seconds=600)
    if cached is not None:
        return cached
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
        params = {
            "localization": "false",
            "tickers": "false",
            "market_data": "true",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        }
        resp = requests.get(url, params=params, headers=CG_HEADERS, timeout=20)
        if resp.status_code == 200:
            return set_memory_cache(COIN_EXTENDED_DETAILS_CACHE, coin_id, resp.json() or {})
    except Exception as e:
        logger.error(f"Coin extended detail error for {coin_id}: {e}")
    return {}

def to_bool_flag(value: Any) -> bool:
    return value in (True, 1, "1", "true", "True", "yes", "Yes")

def safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, "", "null"):
            return None
        return float(value)
    except Exception:
        return None

def pick_primary_contract(details: dict) -> tuple[Optional[str], Optional[str]]:
    asset_platform = details.get("asset_platform_id")
    platforms = details.get("platforms") or {}
    if asset_platform and platforms.get(asset_platform):
        return asset_platform, platforms.get(asset_platform)
    for platform, address in platforms.items():
        if address:
            return platform, address
    return None, None

def estimate_slippage(levels: List[list], usd_size: float, side: str) -> Optional[dict]:
    if not levels or usd_size <= 0:
        return None
    remaining_usd = usd_size
    total_qty = 0.0
    total_cost = 0.0
    best_price = safe_float(levels[0][0])
    if not best_price or best_price <= 0:
        return None

    for raw_price, raw_qty in levels:
        price = safe_float(raw_price)
        qty = safe_float(raw_qty)
        if not price or not qty or price <= 0 or qty <= 0:
            continue
        level_notional = price * qty
        take_notional = min(level_notional, remaining_usd)
        take_qty = take_notional / price
        total_qty += take_qty
        total_cost += take_notional
        remaining_usd -= take_notional
        if remaining_usd <= 0:
            break

    if total_qty <= 0:
        return None

    average_price = total_cost / total_qty
    if side == "buy":
        slippage_pct = ((average_price - best_price) / best_price) * 100
    else:
        slippage_pct = ((best_price - average_price) / best_price) * 100
    return {
        "usd_size": usd_size,
        "average_price": round(average_price, 8),
        "best_price": round(best_price, 8),
        "slippage_pct": round(max(0.0, slippage_pct), 4),
        "filled_usd": round(total_cost, 2),
        "fully_filled": remaining_usd <= 0,
    }

def get_binance_orderbook_metrics(symbol: str) -> dict:
    pair = f"{symbol.upper()}USDT"
    cached = get_memory_cache(ORDERBOOK_CACHE, pair, ttl_seconds=30)
    if cached is not None:
        return cached
    try:
        resp = requests.get(
            "https://api.binance.com/api/v3/depth",
            params={"symbol": pair, "limit": 100},
            timeout=12,
        )
        if resp.status_code != 200:
            return {"available": False, "pair": pair}
        data = resp.json() or {}
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if not bids or not asks:
            return {"available": False, "pair": pair}

        best_bid = safe_float(bids[0][0]) or 0
        best_ask = safe_float(asks[0][0]) or 0
        mid = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
        spread_pct = ((best_ask - best_bid) / mid * 100) if mid else None

        def depth_within_pct(levels: List[list], reference: float, pct: float, side: str) -> float:
            total = 0.0
            for raw_price, raw_qty in levels:
                price = safe_float(raw_price)
                qty = safe_float(raw_qty)
                if not price or not qty:
                    continue
                if side == "bid" and price >= reference * (1 - pct / 100):
                    total += price * qty
                elif side == "ask" and price <= reference * (1 + pct / 100):
                    total += price * qty
            return round(total, 2)

        return set_memory_cache(ORDERBOOK_CACHE, pair, {
            "available": True,
            "pair": pair,
            "best_bid": round(best_bid, 8),
            "best_ask": round(best_ask, 8),
            "mid_price": round(mid, 8) if mid else None,
            "spread_pct": round(spread_pct, 4) if spread_pct is not None else None,
            "bid_depth_1pct_usd": depth_within_pct(bids, mid, 1.0, "bid") if mid else 0,
            "ask_depth_1pct_usd": depth_within_pct(asks, mid, 1.0, "ask") if mid else 0,
            "slippage_buy": [s for s in [estimate_slippage(asks, size, "buy") for size in (1000, 5000, 10000)] if s],
            "slippage_sell": [s for s in [estimate_slippage(bids, size, "sell") for size in (1000, 5000, 10000)] if s],
            "source": "Binance Spot",
        })
    except Exception as e:
        logger.error(f"Binance orderbook error for {pair}: {e}")
        return {"available": False, "pair": pair}

def get_binance_derivatives_metrics(symbol: str) -> dict:
    pair = f"{symbol.upper()}USDT"
    cached = get_memory_cache(DERIVATIVES_CACHE, pair, ttl_seconds=30)
    if cached is not None:
        return cached
    try:
        oi_resp = requests.get(
            "https://fapi.binance.com/fapi/v1/openInterest",
            params={"symbol": pair},
            timeout=12,
        )
        funding_resp = requests.get(
            "https://fapi.binance.com/fapi/v1/premiumIndex",
            params={"symbol": pair},
            timeout=12,
        )
        if oi_resp.status_code != 200 or funding_resp.status_code != 200:
            return {"available": False, "pair": pair}
        oi_data = oi_resp.json() or {}
        funding_data = funding_resp.json() or {}
        open_interest = safe_float(oi_data.get("openInterest")) or 0
        mark_price = safe_float(funding_data.get("markPrice")) or safe_float(funding_data.get("indexPrice")) or 0
        funding_rate = safe_float(funding_data.get("lastFundingRate"))
        next_funding_time = funding_data.get("nextFundingTime")
        return set_memory_cache(DERIVATIVES_CACHE, pair, {
            "available": True,
            "pair": pair,
            "open_interest_contracts": round(open_interest, 2),
            "open_interest_usd": round(open_interest * mark_price, 2) if mark_price else None,
            "funding_rate_pct": round((funding_rate or 0) * 100, 4) if funding_rate is not None else None,
            "mark_price": round(mark_price, 8) if mark_price else None,
            "index_price": round(safe_float(funding_data.get("indexPrice")) or 0, 8) if funding_data.get("indexPrice") else None,
            "next_funding_time": datetime.fromtimestamp(int(next_funding_time) / 1000, tz=timezone.utc).isoformat() if next_funding_time else None,
            "source": "Binance Futures",
        })
    except Exception as e:
        logger.error(f"Binance derivatives error for {pair}: {e}")
        return {"available": False, "pair": pair}

def get_holder_distribution(platform: Optional[str], contract_address: Optional[str]) -> dict:
    if not platform or not contract_address or not COINGECKO_API_KEY:
        return {"available": False}
    cache_key = f"{platform}::{contract_address.lower()}"
    cached = get_memory_cache(HOLDER_DISTRIBUTION_CACHE, cache_key, ttl_seconds=600)
    if cached is not None:
        return cached
    network = COINGECKO_NETWORK_MAP.get(platform)
    if not network:
        return {"available": False}
    try:
        resp = requests.get(
            f"https://pro-api.coingecko.com/api/v3/onchain/networks/{network}/tokens/{contract_address}/info",
            headers={"x-cg-pro-api-key": COINGECKO_API_KEY},
            timeout=20,
        )
        if resp.status_code != 200:
            return {"available": False}
        payload = resp.json() or {}
        attributes = (payload.get("data") or {}).get("attributes") or {}
        holders = attributes.get("holders") or {}
        distribution = holders.get("distribution_percentage") or {}
        return set_memory_cache(HOLDER_DISTRIBUTION_CACHE, cache_key, {
            "available": True,
            "holder_count": holders.get("count"),
            "distribution_percentage": distribution,
            "source": "CoinGecko Onchain",
        })
    except Exception as e:
        logger.error(f"Holder distribution error for {platform}:{contract_address}: {e}")
        return {"available": False}

def get_goplus_security(platform: Optional[str], contract_address: Optional[str]) -> dict:
    if not platform or not contract_address:
        return {"available": False}
    cache_key = f"{platform}::{contract_address.lower()}"
    cached = get_memory_cache(GOPLUS_SECURITY_CACHE, cache_key, ttl_seconds=600)
    if cached is not None:
        return cached
    try:
        if platform == "solana":
            resp = requests.get(
                "https://api.gopluslabs.io/api/v1/solana/token_security",
                params={"contract_addresses": contract_address},
                timeout=20,
            )
        else:
            chain_id = GOPLUS_CHAIN_MAP.get(platform)
            if not chain_id:
                return {"available": False}
            resp = requests.get(
                f"https://api.gopluslabs.io/api/v1/token_security/{chain_id}",
                params={"contract_addresses": contract_address},
                timeout=20,
            )
        if resp.status_code != 200:
            return {"available": False}
        payload = resp.json() or {}
        result = payload.get("result") or {}
        token_data = result.get(contract_address) or result.get(contract_address.lower()) or {}
        if not token_data and isinstance(result, dict) and len(result) == 1:
            token_data = next(iter(result.values()))
        return set_memory_cache(GOPLUS_SECURITY_CACHE, cache_key, {"available": bool(token_data), "data": token_data, "source": "GoPlus"})
    except Exception as e:
        logger.error(f"GoPlus security error for {platform}:{contract_address}: {e}")
        return {"available": False}


def normalize_solana_goplus_safety(goplus_result: dict) -> dict:
    """Normalize GoPlus Solana token_security response into compact safety fields.

    Additive helper for experimental Solana safety.
    """
    result = goplus_result or {}
    data = result.get("data") or {}
    if not result.get("available") or not data:
        return {
            "available": False,
            "source": "GoPlus Solana",
            "solana_safety_status": "unavailable",
            "safety_red_flags": ["solana_safety_unavailable"],
        }

    def status_value(field: str):
        value = data.get(field)
        if isinstance(value, dict):
            return str(value.get("status", "")).strip()
        return str(value).strip() if value is not None else ""

    def is_enabled(field: str) -> bool:
        return status_value(field) == "1"

    red_flags = []

    mintable = is_enabled("mintable")
    freezable = is_enabled("freezable")
    closable = is_enabled("closable")
    metadata_mutable = is_enabled("metadata_mutable")
    balance_mutable_authority = is_enabled("balance_mutable_authority")
    default_account_state_upgradable = is_enabled("default_account_state_upgradable")
    transfer_fee_upgradable = is_enabled("transfer_fee_upgradable")
    transfer_hook_upgradable = is_enabled("transfer_hook_upgradable")

    if mintable:
        red_flags.append("solana_mintable")
    if freezable:
        red_flags.append("solana_freezable")
    if closable:
        red_flags.append("solana_closable")
    if metadata_mutable:
        red_flags.append("metadata_mutable")
    if balance_mutable_authority:
        red_flags.append("balance_mutable_authority")
    if default_account_state_upgradable:
        red_flags.append("default_account_state_upgradable")
    if transfer_fee_upgradable:
        red_flags.append("transfer_fee_upgradable")
    if transfer_hook_upgradable:
        red_flags.append("transfer_hook_upgradable")

    holders = data.get("holders") or []
    holder_count_raw = data.get("holder_count")
    try:
        holder_count = int(float(holder_count_raw)) if holder_count_raw not in (None, "") else None
    except Exception:
        holder_count = None

    top_holder_percent = None
    if holders:
        try:
            top_holder_percent = max(float(h.get("percent") or 0) for h in holders)
        except Exception:
            top_holder_percent = None

    if top_holder_percent is not None:
        if top_holder_percent >= 0.30:
            red_flags.append("top_holder_high_concentration")
        elif top_holder_percent >= 0.15:
            red_flags.append("top_holder_elevated_concentration")

    if holder_count is not None and holder_count < 100:
        red_flags.append("low_holder_count")

    trusted_token = str(data.get("trusted_token", "0")) == "1"

    if red_flags:
        status = "risk_flags"
    elif trusted_token:
        status = "trusted_no_flags"
    else:
        status = "no_major_flags"

    return {
        "available": True,
        "source": result.get("source") or "GoPlus Solana",
        "solana_safety_status": status,
        "mintable": mintable,
        "freezable": freezable,
        "closable": closable,
        "metadata_mutable": metadata_mutable,
        "balance_mutable_authority": balance_mutable_authority,
        "default_account_state_upgradable": default_account_state_upgradable,
        "transfer_fee_upgradable": transfer_fee_upgradable,
        "transfer_hook_upgradable": transfer_hook_upgradable,
        "trusted_token": trusted_token,
        "holder_count": holder_count,
        "top_holder_percent": top_holder_percent,
        "safety_red_flags": sorted(set(red_flags)),
    }

def get_goplus_rugpull(platform: Optional[str], contract_address: Optional[str]) -> dict:
    if not platform or not contract_address or platform == "solana":
        return {"available": False}
    cache_key = f"{platform}::{contract_address.lower()}"
    cached = get_memory_cache(GOPLUS_RUGPULL_CACHE, cache_key, ttl_seconds=600)
    if cached is not None:
        return cached
    chain_id = GOPLUS_CHAIN_MAP.get(platform)
    if not chain_id:
        return {"available": False}
    try:
        resp = requests.get(
            f"https://api.gopluslabs.io/api/v1/rugpull_detecting/{chain_id}",
            params={"contract_addresses": contract_address},
            timeout=20,
        )
        if resp.status_code != 200:
            return {"available": False}
        payload = resp.json() or {}
        result = payload.get("result") or {}
        token_data = result.get(contract_address) or result.get(contract_address.lower()) or {}
        if not token_data and isinstance(result, dict) and len(result) == 1:
            token_data = next(iter(result.values()))
        return set_memory_cache(GOPLUS_RUGPULL_CACHE, cache_key, {"available": bool(token_data), "data": token_data, "source": "GoPlus RugPull"})
    except Exception as e:
        logger.error(f"GoPlus rugpull error for {platform}:{contract_address}: {e}")
        return {"available": False}

def build_tokenomics_profile(details: dict) -> dict:
    market_data = details.get("market_data") or {}
    circulating = safe_float(market_data.get("circulating_supply"))
    total = safe_float(market_data.get("total_supply"))
    max_supply = safe_float(market_data.get("max_supply"))
    fdv = safe_float((market_data.get("fully_diluted_valuation") or {}).get("usd"))
    market_cap = safe_float((market_data.get("market_cap") or {}).get("usd"))

    basis_supply = max_supply or total or 0
    circulating_ratio = (circulating / basis_supply * 100) if circulating and basis_supply else None
    diluted_gap_pct = ((basis_supply - circulating) / basis_supply * 100) if circulating and basis_supply else None
    if diluted_gap_pct is None:
        unlock_risk = "Unknown"
    elif diluted_gap_pct > 60:
        unlock_risk = "High"
    elif diluted_gap_pct > 25:
        unlock_risk = "Medium"
    else:
        unlock_risk = "Low"

    warnings = []
    if diluted_gap_pct is not None and diluted_gap_pct > 40:
        warnings.append("Large percentage of supply is not yet circulating.")
    if fdv and market_cap and fdv > market_cap * 2:
        warnings.append("Fully diluted valuation is much higher than current market cap.")
    if max_supply is None and total is None:
        warnings.append("Supply cap is unclear or not reported.")

    return {
        "circulating_supply": circulating,
        "total_supply": total,
        "max_supply": max_supply,
        "fdv_usd": fdv,
        "market_cap_usd": market_cap,
        "circulating_ratio_pct": round(circulating_ratio, 2) if circulating_ratio is not None else None,
        "dilution_gap_pct": round(diluted_gap_pct, 2) if diluted_gap_pct is not None else None,
        "unlock_risk": unlock_risk,
        "warnings": warnings,
        "source": "CoinGecko",
    }

MEME_CATEGORY_KEYWORDS = {
    "meme", "memes", "animal meme", "internet meme", "dog-themed", "cat-themed", "frog-themed",
}
MEME_NAME_KEYWORDS = {
    "inu", "doge", "shib", "pepe", "bonk", "floki", "wojak", "mog", "cat", "dog", "moon",
    "pump", "baby", "elon", "degen", "based", "rekt", "goat", "fart", "trump", "chillguy",
}
SERIOUS_CATEGORY_KEYWORDS = {
    "artificial intelligence", "ai", "layer 1", "layer 2", "oracle", "infrastructure", "depin",
    "bridge", "restaking", "liquid staking", "storage", "privacy", "real world assets", "rwa",
    "gaming", "exchange-based tokens", "derivatives", "lending", "payments", "enterprise",
    "decentralized exchange", "dex", "yield farming", "defi", "smart contract platform",
}
SERIOUS_NAME_KEYWORDS = {
    "chain", "network", "protocol", "finance", "swap", "dex", "bridge", "oracle", "compute",
    "infrastructure", "staking", "rollup", "layer", "data", "storage", "cloud",
    "artificial", "intelligence", "superintelligence", "alliance",
}

def normalize_identity_text(value: Optional[str]) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", (value or "").lower()))

def build_asset_identity_profile(
    *,
    symbol: str,
    coin_id: str,
    name: Optional[str],
    market_cap: float,
    details: Optional[dict],
    venues: Optional[List[dict]],
) -> dict:
    details = details or {}
    venues = venues or []
    categories = [str(item).strip() for item in (details.get("categories") or []) if str(item).strip()]
    categories_blob = " ".join(normalize_identity_text(item) for item in categories)
    identity_blob = " ".join(
        part for part in [
            normalize_identity_text(symbol),
            normalize_identity_text(coin_id),
            normalize_identity_text(name),
            categories_blob,
        ] if part
    )
    platforms = details.get("platforms") or {}
    has_contract = any(bool(address) for address in platforms.values())
    dex_routes = len([venue for venue in venues if venue.get("type") in {"dex", "swap"}])
    cex_routes = len([venue for venue in venues if venue.get("type") == "cex"])

    meme_category_hits = sum(1 for keyword in MEME_CATEGORY_KEYWORDS if keyword in categories_blob)
    meme_name_hits = sum(1 for keyword in MEME_NAME_KEYWORDS if keyword in identity_blob)
    serious_category_hits = sum(1 for keyword in SERIOUS_CATEGORY_KEYWORDS if keyword in categories_blob)
    serious_name_hits = sum(1 for keyword in SERIOUS_NAME_KEYWORDS if keyword in identity_blob)

    meme_score = 0.0
    meme_score += meme_category_hits * 34.0
    meme_score += min(28.0, meme_name_hits * 12.0)
    if has_contract:
        meme_score += 8.0
    if dex_routes:
        meme_score += min(16.0, dex_routes * 4.0)
    if 0 < market_cap <= 1_000_000_000:
        meme_score += 12.0
    elif 0 < market_cap <= 2_500_000_000:
        meme_score += 6.0

    speculative_score = 0.0
    if has_contract:
        speculative_score += 14.0
    if dex_routes:
        speculative_score += min(18.0, dex_routes * 4.5)
    if market_cap and market_cap <= 750_000_000:
        speculative_score += 20.0
    elif market_cap and market_cap <= 2_000_000_000:
        speculative_score += 10.0
    if not categories:
        speculative_score += 8.0
    if cex_routes <= 2:
        speculative_score += 8.0
    speculative_score += min(20.0, meme_name_hits * 4.0)

    serious_score = 0.0
    serious_score += serious_category_hits * 24.0
    serious_score += min(30.0, serious_name_hits * 7.5)
    if serious_name_hits >= 3:
        serious_score += 12.0
    if market_cap >= 5_000_000_000:
        serious_score += 26.0
    elif market_cap >= 2_000_000_000:
        serious_score += 18.0
    elif market_cap >= 1_000_000_000:
        serious_score += 10.0
    if cex_routes >= 4 and dex_routes == 0:
        serious_score += 8.0

    meme_score = round(min(100.0, meme_score), 1)
    speculative_score = round(min(100.0, speculative_score), 1)
    serious_score = round(min(100.0, serious_score), 1)

    if meme_score >= max(42.0, serious_score + 8.0):
        classification = "meme"
    elif serious_score >= max(38.0, meme_score + 10.0):
        classification = "serious"
    else:
        classification = "speculative"

    summary = (
        f"{symbol} classifies as {classification}. "
        f"Meme score {meme_score}/100, speculative score {speculative_score}/100, serious score {serious_score}/100."
    )
    if categories:
        summary += f" Categories: {', '.join(categories[:4])}."
    if has_contract:
        summary += " Contract-based asset."
    if dex_routes:
        summary += f" {dex_routes} DEX/SWAP route{'s' if dex_routes != 1 else ''} detected."

    return {
        "classification": classification,
        "meme_score": meme_score,
        "speculative_score": speculative_score,
        "serious_score": serious_score,
        "meme_category_hits": meme_category_hits,
        "meme_name_hits": meme_name_hits,
        "serious_category_hits": serious_category_hits,
        "serious_name_hits": serious_name_hits,
        "has_contract": has_contract,
        "dex_routes": dex_routes,
        "cex_routes": cex_routes,
        "categories": categories[:8],
        "summary": summary,
    }

def build_holder_concentration_profile(holder_data: dict, goplus_security: dict) -> dict:
    distribution = holder_data.get("distribution_percentage") or {}
    top_10 = safe_float(distribution.get("top_10"))
    top_20 = safe_float(distribution.get("11_20"))
    owner_balance_raw = safe_float(((goplus_security.get("data") or {}).get("owner_percent")))
    creator_balance_raw = safe_float(((goplus_security.get("data") or {}).get("creator_percent")))
    owner_balance = owner_balance_raw if owner_balance_raw and owner_balance_raw > 0 else None
    creator_balance = creator_balance_raw if creator_balance_raw and creator_balance_raw > 0 else None

    concentration_score = None
    warnings = []
    if top_10 is not None:
        concentration_score = top_10
        if top_10 >= 65:
            warnings.append("Top 10 wallets control a very large share of supply.")
        elif top_10 >= 40:
            warnings.append("Holder distribution is still concentrated.")
    if owner_balance and owner_balance >= 5:
        warnings.append("Owner wallet still controls a meaningful token share.")
    if creator_balance and creator_balance >= 5:
        warnings.append("Creator wallet concentration is elevated.")

    return {
        "available": bool(holder_data.get("available") or concentration_score is not None or owner_balance is not None or creator_balance is not None),
        "holder_count": holder_data.get("holder_count"),
        "top_10_pct": top_10,
        "next_bucket_pct": top_20,
        "owner_pct": owner_balance,
        "creator_pct": creator_balance,
        "warnings": warnings,
        "source": holder_data.get("source") or goplus_security.get("source") or "Unavailable",
    }

def build_wallet_cluster_intelligence(holder_data: dict, goplus_security: dict) -> dict:
    distribution = holder_data.get("distribution_percentage") or {}
    top_10 = safe_float(distribution.get("top_10"))
    next_10 = safe_float(distribution.get("11_20"))
    next_20 = safe_float(distribution.get("21_40"))
    next_60 = safe_float(distribution.get("41_100"))
    has_distribution = any(value is not None for value in [top_10, next_10, next_20, next_60])
    long_tail = max(0.0, 100.0 - sum(value or 0.0 for value in [top_10, next_10, next_20, next_60])) if has_distribution else None

    owner_pct_raw = safe_float(((goplus_security.get("data") or {}).get("owner_percent")))
    creator_pct_raw = safe_float(((goplus_security.get("data") or {}).get("creator_percent")))
    owner_pct = owner_pct_raw if owner_pct_raw and owner_pct_raw > 0 else None
    creator_pct = creator_pct_raw if creator_pct_raw and creator_pct_raw > 0 else None
    insider_control = max([value for value in [owner_pct, creator_pct] if value is not None] or [0.0])
    combined_insider = sum(value or 0.0 for value in [owner_pct, creator_pct])

    concentration_score = None
    cluster_risk_score = None
    distribution_quality_score = None
    cluster_risk_level = "Unknown"
    distribution_quality = "Unknown"
    if has_distribution:
        concentration_score = min(
            100.0,
            (top_10 or 0.0) * 0.72 +
            (next_10 or 0.0) * 0.26 +
            (next_20 or 0.0) * 0.12 +
            combined_insider * 1.4
        )
        cluster_risk_score = round(min(100.0, concentration_score))
        distribution_quality_score = round(max(0.0, min(100.0, 100.0 - (((top_10 or 0.0) * 0.9) + ((next_10 or 0.0) * 0.45) + combined_insider * 1.8))))

        if cluster_risk_score >= 80:
            cluster_risk_level = "High"
        elif cluster_risk_score >= 55:
            cluster_risk_level = "Medium"
        else:
            cluster_risk_level = "Low"

        if distribution_quality_score >= 70:
            distribution_quality = "Healthy"
        elif distribution_quality_score >= 45:
            distribution_quality = "Mixed"
        else:
            distribution_quality = "Fragile"

    insider_control_score = round(min(100.0, insider_control * 7.5 + combined_insider * 2.5)) if (owner_pct is not None or creator_pct is not None) else None

    warnings: List[str] = []
    evidence: List[str] = []

    if top_10 is not None and top_10 >= 65:
        warnings.append("Top 10 wallets control an extremely large share of supply.")
    elif top_10 is not None and top_10 >= 40:
        warnings.append("Top 10 wallets still control a concentrated share of supply.")

    if next_10 is not None and next_10 >= 15:
        warnings.append("The next 10 wallets also hold meaningful size, which can amplify coordinated exits.")

    if insider_control >= 5:
        warnings.append("A single owner or creator wallet still has meaningful control.")
    if combined_insider >= 10:
        warnings.append("Combined insider exposure is elevated enough to raise dump vulnerability.")
    if long_tail is not None and long_tail < 35:
        warnings.append("Too little supply appears to be distributed across the long tail of holders.")

    if top_10 is not None:
        evidence.append(f"Top 10 wallets hold {top_10:.2f}% of supply.")
    if next_10 is not None:
        evidence.append(f"Wallets ranked 11-20 hold another {next_10:.2f}%.")
    if combined_insider > 0:
        evidence.append(f"Owner/creator wallets account for {combined_insider:.2f}% combined.")
    if long_tail is not None:
        evidence.append(f"Estimated long-tail distribution is {long_tail:.2f}% of supply.")

    if has_distribution and cluster_risk_level == "High":
        summary = "Supply looks tightly controlled. A handful of wallets appear capable of amplifying both the pump and the exit."
    elif has_distribution and cluster_risk_level == "Medium":
        summary = "Distribution is not clean yet. Large holders can still influence how quickly the move extends or reverses."
    elif has_distribution:
        summary = "Holder distribution looks relatively healthier. Cluster-driven manipulation risk is lower than average for this setup."
    elif owner_pct is not None or creator_pct is not None:
        summary = "Full holder distribution is not available yet. PumpRadar can only verify owner and creator wallet exposure for this asset right now."
    else:
        summary = "No verified wallet clustering data yet."

    buckets = []
    if has_distribution:
        buckets = [
            {"label": "Top 10", "key": "top_10", "pct": round(top_10 or 0.0, 2), "tone": "rose"},
            {"label": "11-20", "key": "next_10", "pct": round(next_10 or 0.0, 2), "tone": "amber"},
            {"label": "21-40", "key": "next_20", "pct": round(next_20 or 0.0, 2), "tone": "sky"},
            {"label": "41-100", "key": "next_60", "pct": round(next_60 or 0.0, 2), "tone": "emerald"},
            {"label": "Long Tail", "key": "long_tail", "pct": round(long_tail or 0.0, 2), "tone": "slate"},
        ]

    available = bool(has_distribution or owner_pct is not None or creator_pct is not None)
    return {
        "available": available,
        "cluster_risk_score": cluster_risk_score if available else None,
        "cluster_risk_level": cluster_risk_level if available else None,
        "insider_control_score": insider_control_score if available else None,
        "distribution_quality_score": distribution_quality_score if available else None,
        "distribution_quality": distribution_quality if available else None,
        "holder_count": holder_data.get("holder_count") if has_distribution else None,
        "top_10_pct": round(top_10, 2) if top_10 is not None else None,
        "next_10_pct": round(next_10, 2) if next_10 is not None else None,
        "next_20_pct": round(next_20, 2) if next_20 is not None else None,
        "next_60_pct": round(next_60, 2) if next_60 is not None else None,
        "long_tail_pct": round(long_tail, 2) if long_tail is not None else None,
        "owner_pct": round(owner_pct, 2) if owner_pct is not None else None,
        "creator_pct": round(creator_pct, 2) if creator_pct is not None else None,
        "combined_insider_pct": round(combined_insider, 2) if (owner_pct is not None or creator_pct is not None) else None,
        "summary": summary,
        "warnings": warnings,
        "evidence": evidence,
        "buckets": buckets,
        "source": holder_data.get("source") or goplus_security.get("source") or "Unavailable",
    }

def build_contract_risk_profile(platform: Optional[str], contract_address: Optional[str], goplus_security: dict, rugpull_data: dict) -> dict:
    if not platform or not contract_address:
        return {"available": False}
    security = goplus_security.get("data") or {}
    rugpull = rugpull_data.get("data") or {}
    if not security and not rugpull:
        return {
            "available": False,
            "platform": platform,
            "contract_address": contract_address,
            "risk_score": None,
            "risk_level": None,
            "buy_tax_pct": None,
            "sell_tax_pct": None,
            "warnings": [],
            "source": "GoPlus",
        }
    warnings = []
    risk_score = 100

    checks = [
        ("is_honeypot", "Honeypot risk detected.", 40),
        ("cannot_sell_all", "Token may restrict selling.", 20),
        ("is_blacklisted", "Blacklist mechanics detected.", 20),
        ("is_open_source", "Contract is not open source.", 15, True),
        ("is_proxy", "Proxy contract detected.", 8),
        ("hidden_owner", "Hidden owner privileges detected.", 15),
        ("owner_change_balance", "Owner can modify balances.", 25),
        ("selfdestruct", "Self-destruct capability detected.", 20),
        ("transfer_pausable", "Transfers can be paused.", 10),
        ("is_mintable", "Additional supply may be mintable.", 10),
    ]
    for item in checks:
        field = item[0]
        message = item[1]
        penalty = item[2]
        invert = len(item) > 3 and item[3]
        flag = to_bool_flag(security.get(field))
        triggered = (not flag) if invert else flag
        if triggered:
            warnings.append(message)
            risk_score -= penalty

    buy_tax = safe_float(security.get("buy_tax"))
    sell_tax = safe_float(security.get("sell_tax"))
    if buy_tax and buy_tax > 10:
        warnings.append(f"Buy tax is elevated at {buy_tax:.2f}%.")
        risk_score -= 12
    if sell_tax and sell_tax > 10:
        warnings.append(f"Sell tax is elevated at {sell_tax:.2f}%.")
        risk_score -= 12
    if to_bool_flag(rugpull.get("risk")) or to_bool_flag(rugpull.get("is_rugpull")):
        warnings.append("Rug-pull detector flagged this contract.")
        risk_score -= 25

    if risk_score >= 75:
        level = "Low"
    elif risk_score >= 50:
        level = "Medium"
    else:
        level = "High"

    return {
        "available": bool(security or rugpull),
        "platform": platform,
        "contract_address": contract_address,
        "risk_score": max(0, risk_score),
        "risk_level": level,
        "buy_tax_pct": buy_tax,
        "sell_tax_pct": sell_tax,
        "warnings": warnings,
        "source": "GoPlus",
    }

def build_rugpull_profile(
    *,
    asset_identity: Optional[dict],
    tokenomics: Optional[dict],
    wallet_cluster_intelligence: Optional[dict],
    contract_risk: Optional[dict],
    venues: Optional[List[dict]],
    manipulation_profile: Optional[dict] = None,
) -> dict:
    asset_identity = asset_identity or {}
    tokenomics = tokenomics or {}
    wallet_cluster_intelligence = wallet_cluster_intelligence or {}
    contract_risk = contract_risk or {}
    venues = venues or []
    manipulation_profile = manipulation_profile or {}

    venue_count = len(venues)
    dex_routes = len([venue for venue in venues if venue.get("type") in {"dex", "swap"}])
    contract_risk_score = safe_float(contract_risk.get("risk_score"))
    contract_risk_level = (contract_risk.get("risk_level") or "").lower()
    cluster_risk_score = safe_float(wallet_cluster_intelligence.get("cluster_risk_score"))
    insider_pct = safe_float(wallet_cluster_intelligence.get("combined_insider_pct"))
    top_10_pct = safe_float(wallet_cluster_intelligence.get("top_10_pct"))
    long_tail_pct = safe_float(wallet_cluster_intelligence.get("long_tail_pct"))
    dilution_gap = safe_float(tokenomics.get("dilution_gap_pct"))
    dump_risk_score = safe_float(manipulation_profile.get("dump_risk_score"))

    rugpull_score = 0.0
    warnings: List[str] = []

    if asset_identity.get("classification") in {"meme", "speculative"}:
        rugpull_score += 12.0
    if contract_risk_level == "high" or (contract_risk_score is not None and contract_risk_score <= 45):
        rugpull_score += 32.0
        warnings.append("Contract risk is already elevated.")
    elif contract_risk_level == "medium" or (contract_risk_score is not None and contract_risk_score <= 65):
        rugpull_score += 16.0

    if cluster_risk_score is not None and cluster_risk_score >= 75:
        rugpull_score += 24.0
        warnings.append("Wallet clustering is severe.")
    elif cluster_risk_score is not None and cluster_risk_score >= 60:
        rugpull_score += 14.0

    if insider_pct is not None and insider_pct >= 12:
        rugpull_score += 18.0
        warnings.append("Insider concentration is elevated.")
    elif insider_pct is not None and insider_pct >= 6:
        rugpull_score += 10.0

    if top_10_pct is not None and top_10_pct >= 55:
        rugpull_score += 14.0
    elif top_10_pct is not None and top_10_pct >= 40:
        rugpull_score += 8.0

    if long_tail_pct is not None and long_tail_pct < 30:
        rugpull_score += 10.0

    if dilution_gap is not None and dilution_gap >= 50:
        rugpull_score += 12.0
    elif dilution_gap is not None and dilution_gap >= 30:
        rugpull_score += 6.0

    if venue_count <= 1:
        rugpull_score += 14.0
        warnings.append("Execution routes are extremely limited.")
    elif venue_count <= 2:
        rugpull_score += 8.0
    if dex_routes and venue_count <= 3:
        rugpull_score += 6.0

    if dump_risk_score is not None and dump_risk_score >= 80:
        rugpull_score += 12.0
    elif dump_risk_score is not None and dump_risk_score >= 65:
        rugpull_score += 6.0

    rugpull_score = round(min(100.0, rugpull_score), 1)
    if rugpull_score >= 72:
        verdict = "high"
    elif rugpull_score >= 48:
        verdict = "medium"
    else:
        verdict = "low"

    summary = (
        f"Rugpull profile is {verdict}. Score {rugpull_score}/100 "
        f"with {venue_count} route{'s' if venue_count != 1 else ''} and {dex_routes} DEX/SWAP route{'s' if dex_routes != 1 else ''}."
    )
    if warnings:
        summary += " " + " ".join(warnings[:3])

    return {
        "score": rugpull_score,
        "verdict": verdict,
        "venue_count": venue_count,
        "dex_routes": dex_routes,
        "warnings": warnings[:4],
        "summary": summary,
    }

def get_exchange_metadata(identifier: str) -> dict:
    if not identifier:
        return {}
    if identifier in EXCHANGE_METADATA_CACHE:
        return EXCHANGE_METADATA_CACHE[identifier]
    try:
        resp = requests.get(
            f"https://api.coingecko.com/api/v3/exchanges/{identifier}",
            headers=CG_HEADERS,
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            metadata = {
                "image": data.get("image") or "",
                "url": data.get("url") or "",
                "name": data.get("name") or identifier,
            }
            EXCHANGE_METADATA_CACHE[identifier] = metadata
            return metadata
    except Exception as e:
        logger.error(f"Exchange metadata error for {identifier}: {e}")
    EXCHANGE_METADATA_CACHE[identifier] = {}
    return {}

def build_exchange_logo_fallback(name: str) -> str:
    normalized = (name or "").strip().lower()
    known = {
        "binance": "https://assets.coingecko.com/markets/images/52/small/binance.jpg",
        "bitmart": "https://assets.coingecko.com/markets/images/239/small/Bitmart.png",
        "upbit": "https://assets.coingecko.com/markets/images/117/small/upbit.png",
        "xt.com": "https://assets.coingecko.com/markets/images/404/small/xt.png",
        "okx": "https://assets.coingecko.com/markets/images/96/small/WeChat_Image_20220118095654.png",
        "whitebit": "https://assets.coingecko.com/markets/images/418/small/whitebit_final.png",
        "ascendex (bitmax)": "https://assets.coingecko.com/markets/images/501/small/ascendex.png",
        "ascendex": "https://assets.coingecko.com/markets/images/501/small/ascendex.png",
        "bybit": "https://assets.coingecko.com/markets/images/698/small/bybit_spot.png",
        "mexc": "https://assets.coingecko.com/markets/images/409/small/mexc.jpeg",
        "gate": "https://assets.coingecko.com/markets/images/60/small/gateio.png",
        "kucoin": "https://assets.coingecko.com/markets/images/61/small/kucoin.png",
        "coinbase exchange": "https://assets.coingecko.com/markets/images/23/small/Coinbase_Coin_Primary.png",
        "coinbase": "https://assets.coingecko.com/markets/images/23/small/Coinbase_Coin_Primary.png",
        "kraken": "https://assets.coingecko.com/markets/images/29/small/kraken.jpg",
        "uniswap v3 (ethereum)": "https://assets.coingecko.com/markets/images/665/small/uniswap.png",
        "uniswap": "https://assets.coingecko.com/markets/images/665/small/uniswap.png",
        "pancakeswap v3 (bsc)": "https://assets.coingecko.com/markets/images/687/small/pancakeswap.jpeg",
        "pancakeswap": "https://assets.coingecko.com/markets/images/687/small/pancakeswap.jpeg",
        "raydium": "https://assets.coingecko.com/markets/images/694/small/raydium.jpeg",
        "jupiter": "https://assets.coingecko.com/markets/images/1174/small/jupiter.jpg",
        "orca": "https://assets.coingecko.com/markets/images/691/small/orca.jpeg",
    }
    return known.get(normalized, "")

def build_coin_analysis_sections(
    *,
    symbol: str,
    signal_type: str,
    price: float,
    price_change_1h: float,
    price_change_24h: float,
    price_change_7d: float,
    volume_24h: float,
    market_cap: float,
    signal_strength: float,
    confidence: str,
    risk_level: str,
    reason: str,
    social_volume: float = 0,
    galaxy_score: float = 0,
    direction_audit: Optional[dict] = None,
) -> List[dict]:
    volume_ratio = (volume_24h / market_cap * 100) if market_cap else 0.0
    resolved_direction = (direction_audit or {}).get("resolved_direction", signal_type)
    transition_state = (direction_audit or {}).get("transition_state", "bullish_continuation" if resolved_direction == "pump" else "bearish_breakdown")
    if resolved_direction == "pump":
        if transition_state == "bullish_pullback":
            direction_word = "a bullish pullback within a broader upside structure"
        elif transition_state == "bullish_reversal":
            direction_word = "an early bullish reversal attempt after prior weakness"
        else:
            direction_word = "bullish continuation"
    else:
        if transition_state == "dead_cat_bounce":
            direction_word = "bearish structure with only a countertrend bounce so far"
        elif transition_state == "bearish_reversal":
            direction_word = "failed upside structure rolling into fresh downside pressure"
        else:
            direction_word = "distribution / sell pressure"
    acceleration = "accelerating" if abs(price_change_1h) >= abs(price_change_24h) / 6 else "steady"
    confidence_label = confidence.capitalize()
    risk_label = risk_level.capitalize()
    market_participation = "very high" if volume_ratio >= 50 else "healthy" if volume_ratio >= 20 else "light"
    sections = [
        {
            "title": "Momentum Setup",
            "body": (
                f"{symbol} is trading at {format_currency_compact(price)} with a 1h move of {format_pct(price_change_1h)}, "
                f"a 24h move of {format_pct(price_change_24h)}, and a 7d move of {format_pct(price_change_7d)}. "
                f"This profile suggests {direction_word} with {acceleration} short-term momentum rather than a flat range."
            ),
        },
        {
            "title": "Liquidity & Participation",
            "body": (
                f"24h turnover is {format_currency_compact(volume_24h)} against a market cap of {format_currency_compact(market_cap)}, "
                f"which puts the volume/market-cap ratio at {volume_ratio:.2f}%. That points to {market_participation} trading participation; "
                f"the move is more credible when liquidity expands alongside price instead of moving on thin volume."
            ),
        },
        {
            "title": "Signal Read",
            "body": (
                f"PumpRadar scored this setup at {int(signal_strength)}% with {confidence_label.lower()} confidence and {risk_label.lower()} risk. "
                f"The latest trigger was: {reason or 'quantitative momentum, liquidity, and trend alignment'}. "
                f"{'Social activity is elevated. ' if social_volume else ''}"
                f"{f'Galaxy score is {int(galaxy_score)}, reinforcing market attention. ' if galaxy_score else ''}"
                f"This should be treated as a tactical setup, not a long-term conviction call."
            ),
        },
        {
            "title": "Risk Watch",
            "body": (
                f"The main invalidation to monitor is a sharp cooldown in hourly momentum or a drop in volume after the initial move. "
                f"For {'pump' if resolved_direction == 'pump' else 'dump'} setups, weak follow-through after a strong first impulse often signals exhaustion, "
                f"fake {'breakout' if resolved_direction == 'pump' else 'breakdown'} behavior, or a fast reversal."
            ),
        },
    ]
    return sections




def build_solana_dex_context(candidate: dict) -> dict:
    """Normalize Solana DEX / launch context from GeckoTerminal candidate fields."""
    candidate = candidate or {}
    dex_text = str(candidate.get("dex") or "").lower()
    mode = str(candidate.get("mode") or "").lower()
    origin = str(candidate.get("candidate_origin") or "").lower()
    pool_created_at = candidate.get("pool_created_at")

    if "pump.fun" in dex_text:
        dex_family = "pumpfun"
    elif "pumpswap" in dex_text:
        dex_family = "pumpswap"
    elif "meteora" in dex_text:
        dex_family = "meteora"
    elif "raydium" in dex_text:
        dex_family = "raydium"
    elif "orca" in dex_text:
        dex_family = "orca"
    else:
        dex_family = "other"

    is_new_pool = mode in {"new", "new_pools"} or "new_pools" in origin
    is_trending_pool = mode == "trending" or "trending_pools" in origin
    is_pumpfun_related = dex_family in {"pumpfun", "pumpswap"} or str(candidate.get("token_address") or "").lower().endswith("pump")

    reserve_usd = safe_float(candidate.get("reserve_usd")) or 0
    volume = candidate.get("volume_usd") or {}
    changes = candidate.get("price_change_pct") or {}

    volume_h1 = safe_float(volume.get("h1")) or 0
    volume_h24 = safe_float(volume.get("h24")) or 0
    change_h1 = safe_float(changes.get("h1")) or 0
    change_h24 = safe_float(changes.get("h24")) or 0
    vl_h1 = safe_float(candidate.get("volume_liquidity_ratio_h1")) or 0
    vl_h24 = safe_float(candidate.get("volume_liquidity_ratio_h24")) or 0

    flags = []

    if is_new_pool:
        flags.append("solana_new_pool")
    if is_pumpfun_related:
        flags.append("pumpfun_related")
    if reserve_usd and reserve_usd < 2500:
        flags.append("very_thin_solana_liquidity")
    elif reserve_usd and reserve_usd < 10000:
        flags.append("thin_solana_liquidity")
    if vl_h24 >= 30 or vl_h1 >= 5:
        flags.append("high_solana_volume_liquidity_ratio")
    if abs(change_h1) >= 50 or abs(change_h24) >= 300:
        flags.append("extreme_solana_price_move")
    if is_new_pool and reserve_usd < 10000:
        flags.append("new_pool_low_liquidity")
    if is_pumpfun_related and is_new_pool:
        flags.append("pumpfun_new_launch_context")

    if is_new_pool:
        launch_context = "new_pool"
    elif is_pumpfun_related and is_trending_pool:
        launch_context = "active_meme_pool"
    elif is_trending_pool:
        launch_context = "trending_pool"
    else:
        launch_context = "dex_pool"

    if dex_family in {"meteora", "raydium", "orca"} and not is_new_pool:
        launch_context = "post_launch_dex_pool"

    return {
        "available": True,
        "source": "GeckoTerminal Solana DEX context",
        "dex_family": dex_family,
        "launch_context": launch_context,
        "is_pumpfun_related": is_pumpfun_related,
        "is_new_pool": is_new_pool,
        "is_trending_pool": is_trending_pool,
        "pool_created_at": pool_created_at,
        "reserve_usd": reserve_usd,
        "volume_h1": volume_h1,
        "volume_h24": volume_h24,
        "volume_liquidity_ratio_h1": vl_h1,
        "volume_liquidity_ratio_h24": vl_h24,
        "price_change_h1": change_h1,
        "price_change_h24": change_h24,
        "solana_meme_risk_flags": sorted(set(flags)),
    }

def build_geckoterminal_signal_v2(candidate: dict) -> dict:
    """Build experimental Signal Schema v2 payload from a GeckoTerminal pool candidate.

    Additive only:
    - does not modify pump_signals/dump_signals
    - does not feed the main dashboard yet
    - intended for experimental_signals_v2.geckoterminal in snapshots
    """
    candidate = candidate or {}

    symbol = (candidate.get("symbol") or "UNKNOWN").upper()
    name = candidate.get("name") or symbol
    chain = candidate.get("chain") or candidate.get("network")
    direction = (candidate.get("direction_hint") or "pump").lower()
    if direction not in {"pump", "dump"}:
        direction = "pump"

    score = _clamp_score(candidate.get("dex_candidate_score"), 0)
    reserve_usd = safe_float(candidate.get("reserve_usd")) or 0
    volume = candidate.get("volume_usd") or {}
    changes = candidate.get("price_change_pct") or {}
    tx = candidate.get("transactions") or {}

    volume_h1 = safe_float(volume.get("h1")) or 0
    volume_h24 = safe_float(volume.get("h24")) or 0
    change_m5 = safe_float(changes.get("m5")) or 0
    change_h1 = safe_float(changes.get("h1")) or 0
    change_h24 = safe_float(changes.get("h24")) or 0
    vl_h1 = safe_float(candidate.get("volume_liquidity_ratio_h1")) or 0
    vl_h24 = safe_float(candidate.get("volume_liquidity_ratio_h24")) or 0
    bs_h1 = safe_float(tx.get("h1_buy_sell_ratio")) or 0
    bs_h24 = safe_float(tx.get("h24_buy_sell_ratio")) or 0

    abs_h1 = abs(change_h1)
    abs_h24 = abs(change_h24)

    manipulation_setup_score = _clamp_score(
        score * 0.45
        + min(35, vl_h1 * 7)
        + min(25, vl_h24 * 0.9)
        + min(20, abs_h1 * 0.35)
    )

    if direction == "dump":
        pump_coordination_score = 0
        dump_distribution_score = _clamp_score(
            score * 0.35
            + min(35, abs_h1 * 0.45)
            + min(20, max(0, 1 - bs_h1) * 20)
            + min(20, vl_h1 * 5)
        )
    else:
        pump_coordination_score = _clamp_score(
            score * 0.40
            + min(30, max(0, bs_h1 - 1) * 18)
            + min(30, abs_h1 * 0.25)
        )
        dump_distribution_score = _clamp_score(
            min(35, max(0, vl_h24 - 20) * 1.2)
            + min(25, max(0, abs_h24 - 250) * 0.08)
            + (20 if reserve_usd and reserve_usd < 50000 else 0)
        )

    noise_score = 30
    red_flags = ["dex_only_signal", "safety_not_connected", "holders_not_connected"]

    if reserve_usd and reserve_usd < 25000:
        noise_score += 20
        red_flags.append("thin_liquidity")
    elif reserve_usd and reserve_usd < 75000:
        noise_score += 10
        red_flags.append("limited_liquidity")

    if vl_h24 >= 30 or vl_h1 >= 5:
        noise_score += 15
        red_flags.append("high_volume_liquidity_ratio")

    if abs_h1 >= 100 or abs_h24 >= 500:
        noise_score += 10
        red_flags.append("extreme_price_move")

    if (tx.get("h1_buys") or 0) + (tx.get("h1_sells") or 0) < 20:
        noise_score += 10
        red_flags.append("low_transaction_sample")

    if change_h24 >= 100 and change_h1 <= -5:
        red_flags.append("pump_then_reversal")

    noise_score = _clamp_score(noise_score)

    if direction == "dump":
        strong_dump_context = dump_distribution_score >= 55 or (score >= 60 and abs_h1 >= 15)
        distribution_context = (
            score >= 35
            and (
                abs_h1 >= 5
                or abs_h24 >= 25
                or (change_h24 >= 100 and change_h1 <= -5)
            )
        )

        if strong_dump_context:
            verdict = "Dump Risk"
            action = "watch_risk" if dump_distribution_score < 75 else "avoid"
            phase = "dump_risk_active"
        elif distribution_context:
            verdict = "Distribution"
            action = "watch_risk"
            phase = "sell_pressure"
        else:
            verdict = "Noise"
            action = "avoid"
            phase = "weak_dex_dump_noise"
    else:
        if manipulation_setup_score >= 75 and noise_score >= 55:
            verdict = "High-Risk Pump"
            action = "watch_high_risk"
        elif manipulation_setup_score >= 65:
            verdict = "Pump Watch"
            action = "watch"
        elif score >= 45:
            verdict = "Early DEX Watch"
            action = "watch"
        else:
            verdict = "Noise"
            action = "avoid"
        phase = "dex_breakout_active" if manipulation_setup_score >= 65 else "early_dex_setup"

    if abs(change_m5) >= 20 or abs_h1 >= 60:
        timing = "developing"
    elif candidate.get("mode") in {"new", "new_pools"}:
        timing = "early"
    elif abs_h24 >= 300:
        timing = "late"
    else:
        timing = "watch"

    solana_dex_context = {
        "available": False,
        "source": "GeckoTerminal Solana DEX context",
        "dex_family": None,
        "launch_context": None,
        "solana_meme_risk_flags": [],
    }

    if str(chain or "").lower() == "solana":
        try:
            solana_dex_context = build_solana_dex_context(candidate)
            for flag in solana_dex_context.get("solana_meme_risk_flags") or []:
                red_flags.append(flag)
        except Exception as exc:
            logger.warning("Failed Solana DEX context for %s: %s", symbol, exc)
            solana_dex_context = {
                "available": False,
                "source": "GeckoTerminal Solana DEX context",
                "dex_family": None,
                "launch_context": "unknown",
                "solana_meme_risk_flags": ["solana_dex_context_error"],
            }
            red_flags.append("solana_dex_context_error")

    solana_safety = {
        "available": False,
        "source": "GoPlus Solana",
        "solana_safety_status": "not_applicable",
        "safety_red_flags": [],
    }

    if str(chain or "").lower() == "solana" and candidate.get("token_address"):
        try:
            solana_safety = normalize_solana_goplus_safety(
                get_goplus_security("solana", candidate.get("token_address"))
            )
            for flag in solana_safety.get("safety_red_flags") or []:
                red_flags.append(flag)
        except Exception as exc:
            logger.warning("Failed Solana GoPlus safety for %s: %s", symbol, exc)
            solana_safety = {
                "available": False,
                "source": "GoPlus Solana",
                "solana_safety_status": "error",
                "safety_red_flags": ["solana_safety_error"],
            }
            red_flags.append("solana_safety_error")

    source_stack = candidate.get("source_stack_hint") or {
        "dex": ["GeckoTerminal", candidate.get("dex")],
        "market": ["GeckoTerminal"],
        "social": [],
        "reddit": [],
        "x": [],
        "safety": [],
        "holders": [],
    }

    if solana_dex_context.get("available"):
        dex_sources = list(source_stack.get("dex") or [])
        dex_sources.append("GeckoTerminal Solana DEX context")
        source_stack["dex"] = sorted(set(dex_sources))

    if solana_safety.get("available"):
        red_flags = [flag for flag in red_flags if flag != "safety_not_connected"]

        if solana_safety.get("holder_count") is not None or solana_safety.get("top_holder_percent") is not None:
            red_flags = [flag for flag in red_flags if flag != "holders_not_connected"]

        safety_sources = list(source_stack.get("safety") or [])
        safety_sources.append("GoPlus Solana")
        source_stack["safety"] = sorted(set(safety_sources))

        if solana_safety.get("holder_count") is not None or solana_safety.get("top_holder_percent") is not None:
            holder_sources = list(source_stack.get("holders") or [])
            holder_sources.append("GoPlus Solana")
            source_stack["holders"] = sorted(set(holder_sources))

    why_now = []
    if vl_h24:
        why_now.append(f"24h volume/liquidity ratio is {round(vl_h24, 2)}")
    if vl_h1:
        why_now.append(f"1h volume/liquidity ratio is {round(vl_h1, 2)}")
    if change_h1:
        why_now.append(f"1h price move is {round(change_h1, 2)}%")
    if change_h24:
        why_now.append(f"24h price move is {round(change_h24, 2)}%")
    if solana_dex_context.get("launch_context"):
        why_now.append(f"Solana DEX context: {solana_dex_context.get('launch_context')}")

    if candidate.get("candidate_origin"):
        why_now.append(f"Detected from {candidate.get('candidate_origin')}")

    if not why_now:
        why_now.append("Detected by GeckoTerminal DEX discovery layer")

    trigger_parts = ["GeckoTerminal DEX pool"]
    if candidate.get("dex"):
        trigger_parts.append(str(candidate.get("dex")))
    if vl_h24 >= 20 or vl_h1 >= 5:
        trigger_parts.append("high volume/liquidity ratio")
    if abs_h1 >= 25 or abs_h24 >= 100:
        trigger_parts.append("strong price movement")
    if solana_dex_context.get("launch_context"):
        trigger_parts.append(str(solana_dex_context.get("launch_context")))

    return {
        "schema_version": "signal_v2",
        "experimental": True,
        "source": "geckoterminal",
        "symbol": symbol,
        "name": name,
        "chain": chain,
        "contract_or_mint": candidate.get("token_address"),
        "pool_address": candidate.get("pool_address"),
        "pool_url": candidate.get("pool_url"),
        "dex": candidate.get("dex"),
        "direction": direction,
        "verdict": verdict,
        "action": action,
        "confidence": score,
        "manipulation_setup_score": manipulation_setup_score,
        "pump_coordination_score": pump_coordination_score,
        "dump_distribution_score": dump_distribution_score,
        "noise_score": noise_score,
        "social_coordination_score": 0,
        "whale_flow_score": 0,
        "smart_money_signal": "unknown",
        "phase": phase,
        "timing": timing,
        "trigger": " + ".join([part for part in trigger_parts if part]),
        "why_now": why_now[:6],
        "red_flags": sorted(set(red_flags)),
        "solana_dex_context": solana_dex_context,
        "solana_safety": solana_safety,
        "source_stack": source_stack,
        "tradeability": {
            "status": "dex_available",
            "primary_venue": candidate.get("dex"),
            "network": candidate.get("network"),
            "reserve_usd": reserve_usd,
            "volume_h1": volume_h1,
            "volume_h24": volume_h24,
            "buy_sell_ratio_h1": bs_h1,
            "buy_sell_ratio_h24": bs_h24,
        },
        "market_context": {
            "price_usd": candidate.get("price_usd"),
            "fdv_usd": candidate.get("fdv_usd"),
            "market_cap_usd": candidate.get("market_cap_usd"),
            "reserve_usd": reserve_usd,
            "volume_usd": volume,
            "price_change_pct": changes,
            "transactions": tx,
            "volume_liquidity_ratio_h1": vl_h1,
            "volume_liquidity_ratio_h24": vl_h24,
        },
        "source_layers": {
            "market": {
                "available": True,
                "sources": ["GeckoTerminal"],
                "status": "active",
            },
            "dex": {
                "available": True,
                "sources": sorted(set([x for x in ["GeckoTerminal", candidate.get("dex")] if x])),
                "status": "active",
            },
            "solana_dex_context": {
                "available": bool(solana_dex_context.get("available")),
                "source": solana_dex_context.get("source"),
                "status": "active" if solana_dex_context.get("available") else "inactive",
            },
            "safety": {
                "available": bool(solana_safety.get("available")),
                "sources": source_stack.get("safety") or [],
                "status": "active" if solana_safety.get("available") else "not_connected",
            },
            "holders": {
                "available": solana_safety.get("holder_count") is not None or solana_safety.get("top_holder_percent") is not None,
                "sources": source_stack.get("holders") or [],
                "status": "active" if (solana_safety.get("holder_count") is not None or solana_safety.get("top_holder_percent") is not None) else "not_connected",
            },
            "telegram": {
                "available": False,
                "sources": [],
                "status": "not_connected_for_geckoterminal_experimental",
            },
            "reddit": {
                "available": False,
                "sources": [],
                "status": "planned",
            },
            "x": {
                "available": False,
                "sources": [],
                "status": "planned_confirmation_layer",
            },
            "ai_judge": {
                "available": False,
                "provider": None,
                "model": None,
                "status": "fallback_local_only_for_geckoterminal_experimental",
            },
        },
        "social_layer": {
            "telegram": {
                "available": False,
                "mentions": 0,
                "sources": 0,
                "score": 0,
                "status": "not_connected_for_geckoterminal_experimental",
            },
            "reddit": {
                "available": False,
                "mentions": 0,
                "narrative_score": 0,
                "warning_score": 0,
                "status": "planned",
            },
            "x_confirmation": {
                "available": False,
                "x_confirmation_score": 0,
                "x_callers_count": 0,
                "x_large_accounts_count": 0,
                "x_mentions_velocity": 0,
                "x_copy_paste_ratio": 0,
                "x_warning_mentions": 0,
                "x_scam_mentions": 0,
                "status": "planned",
            },
        },
        "x_confirmation_layer": {
            "available": False,
            "provider": None,
            "intended_provider": "Apify or X API",
            "role": "confirmation_only",
            "can_override_safety": False,
            "status": "planned",
        },
        "ai_judge_layer": {
            "available": False,
            "provider": None,
            "model": None,
            "status": "local_rules_only",
            "fallback": "deterministic_geckoterminal_signal_v2",
        },
        "schema_readiness": {
            "dashboard_ready": False,
            "experimental_only": True,
            "missing_layers": [
                layer for layer, ok in {
                    "telegram": False,
                    "reddit": False,
                    "x_confirmation": False,
                    "ai_judge": False,
                    "safety": bool(solana_safety.get("available")),
                    "holders": solana_safety.get("holder_count") is not None or solana_safety.get("top_holder_percent") is not None,
                }.items() if not ok
            ],
            "promotion_status": "not_promoted_to_main_dashboard",
        },
        "explanation": (
            f"{symbol} is an experimental GeckoTerminal {direction} candidate. "
            "It is not yet promoted into the main dashboard signal lists because safety, holders and social confirmation are still experimental for this candidate."
        ),
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }

def fetch_geckoterminal_pool_candidates(network: str = "solana", mode: str = "trending", limit: int = 20) -> List[dict]:
    """Fetch GeckoTerminal trending/new pools as raw candidate discovery input.

    This is additive discovery only. It does not push candidates directly into dashboard signals yet.
    """
    network = (network or "solana").strip().lower()
    mode = (mode or "trending").strip().lower()

    if network in {"ethereum", "eth"}:
        gt_network = "eth"
        chain = "ethereum"
    elif network == "solana":
        gt_network = "solana"
        chain = "solana"
    else:
        gt_network = network
        chain = network

    endpoint = "new_pools" if mode in {"new", "new_pools"} else "trending_pools"

    try:
        resp = requests.get(
            f"https://api.geckoterminal.com/api/v2/networks/{gt_network}/{endpoint}",
            params={"include": "base_token,quote_token,dex"},
            headers={"accept": "application/json"},
            timeout=20,
        )
        if resp.status_code != 200:
            logger.warning("GeckoTerminal candidate discovery failed %s/%s status=%s", gt_network, endpoint, resp.status_code)
            return []

        payload = resp.json() or {}
        pools = payload.get("data") or []
        included = payload.get("included") or []

        token_index = {
            item.get("id"): item.get("attributes") or {}
            for item in included
            if item.get("type") == "token"
        }
        dex_index = {
            item.get("id"): item.get("attributes") or {}
            for item in included
            if item.get("type") == "dex"
        }

        candidates = []
        for pool in pools[: max(1, min(limit, 50))]:
            attrs = pool.get("attributes") or {}
            rel = pool.get("relationships") or {}

            base_rel = ((rel.get("base_token") or {}).get("data") or {}).get("id")
            quote_rel = ((rel.get("quote_token") or {}).get("data") or {}).get("id")
            dex_rel = ((rel.get("dex") or {}).get("data") or {}).get("id")

            base = token_index.get(base_rel, {})
            quote = token_index.get(quote_rel, {})
            dex = dex_index.get(dex_rel, {})

            volume = attrs.get("volume_usd") or {}
            changes = attrs.get("price_change_percentage") or {}
            tx = attrs.get("transactions") or {}

            h1_tx = tx.get("h1") or {}
            h24_tx = tx.get("h24") or {}

            buys_h1 = int(h1_tx.get("buys") or 0)
            sells_h1 = int(h1_tx.get("sells") or 0)
            buys_h24 = int(h24_tx.get("buys") or 0)
            sells_h24 = int(h24_tx.get("sells") or 0)

            reserve_usd = safe_float(attrs.get("reserve_in_usd")) or 0
            volume_h1 = safe_float(volume.get("h1")) or 0
            volume_h24 = safe_float(volume.get("h24")) or 0

            volume_liquidity_ratio_h1 = round(volume_h1 / reserve_usd, 4) if reserve_usd else None
            volume_liquidity_ratio_h24 = round(volume_h24 / reserve_usd, 4) if reserve_usd else None
            buy_sell_ratio_h1 = round(buys_h1 / max(sells_h1, 1), 4)
            buy_sell_ratio_h24 = round(buys_h24 / max(sells_h24, 1), 4)

            score = 0.0
            score += min(28, abs(safe_float(changes.get("h1")) or 0) * 1.4)
            score += min(22, abs(safe_float(changes.get("h24")) or 0) * 0.18)
            score += min(24, (volume_liquidity_ratio_h1 or 0) * 8)
            score += min(16, (volume_liquidity_ratio_h24 or 0) * 1.2)
            score += min(10, max(0, buy_sell_ratio_h1 - 1) * 10)
            score = int(max(0, min(100, round(score))))

            direction = "pump" if (safe_float(changes.get("h1")) or 0) >= 0 else "dump"

            candidates.append({
                "source": "GeckoTerminal",
                "candidate_origin": f"geckoterminal_{endpoint}",
                "network": gt_network,
                "chain": chain,
                "mode": mode,
                "pool_address": attrs.get("address"),
                "pool_url": f"https://www.geckoterminal.com/{gt_network}/pools/{attrs.get('address')}",
                "pool_name": attrs.get("name"),
                "pool_created_at": attrs.get("pool_created_at"),
                "dex": dex.get("name") or dex_rel,
                "token_address": base.get("address"),
                "symbol": (base.get("symbol") or "").upper(),
                "name": base.get("name"),
                "quote_symbol": quote.get("symbol"),
                "quote_address": quote.get("address"),
                "coingecko_coin_id": base.get("coingecko_coin_id"),
                "price_usd": safe_float(attrs.get("base_token_price_usd")),
                "fdv_usd": safe_float(attrs.get("fdv_usd")),
                "market_cap_usd": safe_float(attrs.get("market_cap_usd")),
                "reserve_usd": round(reserve_usd, 2),
                "volume_usd": {
                    "m5": safe_float(volume.get("m5")) or 0,
                    "m15": safe_float(volume.get("m15")) or 0,
                    "m30": safe_float(volume.get("m30")) or 0,
                    "h1": round(volume_h1, 2),
                    "h6": safe_float(volume.get("h6")) or 0,
                    "h24": round(volume_h24, 2),
                },
                "price_change_pct": {
                    "m5": safe_float(changes.get("m5")) or 0,
                    "m15": safe_float(changes.get("m15")) or 0,
                    "m30": safe_float(changes.get("m30")) or 0,
                    "h1": safe_float(changes.get("h1")) or 0,
                    "h6": safe_float(changes.get("h6")) or 0,
                    "h24": safe_float(changes.get("h24")) or 0,
                },
                "transactions": {
                    "h1_buys": buys_h1,
                    "h1_sells": sells_h1,
                    "h24_buys": buys_h24,
                    "h24_sells": sells_h24,
                    "h1_buy_sell_ratio": buy_sell_ratio_h1,
                    "h24_buy_sell_ratio": buy_sell_ratio_h24,
                },
                "volume_liquidity_ratio_h1": volume_liquidity_ratio_h1,
                "volume_liquidity_ratio_h24": volume_liquidity_ratio_h24,
                "direction_hint": direction,
                "dex_candidate_score": score,
                "source_stack_hint": {
                    "dex": ["GeckoTerminal", dex.get("name") or dex_rel],
                    "market": ["GeckoTerminal trending pools"],
                    "social": [],
                    "reddit": [],
                    "x": [],
                    "safety": [],
                    "holders": [],
                },
            })

        candidates.sort(key=lambda item: item.get("dex_candidate_score", 0), reverse=True)
        return candidates

    except Exception as exc:
        logger.warning("GeckoTerminal candidate discovery error network=%s mode=%s: %s", network, mode, exc)
        return []


def fetch_geckoterminal_token_pools(platform: Optional[str], contract_address: Optional[str]) -> List[dict]:
    if not platform or not contract_address:
        return []
    network = COINGECKO_NETWORK_MAP.get(platform)
    if not network:
        return []
    try:
        resp = requests.get(
            f"https://api.geckoterminal.com/api/v2/networks/{network}/tokens/{contract_address}/pools",
            params={"include": "dex", "page": 1},
            headers={"accept": "application/json"},
            timeout=20,
        )
        if resp.status_code != 200:
            return []
        payload = resp.json() or {}
        pools = payload.get("data") or []
        included = payload.get("included") or []
        dex_index = {
            item.get("id"): item.get("attributes") or {}
            for item in included
            if item.get("type") == "dex"
        }
        venue_map: Dict[tuple[str, str], dict] = {}
        for pool in pools:
            attrs = pool.get("attributes") or {}
            relationships = pool.get("relationships") or {}
            dex_rel = (relationships.get("dex") or {}).get("data") or {}
            dex_attrs = dex_index.get(dex_rel.get("id"), {})
            dex_name = dex_attrs.get("name") or attrs.get("dex_name") or attrs.get("name") or "Onchain venue"
            pool_address = attrs.get("address") or (pool.get("id") or "").split("_", 1)[-1]
            volume = safe_float((attrs.get("volume_usd") or {}).get("h24")) or safe_float(attrs.get("volume_usd")) or 0
            reserve_usd = safe_float(attrs.get("reserve_in_usd")) or 0
            pair_name = attrs.get("name") or f"{contract_address[:6]}..."
            venue = {
                "name": dex_name,
                "url": f"https://www.geckoterminal.com/{network}/pools/{pool_address}",
                "type": classify_trading_venue(dex_name),
                "pair": pair_name,
                "volume_usd": round(volume, 2),
                "trust_score": "onchain",
                "trust_score_numeric": 55,
                "spread_pct": None,
                "logo": dex_attrs.get("image_url") or build_exchange_logo_fallback(dex_name),
                "source": "GeckoTerminal pool",
                "reserve_usd": round(reserve_usd, 2),
            }
            dedupe_key = ((dex_name or "").strip().lower(), (pair_name or "").strip().upper())
            current = venue_map.get(dedupe_key)
            if not current or (venue.get("volume_usd", 0), venue.get("reserve_usd", 0)) > (current.get("volume_usd", 0), current.get("reserve_usd", 0)):
                venue_map[dedupe_key] = venue
        venues = list(venue_map.values())
        venues.sort(key=lambda item: (item.get("volume_usd", 0), item.get("reserve_usd", 0)), reverse=True)
        return venues[:8]
    except Exception as e:
        logger.error(f"GeckoTerminal pools error for {platform}:{contract_address}: {e}")
        return []

def build_market_venues(symbol: str, coin_id: str, platform: Optional[str] = None, contract_address: Optional[str] = None) -> List[dict]:
    venues: List[dict] = []
    seen = set()
    tickers = fetch_coin_tickers(coin_id)

    for ticker in tickers:
        market = ticker.get("market", {}) or {}
        market_identifier = market.get("identifier") or ""
        market_name = market.get("name") or market_identifier or "Unknown venue"
        market_meta = get_exchange_metadata(market_identifier) if market_identifier else {}
        base = ticker.get("base") or symbol
        target = ticker.get("target") or "USD"
        pair_label = build_route_pair_label(
            symbol=symbol,
            base=base,
            target=target,
            target_coin_id=ticker.get("target_coin_id"),
        )
        dedupe_key = (market_name.lower(), pair_label.upper())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        trade_url = ticker.get("trade_url") or market_meta.get("url") or f"https://www.coingecko.com/en/coins/{coin_id}"
        if not isinstance(trade_url, str) or not trade_url.startswith("http"):
            trade_url = f"https://www.coingecko.com/en/coins/{coin_id}"
        bid_ask_spread_pct = ticker.get("bid_ask_spread_percentage")
        volume_usd = round((ticker.get("converted_volume", {}) or {}).get("usd") or 0, 2)
        venues.append({
            "name": market_meta.get("name") or market_name,
            "url": trade_url,
            "type": classify_trading_venue(market_name),
            "pair": pair_label,
            "volume_usd": volume_usd,
            "trust_score": ticker.get("trust_score") or "unknown",
            "trust_score_numeric": score_trust_level(ticker.get("trust_score") or "unknown"),
            "spread_pct": round(float(bid_ask_spread_pct), 4) if bid_ask_spread_pct is not None else None,
            "logo": market.get("logo") or market_meta.get("image") or build_exchange_logo_fallback(market_meta.get("name") or market_name),
            "source": "CoinGecko ticker",
        })

    if venues:
        grouped_limits = {"cex": 6, "dex": 4, "swap": 4}
        final: List[dict] = []
        for venue_type in ("cex", "dex", "swap"):
            group = [v for v in venues if v["type"] == venue_type]
            group.sort(
                key=lambda item: (
                    item.get("volume_usd", 0),
                    item.get("trust_score_numeric", 0),
                    -((item.get("spread_pct") or 99)),
                ),
                reverse=True,
            )
            final.extend(group[:grouped_limits[venue_type]])
        if final:
            return final

    onchain_venues = fetch_geckoterminal_token_pools(platform, contract_address)
    if onchain_venues:
        return onchain_venues

    return []

def build_signal_execution_plan(
    *,
    signal_type: str,
    symbol: str,
    price: float,
    price_change_1h: float,
    price_change_24h: float,
    price_change_7d: float,
    volume_24h: float,
    market_cap: float,
    signal_strength: float,
    confidence: str,
    risk_level: str,
    venues: List[dict],
    direction_audit: Optional[dict] = None,
) -> dict:
    volume_ratio = (volume_24h / market_cap * 100) if market_cap else 0.0
    resolved_direction = (direction_audit or {}).get("resolved_direction", signal_type)
    transition_state = (direction_audit or {}).get("transition_state", "bullish_continuation" if resolved_direction == "pump" else "bearish_breakdown")
    absolute_move = max(abs(price_change_1h), abs(price_change_24h) / 6, abs(price_change_7d) / 20)
    volatility_pct = max(1.2, min(14.0, absolute_move * 1.35))
    best_venue = max(
        venues,
        key=lambda item: (
            item.get("trust_score_numeric", 0),
            item.get("volume_usd", 0),
            -(item.get("spread_pct") or 999),
        ),
        default=None,
    )
    venue_count = len(venues)
    average_spread = round(
        sum(v.get("spread_pct") or 0 for v in venues if v.get("spread_pct") is not None) /
        max(1, len([v for v in venues if v.get("spread_pct") is not None])),
        4,
    ) if any(v.get("spread_pct") is not None for v in venues) else None

    liquidity_score = round(min(100, volume_ratio * 2.2 + min(venue_count, 8) * 5 + min((best_venue or {}).get("volume_usd", 0) / 2_000_000, 30)))
    spread_score = 55 if average_spread is None else round(max(0, min(100, 100 - average_spread * 650)))
    venue_quality_score = round(min(100, ((best_venue or {}).get("trust_score_numeric", 45) * 0.55) + spread_score * 0.25 + min(venue_count * 4, 20)))
    execution_score = round(min(100, float(signal_strength or 0) * 0.45 + liquidity_score * 0.25 + venue_quality_score * 0.20 + spread_score * 0.10))

    entry_buffer_pct = max(0.4, min(3.0, volatility_pct * 0.18))
    stop_buffer_pct = max(1.0, min(6.5, volatility_pct * 0.42))
    target1_buffer_pct = max(1.2, min(8.0, stop_buffer_pct * 1.2))
    target2_buffer_pct = max(2.4, min(14.0, stop_buffer_pct * 2.0))

    if resolved_direction == "pump":
        entry_low = price * (1 - entry_buffer_pct / 100)
        entry_high = price * (1 + entry_buffer_pct / 100)
        stop_loss = price * (1 - stop_buffer_pct / 100)
        target_1 = price * (1 + target1_buffer_pct / 100)
        target_2 = price * (1 + target2_buffer_pct / 100)
        invalidation = (
            f"Pump thesis weakens if {symbol} loses {format_currency_compact(stop_loss)} "
            f"or if hourly volume fades materially while price stalls."
        )
        if transition_state == "bullish_pullback":
            setup_bias = "pullback continuation"
        elif transition_state == "bullish_reversal":
            setup_bias = "reversal attempt"
        else:
            setup_bias = "breakout continuation"
    else:
        entry_low = price * (1 - entry_buffer_pct / 100)
        entry_high = price * (1 + entry_buffer_pct / 100)
        stop_loss = price * (1 + stop_buffer_pct / 100)
        target_1 = price * (1 - target1_buffer_pct / 100)
        target_2 = price * (1 - target2_buffer_pct / 100)
        invalidation = (
            f"Dump thesis weakens if {symbol} reclaims {format_currency_compact(stop_loss)} "
            f"or if downside momentum fades despite elevated volume."
        )
        setup_bias = "failed-bounce continuation" if transition_state == "dead_cat_bounce" else "downside continuation"

    warning_flags: List[str] = []
    if volume_ratio < 8:
        warning_flags.append("Volume participation is still light relative to market cap.")
    if average_spread is not None and average_spread > 0.6:
        warning_flags.append("Venue spread is wide, so slippage risk is elevated.")
    if venue_count <= 2:
        warning_flags.append("The coin is trading on limited venues, which reduces execution flexibility.")
    if risk_level == "high":
        warning_flags.append("Model already flags this setup as high risk.")
    if abs(price_change_1h) > 7:
        warning_flags.append("Short-term move is stretched, so entry chasing is risky.")

    if execution_score >= 78:
        trade_readiness = "Ready"
    elif execution_score >= 60:
        trade_readiness = "Monitor Closely"
    else:
        trade_readiness = "Needs Confirmation"

    stop_distance = abs((price - stop_loss) / price) * 100 if price else 0
    target_distance = abs((target_1 - price) / price) * 100 if price else 0
    risk_reward = round(target_distance / stop_distance, 2) if stop_distance else 0

    return {
        "setup_bias": setup_bias,
        "trade_readiness": trade_readiness,
        "execution_score": execution_score,
        "liquidity_score": liquidity_score,
        "venue_quality_score": venue_quality_score,
        "spread_score": spread_score,
        "average_spread_pct": average_spread,
        "volume_market_cap_ratio": round(volume_ratio, 2),
        "venue_count": venue_count,
        "preferred_venue": best_venue,
        "entry_zone": {"low": round(entry_low, 8), "high": round(entry_high, 8)},
        "stop_loss": round(stop_loss, 8),
        "targets": [round(target_1, 8), round(target_2, 8)],
        "risk_reward": risk_reward,
        "invalidation": invalidation,
        "warning_flags": warning_flags,
        "position_sizing_note": (
            f"{str(confidence).capitalize()} confidence / {risk_level} risk setup. "
            f"Treat this as a {setup_bias} trade, not a blind market order."
        ),
    }

async def get_recent_telegram_signal_map(hours: int = 24) -> Dict[str, dict]:
    cache_key = f"telegram_signal_map::{hours}"
    cached = get_memory_cache(TELEGRAM_SIGNAL_MAP_CACHE, cache_key, ttl_seconds=3600)
    if cached is not None:
        return cached
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    signals = await db.telegram_signals.find({
        "posted_at": {"$gte": cutoff},
        "symbol": {"$exists": True, "$ne": None},
    }).to_list(length=2000)

    symbol_map: Dict[str, dict] = {}
    for item in signals:
        symbol = (item.get("symbol") or "").upper()
        if not symbol:
            continue
        bucket = symbol_map.setdefault(symbol, {
            "mentions": 0,
            "bullish_mentions": 0,
            "bearish_mentions": 0,
            "unique_sources": set(),
            "avg_score_total": 0.0,
            "last_posted_at": None,
        })
        bucket["mentions"] += 1
        if item.get("direction") == "pump":
            bucket["bullish_mentions"] += 1
        elif item.get("direction") == "dump":
            bucket["bearish_mentions"] += 1
        source_name = item.get("source_name")
        if source_name:
            bucket["unique_sources"].add(source_name)
        bucket["avg_score_total"] += float(item.get("composite_score") or 0)
        posted_at = normalize_datetime(item.get("posted_at"))
        if posted_at and (bucket["last_posted_at"] is None or posted_at > bucket["last_posted_at"]):
            bucket["last_posted_at"] = posted_at

    for symbol, bucket in symbol_map.items():
        mentions = bucket["mentions"] or 1
        bucket["avg_score"] = round(bucket["avg_score_total"] / mentions, 2)
        bucket["unique_sources"] = len(bucket["unique_sources"])
        last_posted = bucket.get("last_posted_at")
        bucket["last_posted_at"] = last_posted.isoformat() if isinstance(last_posted, datetime) else None
        bucket.pop("avg_score_total", None)

    return set_memory_cache(TELEGRAM_SIGNAL_MAP_CACHE, cache_key, symbol_map)

def build_telegram_early_signal_candidates(signals: List[dict], hours: int = TELEGRAM_EARLY_SIGNAL_HOURS, limit: int = TELEGRAM_EARLY_SIGNAL_LIMIT) -> List[dict]:
    grouped: Dict[str, dict] = {}
    allowed_quality_labels = {"real_trade_signal", "possible_trade_signal"}

    filtered_signals = []
    ignored_by_quality = 0
    for signal in signals:
        quality = signal.get("quality_judge") or {}
        if quality and quality.get("label") not in allowed_quality_labels:
            ignored_by_quality += 1
            continue
        if quality and quality.get("is_trade_signal") is False:
            ignored_by_quality += 1
            continue
        filtered_signals.append(signal)

    for signal in filtered_signals:
        symbol = (signal.get("symbol") or "").upper()
        if not symbol:
            continue
        item = grouped.setdefault(symbol, {
            "symbol": symbol,
            "coin_name": signal.get("coin_name") or symbol,
            "coin_id": signal.get("coin_id"),
            "direction_votes": {"pump": 0, "dump": 0},
            "mentions": 0,
            "source_names": set(),
            "scores": [],
            "parser_scores": [],
            "cross_source_max": 1,
            "latest_posted_at": None,
            "reference_price": None,
            "message_urls": [],
        })
        item["mentions"] += 1
        direction = signal.get("direction") or "pump"
        item["direction_votes"][direction] = item["direction_votes"].get(direction, 0) + 1
        if signal.get("source_name"):
            item["source_names"].add(signal["source_name"])
        if signal.get("composite_score") is not None:
            item["scores"].append(float(signal.get("composite_score") or 0))
        if signal.get("parser_confidence") is not None:
            item["parser_scores"].append(float(signal.get("parser_confidence") or 0))
        item["cross_source_max"] = max(item["cross_source_max"], int(signal.get("cross_source_count") or 1))
        posted_at = normalize_datetime(signal.get("posted_at"))
        if posted_at and (item["latest_posted_at"] is None or posted_at > item["latest_posted_at"]):
            item["latest_posted_at"] = posted_at
            item["reference_price"] = signal.get("reference_price")
            if signal.get("coin_name"):
                item["coin_name"] = signal.get("coin_name")
            if signal.get("coin_id"):
                item["coin_id"] = signal.get("coin_id")
        if signal.get("message_url"):
            item["message_urls"].append(signal["message_url"])

    early_signals: List[dict] = []
    for item in grouped.values():
        unique_sources = len(item["source_names"])
        avg_score = round(sum(item["scores"]) / len(item["scores"]), 2) if item["scores"] else 0.0
        parser_confidence_avg = round(sum(item["parser_scores"]) / len(item["parser_scores"]), 2) if item["parser_scores"] else 0.0
        bullish_mentions = int(item["direction_votes"].get("pump", 0))
        bearish_mentions = int(item["direction_votes"].get("dump", 0))
        direction = "dump" if bearish_mentions > bullish_mentions else "pump"
        promotion_reasons: List[str] = []
        if item["mentions"] >= 2:
            promotion_reasons.append("repeat_mentions")
        if unique_sources >= 2:
            promotion_reasons.append("cross_source_confirmation")
        if avg_score >= 60:
            promotion_reasons.append("high_composite_score")
        if item["cross_source_max"] >= 2:
            promotion_reasons.append("clustered_call_window")

        candidate_ready = bool(
            (item["mentions"] >= 2 and unique_sources >= 2) or
            (avg_score >= 60 and item["cross_source_max"] >= 2) or
            (item["mentions"] >= 3 and avg_score >= 52) or
            (item["mentions"] >= 2 and avg_score >= 55) or
            (unique_sources >= 1 and avg_score >= 70)
        )
        candidate_priority = round(
            item["mentions"] * 11 +
            unique_sources * 14 +
            avg_score * 0.65 +
            parser_confidence_avg * 0.15 +
            item["cross_source_max"] * 8,
            2,
        )

        early_signals.append({
            "symbol": item["symbol"],
            "coin_name": item["coin_name"],
            "coin_id": item["coin_id"],
            "direction": direction,
            "mentions": item["mentions"],
            "bullish_mentions": bullish_mentions,
            "bearish_mentions": bearish_mentions,
            "unique_sources": unique_sources,
            "avg_score": avg_score,
            "parser_confidence_avg": parser_confidence_avg,
            "cross_source_max": item["cross_source_max"],
            "candidate_ready": candidate_ready,
            "candidate_priority": candidate_priority,
            "promotion_reasons": promotion_reasons,
            "latest_posted_at": serialize_datetime(item["latest_posted_at"]),
            "reference_price": item["reference_price"],
            "source_names": sorted(item["source_names"]),
            "message_urls": item["message_urls"][:3],
            "window_hours": hours,
        })

    early_signals.sort(
        key=lambda item: (
            int(item["candidate_ready"]),
            item["candidate_priority"],
            item["avg_score"],
            item["mentions"],
        ),
        reverse=True,
    )
    return early_signals[:limit]

async def persist_telegram_pipeline_audit(records: List[dict], snapshot_at: datetime, trigger: str) -> None:
    if not records:
        return
    now = datetime.now(timezone.utc)
    try:
        await db.telegram_signal_pipeline_audit.insert_many([
            {
                **record,
                "trigger": trigger,
                "snapshot_at": snapshot_at,
                "created_at": now,
            }
            for record in records
        ])
    except Exception as e:
        logger.warning("Failed to persist Telegram pipeline audit records: %s", e)

async def build_telegram_calibration_summary(hours: int = 72) -> dict:
    hours = max(6, min(hours, 24 * 14))
    cache_key = f"telegram_calibration::{hours}"
    cached = get_memory_cache(TELEGRAM_CALIBRATION_CACHE, cache_key, ttl_seconds=180)
    if cached is not None:
        return cached

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    parser_rejections = await db.telegram_signal_rejections.find({
        "created_at": {"$gte": cutoff},
    }).sort("created_at", -1).limit(2000).to_list(length=2000)
    pipeline_records = await db.telegram_signal_pipeline_audit.find({
        "created_at": {"$gte": cutoff},
    }).sort("created_at", -1).limit(3000).to_list(length=3000)
    telegram_signals = await db.telegram_signals.find({
        "posted_at": {"$gte": cutoff},
    }).sort("posted_at", -1).limit(3000).to_list(length=3000)
    latest_snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])

    parser_reason_counts: Counter[str] = Counter()
    short_call_rejections = 0
    for item in parser_rejections:
        parser_reason_counts.update(item.get("reasons") or [])
        if item.get("short_call_detected"):
            short_call_rejections += 1

    pipeline_stage_counts: Counter[str] = Counter()
    pipeline_reason_counts: Counter[tuple[str, str]] = Counter()
    for item in pipeline_records:
        stage = item.get("stage") or "unknown"
        pipeline_stage_counts[stage] += 1
        for reason in (item.get("reasons") or ["unspecified"]):
            pipeline_reason_counts[(stage, reason)] += 1

    verified_4h = []
    verified_4h_cross_source = []
    source_rollup: Dict[str, dict] = {}
    for signal in telegram_signals:
        source_name = signal.get("source_name") or "Unknown source"
        bucket = source_rollup.setdefault(source_name, {
            "source_name": source_name,
            "count": 0,
            "composite_total": 0.0,
            "cross_source_total": 0.0,
            "verified_count": 0,
        })
        bucket["count"] += 1
        bucket["composite_total"] += float(signal.get("composite_score") or 0)
        bucket["cross_source_total"] += float(signal.get("cross_source_count") or 1)
        four_hour = ((signal.get("verification") or {}).get("four_hour") or {})
        if four_hour.get("checked_at") is not None:
            hit = bool(four_hour.get("hit"))
            verified_4h.append(hit)
            bucket["verified_count"] += 1
            if int(signal.get("cross_source_count") or 1) >= 2:
                verified_4h_cross_source.append(hit)

    source_leaders = sorted(
        [
            {
                "source_name": source_name,
                "signal_count": bucket["count"],
                "avg_composite_score": round(bucket["composite_total"] / bucket["count"], 2) if bucket["count"] else 0.0,
                "avg_cross_source_count": round(bucket["cross_source_total"] / bucket["count"], 2) if bucket["count"] else 0.0,
                "verified_count": bucket["verified_count"],
            }
            for source_name, bucket in source_rollup.items()
        ],
        key=lambda item: (
            item["signal_count"],
            item["avg_composite_score"],
            item["verified_count"],
        ),
        reverse=True,
    )[:5]

    latest_snapshot_signals = ((latest_snapshot or {}).get("pump_signals", []) or []) + ((latest_snapshot or {}).get("dump_signals", []) or [])
    latest_snapshot_telegram_confirmed = [
        signal for signal in latest_snapshot_signals
        if str(signal.get("candidate_origin") or "").startswith("telegram") or (signal.get("telegram_early_seed") is not None)
    ]

    overall_4h_hit_rate = round((sum(1 for item in verified_4h if item) / len(verified_4h)) * 100, 2) if verified_4h else 0.0
    cross_source_4h_hit_rate = round((sum(1 for item in verified_4h_cross_source if item) / len(verified_4h_cross_source)) * 100, 2) if verified_4h_cross_source else 0.0

    top_parser_rejections = [
        {
            "reason": reason,
            "count": count,
            "share_pct": round((count / max(1, len(parser_rejections))) * 100, 2),
        }
        for reason, count in parser_reason_counts.most_common(5)
    ]
    top_pipeline_rejections = [
        {
            "stage": stage,
            "reason": reason,
            "count": count,
            "share_pct": round((count / max(1, len(pipeline_records))) * 100, 2),
        }
        for (stage, reason), count in pipeline_reason_counts.most_common(7)
    ]

    recommendations: List[dict] = []

    missing_structure = parser_reason_counts.get("missing_contract_or_structured_plan", 0)
    if missing_structure >= 8:
        recommendations.append({
            "priority": "high",
            "title": "Short-call parser still drops a meaningful share of messages",
            "detail": (
                f"{missing_structure} parser rejects in the last {hours}h still failed on missing contract/plan structure. "
                f"{short_call_rejections} of total rejects were short-call style messages."
            ),
            "suggested_action": "Keep early-signal mode permissive and consider adding channel-specific parser patterns for the noisiest formats.",
        })

    market_lookup_misses = pipeline_stage_counts.get("market_data_lookup", 0)
    if market_lookup_misses >= 5:
        recommendations.append({
            "priority": "high",
            "title": "Telegram seeds are being lost at market lookup",
            "detail": f"{market_lookup_misses} promoted Telegram symbols in the last {hours}h could not be resolved into market data.",
            "suggested_action": "Improve symbol-to-coin resolution, contract-based lookup, and fallback handling for newly launched tokens.",
        })

    source_stack_thin = sum(
        count for (stage, reason), count in pipeline_reason_counts.items()
        if stage == "snapshot_gate" and reason in {"source_stack_too_thin_for_pump", "source_stack_too_thin_for_dump"}
    )
    if source_stack_thin >= 5:
        recommendations.append({
            "priority": "medium",
            "title": "Snapshot gate still removes many Telegram-led setups",
            "detail": f"{source_stack_thin} recent snapshot rejects failed mainly because the source stack stayed too thin after enrichment.",
            "suggested_action": "Consider keeping Telegram-led symbols visible longer as early signals even when they miss full snapshot confirmation.",
        })

    if verified_4h_cross_source and cross_source_4h_hit_rate >= overall_4h_hit_rate + 8:
        recommendations.append({
            "priority": "medium",
            "title": "Cross-source Telegram clusters are outperforming the baseline",
            "detail": (
                f"4h hit rate is {cross_source_4h_hit_rate:.2f}% for signals with cross-source count >= 2, "
                f"vs {overall_4h_hit_rate:.2f}% overall."
            ),
            "suggested_action": "Increase ranking weight for clustered Telegram calls rather than lowering all thresholds globally.",
        })

    if not recommendations:
        recommendations.append({
            "priority": "low",
            "title": "No dominant failure mode yet",
            "detail": f"Recent Telegram flow is distributed across parser, lookup, and snapshot stages over the last {hours}h.",
            "suggested_action": "Collect more audit data before moving thresholds again.",
        })

    summary = {
        "hours": hours,
        "generated_at": serialize_datetime(now),
        "totals": {
            "stored_signals": len(telegram_signals),
            "parser_rejections": len(parser_rejections),
            "pipeline_rejections": len(pipeline_records),
            "short_call_rejections": short_call_rejections,
            "promoted_candidates": pipeline_stage_counts.get("telegram_promoted_candidate", 0) + pipeline_stage_counts.get("telegram_seed_attached", 0),
            "snapshot_rejections": pipeline_stage_counts.get("snapshot_gate", 0),
            "latest_snapshot_telegram_confirmed": len(latest_snapshot_telegram_confirmed),
            "verified_4h_samples": len(verified_4h),
        },
        "hit_rates": {
            "overall_4h_pct": overall_4h_hit_rate,
            "cross_source_4h_pct": cross_source_4h_hit_rate,
        },
        "top_parser_rejections": top_parser_rejections,
        "top_pipeline_rejections": top_pipeline_rejections,
        "source_leaders": source_leaders,
        "recommendations": recommendations[:4],
    }
    return set_memory_cache(TELEGRAM_CALIBRATION_CACHE, cache_key, summary)

def build_manipulation_profile(
    *,
    signal_type: str,
    symbol: str,
    price_change_1h: float,
    price_change_24h: float,
    price_change_7d: float,
    volume_24h: float,
    market_cap: float,
    signal_strength: float,
    risk_level: str,
    is_trending: bool,
    social_volume: float,
    sentiment: float,
    galaxy_score: float,
    decision_engine: Optional[dict],
    telegram_stats: Optional[dict] = None,
    derivatives_data: Optional[dict] = None,
    tokenomics: Optional[dict] = None,
    wallet_concentration: Optional[dict] = None,
    contract_risk: Optional[dict] = None,
    direction_audit: Optional[dict] = None,
) -> dict:
    resolved_direction = (direction_audit or {}).get("resolved_direction", signal_type)
    transition_state = (direction_audit or {}).get("transition_state", "bullish_continuation" if resolved_direction == "pump" else "bearish_breakdown")
    telegram_stats = telegram_stats or {}
    volume_ratio = (volume_24h / market_cap * 100) if market_cap else float((decision_engine or {}).get("volume_market_cap_ratio") or 0)
    venue_count = int((decision_engine or {}).get("venue_count") or 0)
    spread = (decision_engine or {}).get("average_spread_pct")
    mentions = int(telegram_stats.get("mentions") or 0)
    unique_sources = int(telegram_stats.get("unique_sources") or 0)
    bullish_mentions = int(telegram_stats.get("bullish_mentions") or 0)
    bearish_mentions = int(telegram_stats.get("bearish_mentions") or 0)

    social_burst_score = round(min(100, social_volume / 18 + galaxy_score * 0.45 + max(sentiment, 0) * 0.2 + (18 if is_trending else 0)))
    coordination_score = round(min(100, mentions * 11 + unique_sources * 13 + min(telegram_stats.get("avg_score", 0) * 0.25, 20)))
    liquidity_trap_score = round(min(100, max(0, 30 - venue_count * 5) + min(volume_ratio * 1.3, 35) + (18 if spread and spread > 0.7 else 0)))
    early_entry_score = round(max(0, min(100, signal_strength * 0.45 + max(0, 18 - abs(price_change_24h)) * 2 + max(0, 10 - abs(price_change_1h) * 2.2))))

    funding_rate = (derivatives_data or {}).get("funding_rate_pct")
    open_interest = (derivatives_data or {}).get("open_interest_usd")
    top_10_pct = (wallet_concentration or {}).get("top_10_pct")
    owner_pct = (wallet_concentration or {}).get("owner_pct")
    dilution_gap = (tokenomics or {}).get("dilution_gap_pct")
    contract_risk_score = safe_float((contract_risk or {}).get("risk_score"))
    contract_risk_level = ((contract_risk or {}).get("risk_level") or "").lower()
    contract_risk_penalty = 0
    if contract_risk_level == "high" or (contract_risk_score is not None and contract_risk_score <= 45):
        contract_risk_penalty = 10
    elif contract_risk_level == "medium" or (contract_risk_score is not None and contract_risk_score <= 65):
        contract_risk_penalty = 5

    manipulation_score = round(min(
        100,
        signal_strength * 0.28 +
        social_burst_score * 0.18 +
        coordination_score * 0.18 +
        liquidity_trap_score * 0.16 +
        (18 if is_trending else 0) +
        min(volume_ratio, 30) * 0.35
    ))

    if resolved_direction == "pump":
        upside_24h = max(0.0, float(price_change_24h or 0))
        upside_1h = max(0.0, float(price_change_1h or 0))
        pump_extension_score = min(
            28,
            max(0, min(upside_24h, 35) - 20) * 0.35 +
            max(0, min(upside_24h, 90) - 35) * 0.10 +
            max(0, upside_1h - 6) * 0.9 +
            (4 if upside_24h >= 80 and upside_1h >= 12 else 0)
        )
        venue_penalty = 6 if venue_count == 0 else 6 if venue_count <= 2 else 2 if venue_count == 3 else 0
        trend_penalty = 2 if is_trending and upside_24h >= 24 else 0
        coordination_penalty = 2 if coordination_score >= 55 and social_burst_score >= 50 else 0
        open_interest_penalty = 3 if open_interest and open_interest >= 10_000_000 else 0
        derivatives_penalty = 3 if funding_rate is not None and abs(funding_rate) >= 0.03 else 0
        weak_participation_penalty = 8 if volume_ratio < 6 and upside_24h >= 70 else 4 if volume_ratio < 10 and upside_24h >= 40 else 0
        structure_relief = 0
        if signal_strength >= 70 and volume_ratio >= 12:
            structure_relief += 5
        if venue_count >= 4:
            structure_relief += 4
        if spread is not None and spread <= 0.35:
            structure_relief += 3

        dump_risk_score = round(min(
            100,
            6 +
            pump_extension_score +
            (8 if risk_level == "high" else 4 if risk_level == "medium" else 0) +
            venue_penalty +
            weak_participation_penalty +
            (8 if spread and spread > 0.8 else 0) +
            (8 if dilution_gap and dilution_gap >= 40 else 0) +
            (8 if top_10_pct and top_10_pct >= 45 else 0) +
            (6 if owner_pct and owner_pct >= 5 else 0) +
            contract_risk_penalty +
            derivatives_penalty +
            open_interest_penalty +
            trend_penalty +
            coordination_penalty -
            structure_relief
        ))
    else:
        downside_24h = abs(min(0.0, float(price_change_24h or 0)))
        downside_1h = abs(min(0.0, float(price_change_1h or 0)))
        dump_extension_score = min(
            58,
            max(0, downside_24h - 10) * 1.15 +
            max(0, downside_1h - 3) * 2.2 +
            (8 if downside_24h >= 20 and downside_1h >= 5 else 0) +
            (8 if downside_24h >= 35 and downside_1h >= 8 else 0)
        )
        dump_risk_score = round(min(
            100,
            28 +
            dump_extension_score +
            (12 if risk_level == "high" else 6 if risk_level == "medium" else 0) +
            (12 if venue_count <= 2 else 0) +
            (10 if spread and spread > 0.8 else 0) +
            (10 if dilution_gap and dilution_gap >= 40 else 0) +
            (10 if top_10_pct and top_10_pct >= 45 else 0) +
            (8 if owner_pct and owner_pct >= 5 else 0) +
            contract_risk_penalty +
            (8 if funding_rate is not None and abs(funding_rate) >= 0.03 else 0) +
            (8 if open_interest and open_interest >= 10_000_000 else 0)
        ))

    if resolved_direction == "pump":
        if transition_state == "bullish_reversal":
            stage = "reversal attempt"
        elif transition_state == "bullish_pullback":
            stage = "pullback continuation"
        elif dump_risk_score >= 76 and ((max(price_change_24h or 0, 0) >= 120 and max(price_change_1h or 0, 0) >= 15) or (max(price_change_24h or 0, 0) >= 80 and max(price_change_1h or 0, 0) >= 20)):
            stage = "blow-off risk"
        elif coordination_score >= 45 and social_burst_score >= 45:
            stage = "coordinated hype"
        elif early_entry_score >= 65:
            stage = "stealth build"
        elif dump_risk_score >= 52 and signal_strength >= 70:
            stage = "extended breakout"
        else:
            stage = "breakout active"
    else:
        if transition_state == "dead_cat_bounce":
            stage = "countertrend bounce"
        elif dump_risk_score >= 78:
            stage = "unwind active"
        elif coordination_score >= 40:
            stage = "coordinated unwind"
        else:
            stage = "breakdown pressure"

    bullish_only_stages = {"breakout active", "stealth build", "coordinated hype", "extended breakout", "pullback continuation", "reversal attempt", "blow-off risk"}
    bearish_only_stages = {"breakdown pressure", "coordinated unwind", "unwind active", "countertrend bounce"}
    if resolved_direction == "pump" and stage in bearish_only_stages:
        logger.warning("Directional guardrail corrected %s stage from %s to breakout active for pump structure", symbol, stage)
        stage = "breakout active"
    elif resolved_direction == "dump" and stage in bullish_only_stages:
        logger.warning("Directional guardrail corrected %s stage from %s to breakdown pressure for dump structure", symbol, stage)
        stage = "breakdown pressure"

    warning_flags: List[str] = []
    if unique_sources >= 3:
        warning_flags.append(f"{symbol} is being pushed by {unique_sources} Telegram sources in the same 24h window.")
    if social_burst_score >= 65:
        warning_flags.append("Social velocity is elevated, which is typical in coordinated meme runs.")
    if venue_count <= 2:
        warning_flags.append("Few execution venues means exits can get crowded quickly.")
    if top_10_pct and top_10_pct >= 45:
        warning_flags.append("Top wallet concentration is high, so a fast unwind can hit hard.")
    if dilution_gap and dilution_gap >= 40:
        warning_flags.append("Large non-circulating supply increases future sell-pressure risk.")
    if contract_risk_penalty >= 8:
        warning_flags.append("Contract risk is elevated, which weakens trust in the move.")

    risk_term = "reversal risk" if resolved_direction == "pump" else "dump risk"
    summary = (
        f"{symbol} is currently in {stage}: manipulation score {manipulation_score}/100, "
        f"coordination {coordination_score}/100, social burst {social_burst_score}/100, "
        f"and {risk_term} {dump_risk_score}/100."
    )
    if mentions:
        summary += f" Telegram recorded {mentions} relevant mentions across {unique_sources} watched source{'s' if unique_sources != 1 else ''}."

    return {
        "manipulation_score": manipulation_score,
        "coordinated_hype_score": coordination_score,
        "social_burst_score": social_burst_score,
        "liquidity_trap_score": liquidity_trap_score,
        "early_entry_score": early_entry_score,
        "dump_risk_score": dump_risk_score,
        "risk_metric_label": "Reversal Risk" if resolved_direction == "pump" else "Dump Risk",
        "stage": stage,
        "resolved_direction": resolved_direction,
        "transition_state": transition_state,
        "telegram_mentions": mentions,
        "telegram_sources": unique_sources,
        "bullish_mentions": bullish_mentions,
        "bearish_mentions": bearish_mentions,
        "volume_market_cap_ratio": round(volume_ratio, 2),
        "warning_flags": warning_flags,
        "summary": summary,
    }

def build_manipulation_timeline(
    *,
    symbol: str,
    signal_type: str,
    manipulation_profile: dict,
    decision_engine: Optional[dict],
    fear_greed: Optional[dict],
    is_trending: bool,
    social_volume: float,
    galaxy_score: float,
) -> List[dict]:
    events: List[dict] = []
    resolved_direction = manipulation_profile.get("resolved_direction", signal_type)
    mentions = manipulation_profile.get("telegram_mentions", 0)
    sources = manipulation_profile.get("telegram_sources", 0)
    volume_ratio = manipulation_profile.get("volume_market_cap_ratio", 0)
    stage = manipulation_profile.get("stage", "active")
    dump_risk = manipulation_profile.get("dump_risk_score", 0)
    readiness = (decision_engine or {}).get("trade_readiness", "Needs Confirmation")

    if mentions:
        events.append({
            "phase": "Telegram signal",
            "status": "active",
            "tone": "sky",
            "detail": f"{mentions} structured mentions from {sources} watched source{'s' if sources != 1 else ''}.",
        })
    else:
        events.append({
            "phase": "Telegram signal",
            "status": "quiet",
            "tone": "slate",
            "detail": "No strong coordinated Telegram push detected in the recent tracked window.",
        })

    if social_volume or galaxy_score:
        events.append({
            "phase": "Social burst",
            "status": "active" if manipulation_profile.get("social_burst_score", 0) >= 55 else "forming",
            "tone": "violet",
            "detail": f"Social volume {int(social_volume or 0)} with Galaxy Score {int(galaxy_score or 0)}.",
        })

    events.append({
        "phase": "Volume anomaly",
        "status": "active" if volume_ratio >= 15 else "forming",
        "tone": "emerald" if resolved_direction == "pump" else "rose",
        "detail": f"Volume/market-cap ratio is {volume_ratio:.2f}%, which is {'abnormal' if volume_ratio >= 15 else 'building'} for this setup.",
    })

    events.append({
        "phase": "Setup state",
        "status": "active",
        "tone": "amber" if "risk" in stage or "unwind" in stage else "cyan",
        "detail": f"Current stage: {stage}. Execution readiness: {readiness}.",
    })

    if is_trending:
        events.append({
            "phase": "Crowd discovery",
            "status": "active",
            "tone": "amber",
            "detail": f"{symbol} is also appearing in trending discovery lists, which usually pulls in late retail flow.",
        })

    if fear_greed:
        events.append({
            "phase": "Market backdrop",
            "status": "context",
            "tone": "indigo",
            "detail": f"Fear & Greed is {fear_greed.get('value', 50)}/100 ({fear_greed.get('classification', 'Neutral')}).",
        })

    risk_metric_label = manipulation_profile.get("risk_metric_label", "Reversal Risk" if resolved_direction == "pump" else "Dump Risk")
    events.append({
        "phase": risk_metric_label,
        "status": "watch",
        "tone": "red" if dump_risk >= 70 else "orange" if dump_risk >= 50 else "slate",
        "detail": f"Estimated fast-unwind risk is {dump_risk}/100. This is where late entries usually get trapped.",
    })

    return events[:6]

def build_intelligence_alerts(snapshot: Optional[dict], telegram_consensus_payload: Optional[dict] = None) -> List[dict]:
    if not snapshot:
        return []

    alerts: List[dict] = []
    pump_signals = snapshot.get("pump_signals", []) or []
    dump_signals = snapshot.get("dump_signals", []) or []

    for signal in pump_signals[:6]:
        profile = signal.get("manipulation_profile") or {}
        if (profile.get("coordinated_hype_score") or 0) >= 55:
            alerts.append({
                "type": "coordinated_hype",
                "category": "coordination",
                "severity": "high",
                "severity_score": int(profile.get("coordinated_hype_score") or 0),
                "symbol": signal.get("symbol"),
                "signal_type": signal.get("signal_type", "pump"),
                "title": f"{signal.get('symbol')} is showing coordinated hype",
                "detail": f"{profile.get('telegram_mentions', 0)} mentions, {profile.get('telegram_sources', 0)} watched sources, manipulation score {profile.get('manipulation_score', 0)}/100.",
                "action": "Watch for crowding and do not chase thin liquidity.",
                "evidence": [
                    f"Coordination {profile.get('coordinated_hype_score', 0)}/100",
                    f"Telegram mentions {profile.get('telegram_mentions', 0)}",
                ],
            })
        if (profile.get("early_entry_score") or 0) >= 72:
            alerts.append({
                "type": "early_setup",
                "category": "early",
                "severity": "medium",
                "severity_score": int(profile.get("early_entry_score") or 0),
                "symbol": signal.get("symbol"),
                "signal_type": signal.get("signal_type", "pump"),
                "title": f"{signal.get('symbol')} still looks early",
                "detail": f"Early-entry score is {profile.get('early_entry_score', 0)}/100 with stage {profile.get('stage', 'forming')}.",
                "action": "Treat this as a build phase and wait for volume confirmation.",
                "evidence": [
                    f"Early entry {profile.get('early_entry_score', 0)}/100",
                    f"Stage {profile.get('stage', 'forming')}",
                ],
            })
        stage = (profile.get("stage") or "").lower()
        if stage == "blow-off risk" or (profile.get("dump_risk_score") or 0) >= 88:
            alerts.append({
                "type": "exit_risk",
                "category": "exit",
                "severity": "high",
                "severity_score": int(profile.get("dump_risk_score") or 0),
                "symbol": signal.get("symbol"),
                "signal_type": signal.get("signal_type", "pump"),
                "title": f"{signal.get('symbol')} is entering blow-off territory",
                "detail": f"Exit risk is {profile.get('dump_risk_score', 0)}/100. Momentum is stretched enough that a sharp reversal can hit late entries fast.",
                "action": "Reduce chase risk and tighten invalidation immediately.",
                "evidence": [
                    f"Dump risk {profile.get('dump_risk_score', 0)}/100",
                    f"Stage {profile.get('stage', 'blow-off risk')}",
                ],
            })
        elif stage == "extended breakout" or (profile.get("dump_risk_score") or 0) >= 72:
            alerts.append({
                "type": "late_entry_risk",
                "category": "crowding",
                "severity": "medium",
                "severity_score": int(profile.get("dump_risk_score") or 0),
                "symbol": signal.get("symbol"),
                "signal_type": signal.get("signal_type", "pump"),
                "title": f"{signal.get('symbol')} is becoming crowded",
                "detail": f"Stage is {profile.get('stage', 'extended breakout')} with exit risk {profile.get('dump_risk_score', 0)}/100. Treat this like a continuation setup, not a fresh breakout.",
                "action": "Only engage on disciplined pullbacks or with smaller size.",
                "evidence": [
                    f"Dump risk {profile.get('dump_risk_score', 0)}/100",
                    f"Manipulation {profile.get('manipulation_score', 0)}/100",
                ],
            })

    for signal in dump_signals[:4]:
        profile = signal.get("manipulation_profile") or {}
        if (profile.get("dump_risk_score") or 0) >= 72:
            alerts.append({
                "type": "breakdown",
                "category": "breakdown",
                "severity": "high",
                "severity_score": int(profile.get("dump_risk_score") or 0),
                "symbol": signal.get("symbol"),
                "signal_type": signal.get("signal_type", "dump"),
                "title": f"{signal.get('symbol')} is already in unwind mode",
                "detail": f"Stage is {profile.get('stage', 'breakdown')} with dump risk {profile.get('dump_risk_score', 0)}/100.",
                "action": "Do not fade without a fresh reversal signal.",
                "evidence": [
                    f"Stage {profile.get('stage', 'breakdown')}",
                    f"Dump risk {profile.get('dump_risk_score', 0)}/100",
                ],
            })

    for symbol in (telegram_consensus_payload or {}).get("hot_symbols", [])[:3]:
        if symbol.get("unique_sources", 0) >= 3 and symbol.get("mentions", 0) >= 4:
            alerts.append({
                "type": "telegram_consensus",
                "category": "rumor",
                "severity": "medium",
                "severity_score": int(symbol.get("avg_score", 0) or 0),
                "symbol": symbol.get("symbol"),
                "signal_type": "pump" if symbol.get("stance") == "bullish" else "dump" if symbol.get("stance") == "bearish" else "pump",
                "title": f"{symbol.get('symbol')} is spreading across Telegram",
                "detail": f"{symbol.get('mentions', 0)} mentions across {symbol.get('unique_sources', 0)} sources with {symbol.get('stance', 'mixed')} bias.",
                "action": "Treat this as rumor flow until price and liquidity confirm it.",
                "evidence": [
                    f"Mentions {symbol.get('mentions', 0)}",
                    f"Sources {symbol.get('unique_sources', 0)}",
                ],
            })

    severity_rank = {"high": 3, "medium": 2, "low": 1}
    alerts.sort(key=lambda item: severity_rank.get(item.get("severity", "low"), 1), reverse=True)
    return alerts[:8]

async def build_coin_case_replay(symbol: str, limit: int = 10) -> List[dict]:
    cache_key = f"{symbol.upper()}::{limit}"
    cached = get_memory_cache(CASE_REPLAY_CACHE, cache_key, ttl_seconds=180)
    if cached is not None:
        return cached
    snapshots = await db.signal_snapshots.find({
        "$or": [
            {"pump_signals.symbol": symbol},
            {"dump_signals.symbol": symbol},
        ]
    }).sort("timestamp", -1).limit(limit).to_list(length=limit)

    replay: List[dict] = []
    for snap in snapshots:
        timestamp = serialize_datetime(snap.get("timestamp"))
        signals = (snap.get("pump_signals", []) or []) + (snap.get("dump_signals", []) or [])
        for signal in signals:
            if (signal.get("symbol") or "").upper() != symbol:
                continue
            profile = signal.get("manipulation_profile") or {}
            direction_audit = signal.get("direction_audit") or build_direction_audit(
                symbol=(signal.get("symbol") or "").upper(),
                price_change_1h=signal.get("price_change_1h") or 0,
                price_change_24h=signal.get("price_change_24h") or 0,
                price_change_7d=signal.get("price_change_7d") or 0,
                volume_24h=signal.get("volume_24h") or 0,
                market_cap=signal.get("market_cap") or 0,
                signal_type_hint=signal.get("signal_type"),
                signal_strength_hint=signal.get("signal_strength") or 0,
                pump_strength=signal.get("signal_strength") or 0 if signal.get("signal_type") == "pump" else 0,
                dump_strength=signal.get("signal_strength") or 0 if signal.get("signal_type") == "dump" else 0,
                is_trending=bool(signal.get("is_trending")),
            )
            resolved_signal_type = direction_audit.get("resolved_direction") or signal.get("signal_type")
            normalized_stage = normalize_stage_for_direction(
                profile.get("stage", "active"),
                resolved_signal_type,
                direction_audit.get("transition_state"),
            )
            replay.append({
                "timestamp": timestamp,
                "signal_type": resolved_signal_type,
                "stored_signal_type": signal.get("signal_type"),
                "signal_strength": signal.get("signal_strength", 0),
                "price_change_1h": signal.get("price_change_1h", 0),
                "price_change_24h": signal.get("price_change_24h", 0),
                "stage": normalized_stage,
                "manipulation_score": profile.get("manipulation_score", 0),
                "dump_risk_score": profile.get("dump_risk_score", 0),
                "telegram_mentions": profile.get("telegram_mentions", 0),
                "direction_audit": direction_audit,
                "summary": profile.get("summary") or signal.get("reason", ""),
            })
            break

    replay.sort(key=lambda item: item.get("timestamp") or "", reverse=True)
    return set_memory_cache(CASE_REPLAY_CACHE, cache_key, replay)

async def build_recent_case_replays(limit: int = 40) -> List[dict]:
    snapshots = await db.signal_snapshots.find({}).sort("timestamp", -1).limit(max(limit, 12)).to_list(length=max(limit, 12))
    replay: List[dict] = []

    for snap in snapshots:
        timestamp = serialize_datetime(snap.get("timestamp"))
        signals = (snap.get("pump_signals", []) or []) + (snap.get("dump_signals", []) or [])
        for signal in signals:
            profile = signal.get("manipulation_profile") or {}
            direction_audit = signal.get("direction_audit") or build_direction_audit(
                symbol=(signal.get("symbol") or "").upper(),
                price_change_1h=signal.get("price_change_1h") or 0,
                price_change_24h=signal.get("price_change_24h") or 0,
                price_change_7d=signal.get("price_change_7d") or 0,
                volume_24h=signal.get("volume_24h") or 0,
                market_cap=signal.get("market_cap") or 0,
                signal_type_hint=signal.get("signal_type"),
                signal_strength_hint=signal.get("signal_strength") or 0,
                pump_strength=signal.get("signal_strength") or 0 if signal.get("signal_type") == "pump" else 0,
                dump_strength=signal.get("signal_strength") or 0 if signal.get("signal_type") == "dump" else 0,
                is_trending=bool(signal.get("is_trending")),
            )
            if not signal.get("symbol"):
                continue
            signal_type = direction_audit.get("resolved_direction") or signal.get("signal_type", "pump")
            stage = normalize_stage_for_direction(
                profile.get("stage", "active"),
                signal_type,
                direction_audit.get("transition_state"),
            )
            early_score = int(profile.get("early_entry_score") or 0)
            manipulation_score = int(profile.get("manipulation_score") or 0)
            dump_risk_score = int(profile.get("dump_risk_score") or 0)
            telegram_mentions = int(profile.get("telegram_mentions") or 0)

            if signal_type == "pump":
                if stage == "blow-off risk" or dump_risk_score >= 88:
                    replay_type = "Exhaustion Risk"
                    action = "Avoid late entries and tighten exits immediately."
                elif stage == "extended breakout" or dump_risk_score >= 72:
                    replay_type = "Crowded Continuation"
                    action = "Treat this as a continuation trade, not a fresh breakout."
                elif early_score >= 72:
                    replay_type = "Early Build"
                    action = "Wait for volume confirmation before sizing up."
                else:
                    replay_type = "Coordinated Push"
                    action = "Track whether price and liquidity keep confirming the push."
            else:
                if dump_risk_score >= 80:
                    replay_type = "Breakdown Active"
                    action = "Do not fade the unwind without reversal evidence."
                else:
                    replay_type = "Distribution Pressure"
                    action = "Watch whether the unwind broadens or fades."

            evidence = [
                f"Signal {int(signal.get('signal_strength', 0) or 0)}/100",
                f"Manipulation {manipulation_score}/100",
                f"Dump risk {dump_risk_score}/100",
            ]
            if telegram_mentions:
                evidence.append(f"Telegram mentions {telegram_mentions}")
            if signal.get("price_change_24h") is not None:
                evidence.append(f"24h move {float(signal.get('price_change_24h') or 0):+.2f}%")

            replay.append({
                "timestamp": timestamp,
                "symbol": signal.get("symbol"),
                "name": signal.get("name") or signal.get("symbol"),
                "signal_type": signal_type,
                "signal_strength": signal.get("signal_strength", 0),
                "price_change_1h": signal.get("price_change_1h", 0),
                "price_change_24h": signal.get("price_change_24h", 0),
                "stage": stage,
                "manipulation_score": manipulation_score,
                "dump_risk_score": dump_risk_score,
                "telegram_mentions": telegram_mentions,
                "replay_label": "Pump build" if signal_type == "pump" else "Unwind",
                "replay_type": replay_type,
                "action": action,
                "evidence": evidence[:5],
                "direction_audit": direction_audit,
                "summary": profile.get("summary") or signal.get("reason", ""),
            })
            if len(replay) >= limit:
                return replay
    return replay

def build_coin_trend_conclusion(
    *,
    signal_type: str,
    symbol: str,
    price_change_1h: float,
    price_change_24h: float,
    volume_24h: float,
    market_cap: float,
    signal_strength: float,
    direction_audit: Optional[dict] = None,
) -> str:
    volume_ratio = (volume_24h / market_cap * 100) if market_cap else 0
    resolved_direction = (direction_audit or {}).get("resolved_direction", signal_type)
    transition_state = (direction_audit or {}).get("transition_state", "bullish_continuation" if resolved_direction == "pump" else "bearish_breakdown")
    if resolved_direction == "pump":
        if transition_state == "bullish_reversal":
            return (
                f"{symbol} is trying to pivot bullish after prior weakness, but this still looks like a reversal attempt rather than a fully confirmed uptrend. "
                f"To validate a real pump thesis, the next hourly sequence needs to keep printing upside follow-through with volume still supportive."
            )
        if transition_state == "bullish_pullback":
            return (
                f"{symbol} still leans bullish overall, but the current read looks more like a pullback inside an uptrend than a fresh breakout from the base. "
                f"If buyers keep defending the pullback and turnover stays firm, the upside continuation thesis remains intact."
            )
        if price_change_1h > 0 and price_change_24h > 0 and volume_ratio >= 20:
            return (
                f"{symbol} remains in a constructive short-term uptrend, with both hourly and daily momentum aligned and turnover still supportive. "
                f"As long as volume stays elevated and the signal strength remains near {int(signal_strength)}%, this looks more like active continuation than random noise."
            )
        return (
            f"{symbol} still has a bullish signal, but the setup needs stronger follow-through to confirm a clean continuation. "
            f"Watch whether volume holds and whether the next hourly candles keep building above the recent move."
        )
    if transition_state == "dead_cat_bounce":
        return (
            f"{symbol} still resolves bearish even though there may be a short-lived bounce on the smallest window. "
            f"With daily structure already broken, this reads more like a dead-cat bounce inside a dump than a true bullish reversal."
        )
    if price_change_1h < 0 and price_change_24h < 0 and volume_ratio >= 20:
        return (
            f"{symbol} is still leaning bearish, with downside pressure visible across both the 1h and 24h windows and enough volume to validate the move. "
            f"If sellers keep control, the current dump signal looks like continuation pressure rather than a one-candle flush."
        )
    return (
        f"{symbol} has a bearish read, but this move still needs confirmation from sustained downside momentum and persistent volume. "
        f"If selling pressure fades quickly, the setup can turn into a short-lived spike rather than a full trend leg lower."
    )

def get_coin_exchanges(symbol: str, coin_id: str, platform: Optional[str] = None, contract_address: Optional[str] = None) -> List[dict]:
    return build_market_venues(symbol, coin_id, platform, contract_address)

async def build_coin_pump_engine_payload(platform_id: Optional[str], contract_address: Optional[str]) -> dict:
    if not platform_id or not contract_address:
        return {
            "available": False,
            "reason": "missing_contract_context",
            "message": "Pump engine needs both platform_id and contract_address.",
        }

    try:
        pipeline = get_pump_engine_pipeline()
        analysis = await pipeline.run_from_token(
            chain=platform_id,
            token_address=contract_address,
            use_ai_judge=True,
        )
        return {
            "available": True,
            "analysis": analysis,
        }
    except PumpEngineError as exc:
        logger.info("Pump engine unavailable for %s/%s: %s", platform_id, contract_address, exc.code)
        return {
            "available": False,
            "reason": exc.code,
            "message": exc.message,
        }
    except Exception as exc:
        logger.warning("Pump engine integration error for %s/%s: %s", platform_id, contract_address, exc)
        return {
            "available": False,
            "reason": "PUMP_ENGINE_INTERNAL_ERROR",
            "message": "Pump engine could not analyze this asset right now.",
        }

@app.get("/api/crypto/coin/{symbol}")
async def get_coin_detail(symbol: str, type: str = "pump", refresh: bool = False):
    """Get detailed coin data with AI analysis"""
    symbol = symbol.upper()
    
    # Find signal in latest snapshot
    snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
    signal = None
    if snapshot:
        # First priority: AI-enriched final decision layer from /api/crypto/signals.
        # Coin detail must use the same verdict as dashboard without running Qwen live.
        cached_ai_decisions = (AI_DECISION_SIGNALS_CACHE.get("latest") or {}).get("items") or []
        decision_signals = cached_ai_decisions or build_decision_signals(snapshot)
        for ds in decision_signals:
            if str(ds.get("symbol") or "").upper() != symbol:
                continue

            mc = ds.get("market_context") or {}
            price_change_pct = mc.get("price_change_pct") or {}
            volume_usd = mc.get("volume_usd") or {}

            signal = {
                **ds,
                "id": ds.get("id") or ds.get("coingecko_coin_id") or symbol.lower(),
                "signal_type": ds.get("direction") or ds.get("signal_type") or type or "risk",
                "stored_signal_type": ds.get("direction") or ds.get("signal_type") or "risk",
                "requested_signal_type": ds.get("direction") or ds.get("signal_type") or type,
                "direction": ds.get("direction") or "risk",
                "signal_strength": ds.get("signal_strength") or ds.get("confidence") or 0,
                "confidence": ds.get("confidence") if ds.get("confidence") in {"high", "medium", "low"} else ("high" if int(ds.get("confidence") or 0) >= 70 else "medium" if int(ds.get("confidence") or 0) >= 50 else "low"),
                "risk_level": ds.get("risk_level") or ("high" if str(ds.get("action") or "").startswith("avoid") else "medium"),
                "reason": ds.get("why_now") or ds.get("reason") or ds.get("trigger") or "",
                "technical_factors": ds.get("technical_factors") or "Final decision signal from PumpRadar decision layer",
                "red_flags": ds.get("red_flags"),
                "price": mc.get("price_usd") or ds.get("price_usd") or 0,
                "price_change_1h": price_change_pct.get("h1") or 0,
                "price_change_24h": price_change_pct.get("h24") or 0,
                "volume_24h": volume_usd.get("h24") or ds.get("volume_24h") or 0,
                "market_cap": mc.get("market_cap_usd") or ds.get("market_cap_usd") or mc.get("fdv_usd") or ds.get("fdv_usd") or 0,
                "timestamp": snapshot.get("timestamp").isoformat() if isinstance(snapshot.get("timestamp"), datetime) else snapshot.get("timestamp"),
                "ai_source": ds.get("ai_source") or "Signal Schema v2 decision layer",
                "ai_judge_code": ds.get("ai_judge_code"),
                "ai_live_used": ds.get("ai_live_used"),
                "ai_fallback_used": ds.get("ai_fallback_used"),
                "ai_provider": ds.get("ai_provider"),
                "ai_model": ds.get("ai_model"),
                "ai_verdict_code": ds.get("ai_verdict_code"),
                "ai_reason_short": ds.get("ai_reason_short"),
                "ai_error": ds.get("ai_error"),
                "verdict": ds.get("verdict"),
                "final_verdict": ds.get("final_verdict") or ds.get("verdict"),
                "action": ds.get("action"),
                "timing": ds.get("timing"),
                "signal_quality": ds.get("signal_quality"),
            }
            break

        all_signals = [] if signal is not None else snapshot.get("pump_signals", []) + snapshot.get("dump_signals", [])
        for s in all_signals:
            if s.get("symbol", "").upper() == symbol:
                signal = s
                break

        # Fallback for Signal Schema v2 GeckoTerminal candidates.
        # These candidates are shown on the dashboard through /api/crypto/signals,
        # but they are not stored in legacy pump_signals/dump_signals.
        if signal is None:
            experimental_v2 = snapshot.get("experimental_signals_v2") or {}
            gecko_candidates = (
                experimental_v2.get("actionable_geckoterminal")
                or experimental_v2.get("geckoterminal")
                or []
            )
            for item in gecko_candidates:
                item_symbol = str(item.get("symbol") or item.get("base_symbol") or "").upper()
                if item_symbol != symbol:
                    continue

                market_context = item.get("market_context") or {}
                volume_usd = market_context.get("volume_usd") or {}
                price_change_pct = market_context.get("price_change_pct") or {}
                direction = (item.get("direction") or item.get("direction_hint") or type or "pump").lower()

                raw_reason = item.get("why_now") or item.get("trigger") or item.get("reason") or "Actionable GeckoTerminal Signal Schema v2 candidate."
                if isinstance(raw_reason, list):
                    reason_text = "; ".join(str(x) for x in raw_reason[:6])
                elif isinstance(raw_reason, dict):
                    reason_text = "; ".join(f"{k}: {v}" for k, v in list(raw_reason.items())[:6])
                else:
                    reason_text = str(raw_reason)

                raw_red_flags = item.get("red_flags") or []
                if isinstance(raw_red_flags, list):
                    red_flags_text = ", ".join(str(x) for x in raw_red_flags[:8])
                else:
                    red_flags_text = str(raw_red_flags)

                confidence_value = item.get("confidence", 0)
                try:
                    signal_strength = int(float(item.get("pump_coordination_score") if direction == "pump" else item.get("dump_distribution_score") or item.get("manipulation_setup_score") or item.get("dex_candidate_score") or confidence_value or 0))
                except Exception:
                    signal_strength = 0

                signal = {
                    "symbol": symbol,
                    "name": item.get("name") or item.get("pool_name") or symbol,
                    "id": item.get("coingecko_coin_id") or symbol.lower(),
                    "signal_type": direction,
                    "direction": direction,
                    "signal_strength": max(0, min(100, signal_strength)),
                    "confidence": item.get("confidence_label") or ("high" if float(confidence_value or 0) >= 70 else "medium"),
                    "risk_level": item.get("risk") or item.get("risk_level") or "high",
                    "reason": reason_text,
                    "technical_factors": "GeckoTerminal DEX activity, volume/liquidity anomaly, short-term market structure",
                    "red_flags": red_flags_text,
                    "price": market_context.get("price_usd") or item.get("price_usd") or 0,
                    "price_change_1h": price_change_pct.get("h1") or 0,
                    "price_change_24h": price_change_pct.get("h24") or 0,
                    "volume_24h": volume_usd.get("h24") or 0,
                    "market_cap": market_context.get("market_cap_usd") or item.get("market_cap_usd") or market_context.get("fdv_usd") or item.get("fdv_usd") or 0,
                    "contract_address": item.get("contract_or_mint") or item.get("token_address"),
                    "pool_address": item.get("pool_address"),
                    "pool_url": item.get("pool_url"),
                    "dex": item.get("dex"),
                    "source_stack": item.get("source_stack") or item.get("source_stack_hint") or {"dex": ["GeckoTerminal"]},
                    "tradeability": item.get("tradeability"),
                    "market_context": market_context,
                    "timestamp": (
                        snapshot.get("timestamp").isoformat()
                        if isinstance(snapshot.get("timestamp"), datetime)
                        else snapshot.get("timestamp")
                    ),
                    "ai_source": "Signal Schema v2 GeckoTerminal fallback",
                }
                break
    
    snapshot_ts = snapshot.get("timestamp") if snapshot else None
    snapshot_key = snapshot_ts.isoformat() if isinstance(snapshot_ts, datetime) else str(snapshot_ts)
    detail_cache_key = f"{symbol}::{snapshot_key}"
    if not refresh:
        cached_detail = get_memory_cache(COIN_DETAIL_CACHE, detail_cache_key, ttl_seconds=180)
        if cached_detail is not None:
            cached_exchanges = cached_detail.get("exchanges") or []
            cached_preferred_venue = cached_detail.get("preferred_venue") or ((cached_detail.get("decision_engine") or {}).get("preferred_venue"))
            if cached_exchanges or cached_preferred_venue:
                return api_ok(cached_detail)

    # Get CoinGecko market data
    market_data = {}
    try:
        preferred_name = signal.get("name") if signal else None
        resolved_coin_id = (
            market_data.get("id")
            or (signal.get("id") if signal else None)
            or resolve_coingecko_coin_id(symbol, preferred_name=preferred_name)
        )
        market_cache_key = f"{resolved_coin_id or symbol.lower()}::market"
        cached_market = None if refresh else get_memory_cache(COIN_MARKET_CACHE, market_cache_key, ttl_seconds=300)
        if cached_market is not None:
            market_data = cached_market
        else:
            market_url = "https://api.coingecko.com/api/v3/coins/markets"
            mr = requests.get(market_url, params={
                "vs_currency": "usd", "ids": resolved_coin_id,
                "price_change_percentage": "1h,24h,7d"
            }, headers=CG_HEADERS, timeout=15)
            if mr.status_code == 200 and mr.json():
                market_data = mr.json()[0]
                set_memory_cache(COIN_MARKET_CACHE, market_cache_key, market_data)
    except Exception as e:
        logger.error(f"CoinGecko detail error: {e}")
    
    price = market_data.get("current_price") or (signal.get("price") if signal else 0) or 0
    price_change_1h = market_data.get("price_change_percentage_1h_in_currency") or (signal.get("price_change_1h") if signal else 0) or 0
    price_change_24h = market_data.get("price_change_percentage_24h") or (signal.get("price_change_24h") if signal else 0) or 0
    price_change_7d = market_data.get("price_change_percentage_7d_in_currency") or 0
    volume_24h = market_data.get("total_volume") or (signal.get("volume_24h") if signal else 0) or 0
    market_cap = market_data.get("market_cap") or 0
    image = market_data.get("image") or (signal.get("image") if signal else "")
    coin_id = market_data.get("id") or (signal.get("id") if signal else None) or resolved_coin_id or symbol.lower()
    
    # VALIDATION: If no valid data found, return 404
    if price <= 0 and not signal:
        raise HTTPException(
            status_code=404, 
            detail=api_err(f"Coin '{symbol}' not found or has no valid market data. It may be delisted or not supported.", "COIN_NOT_FOUND")
        )
    
    # If coin has invalid data (price 0, market cap < $100k), warn user
    is_invalid_coin = price <= 0 or market_cap < 100000
    
    # Get chart, social, derivatives, and replay data in parallel for the first paint.
    social_asset_name = market_data.get("name") or (signal.get("name") if signal else None)
    (
        chart_data,
        extended_details,
        market_microstructure,
        derivatives_data,
        social_bundle,
        case_replay,
    ) = await asyncio.gather(
        asyncio.to_thread(get_coin_chart_data, coin_id, 1),
        asyncio.to_thread(get_coin_extended_details, coin_id),
        asyncio.to_thread(get_binance_orderbook_metrics, symbol),
        asyncio.to_thread(get_binance_derivatives_metrics, symbol),
        asyncio.to_thread(get_social_intelligence_bundle, symbol, social_asset_name, 3),
        build_coin_case_replay(symbol, limit=10),
    )
    lunarcrush_topic, lunarcrush_creators = social_bundle

    platform_id, contract_address = pick_primary_contract(extended_details)
    tokenomics = build_tokenomics_profile(extended_details) if extended_details else {
        "circulating_supply": None,
        "total_supply": None,
        "max_supply": None,
        "fdv_usd": None,
        "market_cap_usd": market_cap,
        "circulating_ratio_pct": None,
        "dilution_gap_pct": None,
        "unlock_risk": "Unknown",
        "warnings": [],
        "source": "CoinGecko",
    }
    holder_distribution, goplus_security, goplus_rugpull, exchanges, pump_engine = await asyncio.gather(
        asyncio.to_thread(get_holder_distribution, platform_id, contract_address),
        asyncio.to_thread(get_goplus_security, platform_id, contract_address),
        asyncio.to_thread(get_goplus_rugpull, platform_id, contract_address),
        asyncio.to_thread(get_coin_exchanges, symbol, coin_id, platform_id, contract_address),
        build_coin_pump_engine_payload(platform_id, contract_address),
    )
    wallet_concentration = build_holder_concentration_profile(holder_distribution, goplus_security)
    wallet_cluster_intelligence = build_wallet_cluster_intelligence(holder_distribution, goplus_security)
    contract_risk = build_contract_risk_profile(platform_id, contract_address, goplus_security, goplus_rugpull)
    asset_identity = build_asset_identity_profile(
        symbol=symbol,
        coin_id=coin_id,
        name=market_data.get("name") or (signal.get("name") if signal else symbol),
        market_cap=market_cap,
        details=extended_details,
        venues=exchanges,
    )
    telegram_stats_map = await get_recent_telegram_signal_map(hours=24)
    telegram_stats = telegram_stats_map.get(symbol, {})
    requested_signal_type = type
    stored_signal_type = (signal.get("signal_type") if signal else None) or None
    current_direction_audit = build_direction_audit(
        symbol=symbol,
        price_change_1h=price_change_1h,
        price_change_24h=price_change_24h,
        price_change_7d=price_change_7d,
        volume_24h=volume_24h,
        market_cap=market_cap,
        signal_type_hint=stored_signal_type or requested_signal_type,
        signal_strength_hint=signal.get("signal_strength", 0) if signal else 0,
        pump_strength=signal.get("signal_strength", 0) if signal and stored_signal_type == "pump" else 0,
        dump_strength=signal.get("signal_strength", 0) if signal and stored_signal_type == "dump" else 0,
        is_trending=bool(signal.get("is_trending")) if signal else False,
    )
    resolved_signal_type = current_direction_audit.get("resolved_direction", stored_signal_type or requested_signal_type)

    # Decision-layer signals are already final verdicts. Do not let direction audit
    # downgrade/flip "risk" into simple pump/dump on the coin detail page.
    is_decision_layer_signal = bool(signal) and str(signal.get("ai_source") or "").lower().startswith("signal schema v2 decision layer")
    if is_decision_layer_signal and stored_signal_type:
        resolved_signal_type = stored_signal_type

    if signal and stored_signal_type and resolved_signal_type != stored_signal_type:
        logger.info(
            "Coin detail direction flip for %s: stored=%s resolved=%s requested=%s 1h=%+.2f 24h=%+.2f 7d=%+.2f transition=%s",
            symbol,
            stored_signal_type,
            resolved_signal_type,
            requested_signal_type,
            current_direction_audit.get("price_change_1h", 0.0),
            current_direction_audit.get("price_change_24h", 0.0),
            current_direction_audit.get("price_change_7d", 0.0),
            current_direction_audit.get("transition_state", "unknown"),
        )
    
    analysis_sections: List[dict] = []
    ai_analysis = ""
    trend_conclusion = ""
    if signal:
        analysis_sections = build_coin_analysis_sections(
            symbol=symbol,
            signal_type=resolved_signal_type,
            price=price,
            price_change_1h=price_change_1h,
            price_change_24h=price_change_24h,
            price_change_7d=price_change_7d,
            volume_24h=volume_24h,
            market_cap=market_cap,
            signal_strength=signal.get("signal_strength", 0),
            confidence=signal.get("confidence", "medium"),
            risk_level=signal.get("risk_level", "medium"),
            reason=signal.get("reason", ""),
            social_volume=signal.get("social_volume", 0) or 0,
            galaxy_score=signal.get("galaxy_score", 0) or 0,
            direction_audit=current_direction_audit,
        )
        ai_analysis = "\n\n".join(section["body"] for section in analysis_sections)
        trend_conclusion = build_coin_trend_conclusion(
            signal_type=resolved_signal_type,
            symbol=symbol,
            price_change_1h=price_change_1h,
            price_change_24h=price_change_24h,
            volume_24h=volume_24h,
            market_cap=market_cap,
            signal_strength=signal.get("signal_strength", 0),
            direction_audit=current_direction_audit,
        )
        try:
            system_instruction = (
                "You are a crypto technical analysis expert. "
                "You respond in English only, concisely and directly. "
                "Never reply in Romanian or any other language. "
                "Return strict JSON only."
            )
            prompt = f"""Improve this structured coin analysis without changing the facts. Keep it precise, practical, and in English only.
Do not include Romanian or any bilingual output.

Coin: {symbol}
Signal type: {resolved_signal_type}
Base analysis:
{ai_analysis}

Trend conclusion:
{trend_conclusion}

Respond with JSON:
{{
  "analysis": "4 concise paragraphs separated logically",
  "trend": "2 concise sentences"
}}"""
            ai_result = await call_claude_haiku_json(
                system_instruction=system_instruction,
                user_prompt=prompt,
                temperature=0.15,
                max_tokens=150,
            )

            if ai_result.get("ok") and ai_result.get("json"):
                detail_json = ai_result["json"]
                ai_analysis = detail_json.get("analysis", ai_analysis) or ai_analysis
                trend_conclusion = detail_json.get("trend", trend_conclusion) or trend_conclusion
                ai_paragraphs = [part.strip() for part in ai_analysis.split("\n\n") if part.strip()]
                if ai_paragraphs:
                    titles = ["Momentum Setup", "Liquidity & Participation", "Signal Read", "Risk Watch"]
                    analysis_sections = [
                        {"title": titles[index] if index < len(titles) else f"Section {index + 1}", "body": paragraph}
                        for index, paragraph in enumerate(ai_paragraphs)
                    ]
            else:
                logger.warning(
                    "OpenAI/OpenRouter coin detail refinement failed - keeping deterministic analysis. provider=%s error=%s",
                    ai_result.get("provider"),
                    ai_result.get("error"),
                )
        except Exception as e:
            logger.error(f"OpenAI/OpenRouter coin detail AI error - keeping deterministic analysis: {e}")
    else:
        analysis_sections = [
            {
                "title": "No Active Signal",
                "body": f"No active PumpRadar signal was recorded for {symbol} in the latest hourly snapshot. The coin may still be tradable, but there is no current high-conviction pump or dump setup from the model.",
            },
            {
                "title": "What To Watch",
                "body": "Focus on fresh volume expansion, short-term price acceleration, and whether the asset starts appearing in new trending lists before acting on it.",
            },
        ]
        ai_analysis = "\n\n".join(section["body"] for section in analysis_sections)
        trend_conclusion = "There is no live signal confirmation yet, so this is a watchlist asset rather than an actionable setup right now."
    
    decision_engine = build_signal_execution_plan(
        signal_type=resolved_signal_type,
        symbol=symbol,
        price=price,
        price_change_1h=price_change_1h,
        price_change_24h=price_change_24h,
        price_change_7d=price_change_7d,
        volume_24h=volume_24h,
        market_cap=market_cap,
        signal_strength=signal.get("signal_strength", 0) if signal else 0,
        confidence=signal.get("confidence", "medium") if signal else "medium",
        risk_level=signal.get("risk_level", "medium") if signal else "medium",
        venues=exchanges,
        direction_audit=current_direction_audit,
    )
    manipulation_profile = build_manipulation_profile(
        signal_type=resolved_signal_type,
        symbol=symbol,
        price_change_1h=price_change_1h,
        price_change_24h=price_change_24h,
        price_change_7d=price_change_7d,
        volume_24h=volume_24h,
        market_cap=market_cap,
        signal_strength=signal.get("signal_strength", 0) if signal else 0,
        risk_level=signal.get("risk_level", "medium") if signal else "medium",
        is_trending=bool(signal.get("is_trending")) if signal else False,
        social_volume=signal.get("social_volume", 0) if signal else 0,
        sentiment=signal.get("sentiment", 0) if signal else 0,
        galaxy_score=signal.get("galaxy_score", 0) if signal else 0,
        decision_engine=decision_engine,
        telegram_stats=telegram_stats,
        derivatives_data=derivatives_data,
        tokenomics=tokenomics,
        wallet_concentration=wallet_concentration,
        contract_risk=contract_risk,
        direction_audit=current_direction_audit,
    )
    rugpull_profile = build_rugpull_profile(
        asset_identity=asset_identity,
        tokenomics=tokenomics,
        wallet_cluster_intelligence=wallet_cluster_intelligence,
        contract_risk=contract_risk,
        venues=exchanges,
        manipulation_profile=manipulation_profile,
    )
    manipulation_timeline = build_manipulation_timeline(
        symbol=symbol,
        signal_type=resolved_signal_type,
        manipulation_profile=manipulation_profile,
        decision_engine=decision_engine,
        fear_greed=snapshot.get("fear_greed") if snapshot else None,
        is_trending=bool(signal.get("is_trending")) if signal else False,
        social_volume=signal.get("social_volume", 0) if signal else 0,
        galaxy_score=signal.get("galaxy_score", 0) if signal else 0,
    )
    cross_platform = build_coin_cross_platform_consensus(
        symbol=symbol,
        signal_type=resolved_signal_type,
        signal_strength=signal.get("signal_strength", 0) if signal else 0,
        manipulation_profile=manipulation_profile,
        decision_engine=decision_engine,
        lunar_topic=lunarcrush_topic,
        lunar_creators=lunarcrush_creators,
        is_trending=bool(signal.get("is_trending")) if signal else False,
    )
    signal_sources = (signal.get("signal_sources") if signal else None) or build_signal_source_stack(
        symbol=symbol,
        price_change_1h=price_change_1h,
        price_change_24h=price_change_24h,
        vol_mcap_ratio=(volume_24h / market_cap * 100) if market_cap else 0,
        is_trending=bool(signal.get("is_trending")) if signal else False,
        telegram_mentions=telegram_stats.get("mentions", 0) or 0,
        telegram_sources=telegram_stats.get("unique_sources", 0) or telegram_stats.get("telegram_sources", 0) or 0,
        bullish_mentions=telegram_stats.get("bullish_mentions", 0) or 0,
        bearish_mentions=telegram_stats.get("bearish_mentions", 0) or 0,
        telegram_avg_score=telegram_stats.get("avg_score", 0) or 0,
        social_volume=signal.get("social_volume", 0) if signal else 0,
        galaxy_score=signal.get("galaxy_score", 0) if signal else 0,
        sentiment=signal.get("sentiment", 0) if signal else 0,
        lunar_mentions=(lunarcrush_topic or {}).get("mentions_24h", 0) or 0,
        lunar_creators=(lunarcrush_topic or {}).get("creators_24h", 0) or 0,
        lunar_interactions=(lunarcrush_topic or {}).get("engagements_24h", 0) or 0,
        lunar_dominance=(lunarcrush_topic or {}).get("social_dominance_pct", 0) or 0,
        venue_count=decision_engine.get("venue_count", 0) or len(exchanges),
        preferred_venue=decision_engine.get("preferred_venue"),
    )
    payload = {
        "symbol": symbol,
        "name": market_data.get("name") or (signal.get("name") if signal else symbol),
        "image": image,
        "price": price,
        "price_change_1h": price_change_1h,
        "price_change_24h": price_change_24h,
        "price_change_7d": price_change_7d,
        "volume_24h": volume_24h,
        "market_cap": market_cap,
        "signal_type": resolved_signal_type,
        "requested_signal_type": resolved_signal_type if resolved_signal_type == "risk" else requested_signal_type,
        "stored_signal_type": stored_signal_type,
        "direction": signal.get("direction") if signal else resolved_signal_type,
        "verdict": signal.get("verdict") if signal else None,
        "final_verdict": (signal.get("final_verdict") or signal.get("verdict")) if signal else None,
        "action": signal.get("action") if signal else None,
        "timing": signal.get("timing") if signal else None,
        "signal_quality": signal.get("signal_quality") if signal else None,
        "ai_judge_code": signal.get("ai_judge_code") if signal else None,
        "ai_source": signal.get("ai_source") if signal else None,
        "ai_live_used": signal.get("ai_live_used") if signal else None,
        "ai_fallback_used": signal.get("ai_fallback_used") if signal else None,
        "ai_provider": signal.get("ai_provider") if signal else None,
        "ai_model": signal.get("ai_model") if signal else None,
        "ai_verdict_code": signal.get("ai_verdict_code") if signal else None,
        "ai_reason_short": signal.get("ai_reason_short") if signal else None,
        "ai_error": signal.get("ai_error") if signal else None,
        "signal_strength": signal.get("signal_strength", 0) if signal else 0,
        "reason": signal.get("reason", "") if signal else "",
        "confidence": signal.get("confidence", "medium") if signal else "medium",
        "risk_level": signal.get("risk_level", "medium") if signal else "medium",
        "direction_audit": current_direction_audit,
        "ai_analysis": ai_analysis,
        "analysis_sections": analysis_sections,
        "trend_conclusion": trend_conclusion,
        "decision_engine": decision_engine,
        "preferred_venue": decision_engine.get("preferred_venue"),
        "signal_sources": signal_sources,
        "source_summary": signal_sources.get("summary"),
        "manipulation_profile": manipulation_profile,
        "manipulation_timeline": manipulation_timeline,
        "cross_platform_consensus": cross_platform,
        "case_replay": case_replay,
        "market_microstructure": market_microstructure,
        "derivatives_data": derivatives_data,
        "tokenomics": tokenomics,
        "wallet_concentration": wallet_concentration,
        "wallet_cluster_intelligence": wallet_cluster_intelligence,
        "contract_risk": contract_risk,
        "pump_engine": pump_engine,
        "asset_identity": asset_identity,
        "rugpull_profile": rugpull_profile,
        "lunarcrush_topic": lunarcrush_topic,
        "lunarcrush_creators": lunarcrush_creators,
        "platform_id": platform_id,
        "contract_address": contract_address,
        "exchanges": exchanges,
        "chart_data": chart_data,
    }

    return api_ok(set_memory_cache(COIN_DETAIL_CACHE, detail_cache_key, payload))

# ─────────────────────────────────────────────
# TELEGRAM SIGNALS
# ─────────────────────────────────────────────
async def require_admin(user=Depends(get_current_user)):
    if "admin" not in user.get("roles", []):
        raise HTTPException(status_code=403, detail=api_err("Admin access required", "FORBIDDEN"))
    return user

TELEGRAM_HORIZONS = {
    "one_hour": {"hours": 1, "threshold_pct": 3.0},
    "four_hour": {"hours": 4, "threshold_pct": 5.0},
    "twenty_four_hour": {"hours": 24, "threshold_pct": 8.0},
}

TELEGRAM_SOURCE_STRONG_KEYWORDS = (
    "pump", "dump", "signal", "signals", "call", "calls", "alpha", "gem", "gems",
    "whale", "sniper", "entry", "target", "tp", "sl", "trade", "trading",
)
TELEGRAM_SOURCE_MEDIUM_KEYWORDS = (
    "crypto", "coin", "token", "defi", "dex", "cex", "blockchain", "chain", "network",
    "protocol", "binance", "bybit", "okx", "kucoin", "announcement", "news",
    "official", "ecosystem", "market",
)
TELEGRAM_SOURCE_NEGATIVE_KEYWORDS = (
    "hot girls", "girls", "dating", "escort", "xxx", "adult", "casino", "betting",
)

TELEGRAM_SPAM_PHRASES = (
    "rewards pool", "reward pool", "giveaway", "airdrop", "claim now", "verify your holdings",
    "copy and open", "open atoshi", "updated to the latest version", "latest version",
    "referral", "invite friends", "earn more", "bonus", "join now", "register now",
    "withdraw", "withdrawal", "balance", "contest", "sweepstake", "launch is live",
    "rewards are live", "pool is live", "share your story", "inspire the community",
    "follow us", "retweet", "like and share", "promo code", "promotion", "marketing",
)
TELEGRAM_TRADE_KEYWORDS = (
    "entry", "entries", "buy zone", "accumulate", "target", "targets", "take profit", "tp1", "tp2", "tp3",
    "stop loss", "sl", "breakout", "breakdown", "resistance", "support", "spot entry",
    "leverage", "long", "short", "open long", "open short", "risk reward", "rr",
    "ape", "scalp", "send", "flush", "send it", "watchlist",
)
TELEGRAM_DIRECTION_KEYWORDS = (
    "pump", "dump", "bullish", "bearish", "long", "short", "buy", "sell", "moon", "rug",
    "ape", "send", "flush", "nuke",
)

class TelegramSourceUpsertRequest(BaseModel):
    source_name: str
    source_handle: Optional[str] = None
    source_type: str = "group"
    invite_link: Optional[str] = None
    enabled: bool = True
    notes: Optional[str] = None

class TelegramSignalIngestRequest(BaseModel):
    source_name: str
    source_handle: Optional[str] = None
    source_type: str = "group"
    message_text: str
    message_id: Optional[str] = None
    message_url: Optional[str] = None
    posted_at: Optional[str] = None
    source_id: Optional[str] = None

class TelegramManualOutcomeRequest(BaseModel):
    return_1h_pct: Optional[float] = None
    return_4h_pct: Optional[float] = None
    return_24h_pct: Optional[float] = None

class TelegramAuthCodeRequest(BaseModel):
    code: str
    password: Optional[str] = None

def get_telegram_session_dir() -> str:
    session_dir = os.path.join(os.path.dirname(__file__), ".telegram_sessions")
    os.makedirs(session_dir, exist_ok=True)
    return session_dir

def get_telegram_session_path() -> str:
    return os.path.join(get_telegram_session_dir(), TELEGRAM_SESSION_NAME)

async def create_telegram_client() -> Any:
    if not TelegramClient:
        return None
    client = TelegramClient(get_telegram_session_path(), int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
    await client.connect()
    return client

async def get_enabled_telegram_sources() -> List[dict]:
    whitelisted = await db.telegram_sources.find({"enabled": True, "manual_whitelist": True}).to_list(length=500)
    if whitelisted:
        return whitelisted
    return await db.telegram_sources.find({"enabled": True}).to_list(length=500)

async def enforce_manual_whitelist_sources() -> None:
    has_whitelist = await db.telegram_sources.count_documents({"manual_whitelist": True}, limit=1)
    if not has_whitelist:
        return
    await db.telegram_sources.update_many(
        {"manual_whitelist": {"$ne": True}},
        {"$set": {"enabled": False}},
    )
    await db.telegram_sources.update_many(
        {"manual_whitelist": True},
        {"$set": {"enabled": True}},
    )

def score_telegram_source_relevance(source_name: Optional[str], source_handle: Optional[str]) -> tuple[int, str]:
    handle = normalize_telegram_handle(source_handle) or ""
    haystack = " ".join(part for part in [(source_name or "").lower().strip(), handle] if part).strip()
    if not haystack:
        return 0, "No source name available"
    if any(keyword in haystack for keyword in TELEGRAM_SOURCE_NEGATIVE_KEYWORDS):
        return 0, "Filtered as irrelevant / non-crypto"

    score = 0
    reasons: List[str] = []
    strong_hits = sorted({keyword for keyword in TELEGRAM_SOURCE_STRONG_KEYWORDS if keyword in haystack})
    medium_hits = sorted({keyword for keyword in TELEGRAM_SOURCE_MEDIUM_KEYWORDS if keyword in haystack})

    if strong_hits:
        score += min(70, 25 + len(strong_hits) * 12)
        reasons.append(f"signal keywords: {', '.join(strong_hits[:4])}")
    if medium_hits:
        score += min(35, 10 + len(medium_hits) * 6)
        reasons.append(f"crypto keywords: {', '.join(medium_hits[:4])}")
    if "official" in haystack and "announcement" in haystack:
        score += 8
        reasons.append("official announcement source")
    if any(exchange in haystack for exchange in ("binance", "bybit", "okx", "kucoin")):
        score += 8
        reasons.append("major exchange source")

    score = min(100, score)
    if score == 0:
        return 0, "No clear crypto or signal relevance"
    return score, "; ".join(reasons)

def should_enable_telegram_source(source_name: Optional[str], source_handle: Optional[str]) -> tuple[bool, int, str]:
    score, reason = score_telegram_source_relevance(source_name, source_handle)
    return score >= 28, score, reason

def matches_telegram_source(source: dict, chat_title: Optional[str], chat_username: Optional[str]) -> bool:
    source_handle = normalize_telegram_handle(source.get("source_handle"))
    source_name = (source.get("source_name") or "").strip().lower()
    username = normalize_telegram_handle(chat_username)
    title = (chat_title or "").strip().lower()
    return bool(
        (source_handle and username and source_handle == username) or
        (source_name and title and source_name == title)
    )

def is_telegram_collectable_chat(chat: Any) -> bool:
    if not chat:
        return False
    if getattr(chat, "broadcast", False):
        return True
    if getattr(chat, "megagroup", False):
        return True
    if getattr(chat, "gigagroup", False):
        return True
    title = getattr(chat, "title", None)
    return bool(title)

def build_telegram_source_type(chat: Any) -> str:
    if getattr(chat, "broadcast", False):
        return "channel"
    return "group"

def build_telegram_source_payload(chat: Any) -> Optional[TelegramSourceUpsertRequest]:
    if not is_telegram_collectable_chat(chat):
        return None
    source_name = getattr(chat, "title", None) or getattr(chat, "username", None)
    if not source_name:
        return None
    source_handle = getattr(chat, "username", None)
    enabled, _, reason = should_enable_telegram_source(source_name, source_handle)
    return TelegramSourceUpsertRequest(
        source_name=source_name.strip(),
        source_handle=source_handle,
        source_type=build_telegram_source_type(chat),
        enabled=enabled,
        notes=reason,
    )

async def sync_telegram_dialog_sources(client: Any) -> int:
    if not client:
        return 0
    synced = 0
    async for dialog in client.iter_dialogs(limit=300):
        payload = build_telegram_source_payload(dialog.entity)
        if not payload:
            continue
        await upsert_telegram_source(payload)
        synced += 1
    await enforce_manual_whitelist_sources()
    return synced

def telegram_runtime_status() -> dict:
    import importlib.util

    telethon_installed = importlib.util.find_spec("telethon") is not None
    session_path = get_telegram_session_path()
    session_exists = os.path.exists(session_path) or os.path.exists(f"{session_path}.session")
    authorized = bool(telegram_auth_state.get("authorized"))
    ready = bool(TELEGRAM_API_ID and TELEGRAM_API_HASH and TELEGRAM_PHONE and telethon_installed and TELEGRAM_LIVE_ENABLED)
    reasons = []
    if not TELEGRAM_API_ID:
        reasons.append("Missing TELEGRAM_API_ID")
    if not TELEGRAM_API_HASH:
        reasons.append("Missing TELEGRAM_API_HASH")
    if not TELEGRAM_PHONE:
        reasons.append("Missing TELEGRAM_PHONE")
    if not telethon_installed:
        reasons.append("Telethon is not installed yet")
    if not TELEGRAM_LIVE_ENABLED:
        reasons.append("TELEGRAM_LIVE_ENABLED is false")
    if ready and not session_exists:
        reasons.append("Telegram session is not authorized yet")
    return {
        "ready": ready and session_exists and authorized,
        "telethon_installed": telethon_installed,
        "api_id_configured": bool(TELEGRAM_API_ID),
        "api_hash_configured": bool(TELEGRAM_API_HASH),
        "phone_configured": bool(TELEGRAM_PHONE),
        "live_enabled": TELEGRAM_LIVE_ENABLED,
        "session_name": TELEGRAM_SESSION_NAME,
        "session_exists": session_exists,
        "authorized": authorized,
        "reasons": reasons,
    }

def serialize_datetime(value: Any) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, str):
        return value
    return None

def normalize_telegram_handle(handle: Optional[str]) -> Optional[str]:
    if not handle:
        return None
    value = handle.strip()
    if value.startswith("https://t.me/"):
        value = value.split("https://t.me/", 1)[1]
    if value.startswith("@"):
        value = value[1:]
    return value.lower() or None

def average(values: List[float]) -> float:
    nums = [float(v) for v in values if v is not None]
    return round(sum(nums) / len(nums), 2) if nums else 0.0

def parse_numeric_range(raw: str) -> Optional[dict]:
    if not raw:
        return None
    normalized = raw.replace(",", ".")
    values = re.findall(r"\d+(?:\.\d+)?", normalized)
    if not values:
        return None
    if len(values) == 1:
        value = float(values[0])
        return {"low": value, "high": value}
    low = float(values[0])
    high = float(values[1])
    return {"low": min(low, high), "high": max(low, high)}

def parse_numeric_list(raw: str) -> List[float]:
    if not raw:
        return []
    return [float(item.replace(",", ".")) for item in re.findall(r"\d+(?:\.\d+)?", raw)]

def infer_chain_from_message(message: str, contract_address: Optional[str]) -> Optional[str]:
    lower = (message or "").lower()
    if contract_address:
        if contract_address.startswith("0x"):
            if any(keyword in lower for keyword in ["bsc", "binance smart chain", "bnb chain"]):
                return "binance-smart-chain"
            if "base" in lower:
                return "base"
            if "arb" in lower or "arbitrum" in lower:
                return "arbitrum-one"
            return "ethereum"
        if re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", contract_address):
            return "solana"
        if re.fullmatch(r"[a-z]{2,15}1[0-9a-z]{20,90}", contract_address):
            if contract_address.startswith("akash"):
                return "akash"
            return "cosmos"
    if "solana" in lower or " raydium" in lower or " jupiter" in lower:
        return "solana"
    if "ethereum" in lower or " uniswap" in lower:
        return "ethereum"
    if "base" in lower:
        return "base"
    if "arbitrum" in lower:
        return "arbitrum-one"
    if "bsc" in lower or "bnb chain" in lower:
        return "binance-smart-chain"
    if "akash" in lower:
        return "akash"
    return None

def parse_telegram_signal_message(message_text: str) -> dict:
    text = (message_text or "").strip()
    upper = text.upper()
    lower = text.lower()

    symbol = None
    symbol_candidates = re.findall(r"\$([A-Z0-9]{2,15})", upper)
    if not symbol_candidates:
        symbol_candidates = [match[0] for match in re.findall(r"\b([A-Z0-9]{2,15})/(USDT|USD|BTC|ETH|SOL|BNB)\b", upper)]
    if not symbol_candidates:
        symbol_candidates = re.findall(r"#([A-Z0-9]{2,15})", upper)
    if not symbol_candidates:
        symbol_candidates = re.findall(r"\b[A-Z][A-Z0-9]{1,14}\b", upper)
    blacklist = {"USDT", "USD", "BTC", "ETH", "SOL", "BNB", "LONG", "SHORT", "ENTRY", "TARGET", "STOP", "BUY", "SELL"}
    for candidate in symbol_candidates:
        if candidate not in blacklist:
            symbol = candidate
            break

    direction = None
    if any(keyword in lower for keyword in ["dump", "short", "sell", "rug", "bearish"]):
        direction = "dump"
    elif any(keyword in lower for keyword in ["pump", "long", "buy", "moon", "bullish"]):
        direction = "pump"

    contract_address = None
    contract_patterns = [
        r"\b0x[a-fA-F0-9]{40}\b",
        r"\b[a-z]{2,15}1[0-9a-z]{20,90}\b",
        r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b",
    ]
    for pattern in contract_patterns:
        match = re.search(pattern, text)
        if match:
            contract_address = match.group(0)
            break

    entry_match = re.search(r"(?:entry|buy(?:\s+zone)?|accumulate)\s*[:\-]?\s*([0-9.,\-\s]+)", lower, re.IGNORECASE)
    stop_match = re.search(r"(?:stop(?:\s+loss)?|sl)\s*[:\-]?\s*([0-9.,]+)", lower, re.IGNORECASE)
    target_match = re.search(r"(?:targets?|take profit|tp(?:1|2|3)?)\s*[:\-]?\s*([0-9.,\-\s/]+)", lower, re.IGNORECASE)

    entry_zone = parse_numeric_range(entry_match.group(1)) if entry_match else None
    stop_values = parse_numeric_list(stop_match.group(1)) if stop_match else []
    target_values = parse_numeric_list(target_match.group(1)) if target_match else []

    chain = infer_chain_from_message(text, contract_address)
    confidence_parts = [
        25 if symbol else 0,
        20 if direction else 0,
        20 if contract_address else 0,
        15 if entry_zone else 0,
        10 if stop_values else 0,
        10 if target_values else 0,
    ]
    parser_confidence = min(100, sum(confidence_parts))
    spam_hits = sorted({phrase for phrase in TELEGRAM_SPAM_PHRASES if phrase in lower})
    trade_hits = sorted({phrase for phrase in TELEGRAM_TRADE_KEYWORDS if phrase in lower})
    direction_hits = sorted({phrase for phrase in TELEGRAM_DIRECTION_KEYWORDS if phrase in lower})
    short_call_detected = bool(
        symbol and (
            trade_hits or
            direction_hits or
            contract_address or
            re.search(rf"\b{re.escape(symbol)}\b\s*(?:now|soon|spot|entry|breakout|breakdown|send|ape)\b", upper, re.IGNORECASE)
        )
    )
    parser_confidence = min(
        100,
        parser_confidence +
        (8 if short_call_detected and not entry_zone else 0) +
        (5 if symbol and trade_hits and not target_values else 0)
    )

    return {
        "symbol": symbol,
        "direction": direction or "pump",
        "chain": chain,
        "contract_address": contract_address,
        "entry_zone": entry_zone,
        "stop_loss": stop_values[0] if stop_values else None,
        "targets": target_values,
        "parser_confidence": parser_confidence,
        "spam_hits": spam_hits,
        "trade_hits": trade_hits,
        "direction_hits": direction_hits,
        "short_call_detected": short_call_detected,
    }

def explain_telegram_signal_rejection(parsed: dict) -> List[str]:
    symbol = (parsed.get("symbol") or "").upper()
    contract_address = parsed.get("contract_address")
    parser_confidence = float(parsed.get("parser_confidence") or 0)
    spam_hits = parsed.get("spam_hits") or []
    trade_hits = parsed.get("trade_hits") or []
    direction_hits = parsed.get("direction_hits") or []
    entry_zone = parsed.get("entry_zone")
    stop_loss = parsed.get("stop_loss")
    targets = parsed.get("targets") or []
    short_call_detected = bool(parsed.get("short_call_detected"))
    reasons: List[str] = []
    if not symbol and not contract_address:
        reasons.append("missing_symbol_or_contract")
    if symbol and symbol.isdigit():
        reasons.append("numeric_symbol_candidate")
    if symbol and len(symbol) < 2:
        reasons.append("symbol_too_short")
    numeric_targets = [target for target in targets if isinstance(target, (int, float))]
    has_numeric_plan = bool(entry_zone or stop_loss or numeric_targets)
    has_trade_structure = bool(contract_address or has_numeric_plan or trade_hits)
    has_direction = bool(direction_hits)
    has_short_call = bool(symbol and short_call_detected and (trade_hits or direction_hits))
    has_contract_play = bool(contract_address and has_direction)
    has_structured_plan = bool(symbol and has_direction and has_numeric_plan)
    if spam_hits and not has_contract_play:
        reasons.append("spam_like_message")
    if not has_trade_structure and not has_direction and not has_short_call:
        reasons.append("missing_trade_or_direction_structure")
    if not has_contract_play and not has_structured_plan and not has_short_call:
        reasons.append("missing_contract_or_structured_plan")
    if not has_contract_play and not has_short_call and parser_confidence < 45:
        reasons.append("parser_confidence_below_threshold")
    return reasons

def should_store_telegram_signal(parsed: dict) -> bool:
    return len(explain_telegram_signal_rejection(parsed)) == 0

async def record_telegram_signal_rejection(
    *,
    source: Optional[dict],
    message_text: str,
    parsed: Optional[dict],
    reasons: List[str],
    stage: str,
    message_id: Optional[str] = None,
    message_url: Optional[str] = None,
    posted_at: Optional[datetime] = None,
) -> None:
    try:
        payload = parsed or {}
        await db.telegram_signal_rejections.insert_one({
            "source_id": str(source["_id"]) if source and source.get("_id") else None,
            "source_name": source.get("source_name") if source else None,
            "source_handle": source.get("source_handle") if source else None,
            "message_id": message_id,
            "message_url": message_url,
            "message_text": message_text,
            "posted_at": posted_at or datetime.now(timezone.utc),
            "created_at": datetime.now(timezone.utc),
            "stage": stage,
            "symbol": payload.get("symbol"),
            "direction": payload.get("direction"),
            "contract_address": payload.get("contract_address"),
            "parser_confidence": payload.get("parser_confidence", 0),
            "trade_hits": payload.get("trade_hits", []),
            "direction_hits": payload.get("direction_hits", []),
            "spam_hits": payload.get("spam_hits", []),
            "short_call_detected": bool(payload.get("short_call_detected")),
            "reasons": reasons,
        })
    except Exception as e:
        logger.warning("Failed to persist Telegram rejection audit: %s", e)

async def find_latest_pumpradar_signal(symbol: Optional[str]) -> Optional[dict]:
    if not symbol:
        return None
    snapshot = await db.signal_snapshots.find_one({}, sort=[("timestamp", -1)])
    if not snapshot:
        return None
    for signal in (snapshot.get("pump_signals", []) + snapshot.get("dump_signals", [])):
        if (signal.get("symbol") or "").upper() == symbol.upper():
            return signal
    return None

async def build_market_alignment(symbol: Optional[str], direction: str) -> tuple[float, Optional[dict]]:
    latest_signal = await find_latest_pumpradar_signal(symbol)
    if not latest_signal:
        return 35.0, None
    latest_direction = latest_signal.get("signal_type") or ("pump" if latest_signal in [] else None)
    if not latest_direction:
        latest_direction = "pump" if latest_signal.get("price_change_24h", 0) >= 0 else "dump"
    signal_strength = float(latest_signal.get("signal_strength") or 50)
    if latest_direction == direction:
        return round(min(100, 55 + signal_strength * 0.45), 2), latest_signal
    return round(max(5, 45 - signal_strength * 0.35), 2), latest_signal

def compute_consensus_score(source_count: int) -> float:
    if source_count <= 1:
        return 25.0
    return float(min(100, 25 + (source_count - 1) * 18))

def compute_telegram_signal_score(source_score: float, parser_confidence: float, market_alignment_score: float, consensus_score: float) -> float:
    return round(
        source_score * 0.35 +
        parser_confidence * 0.25 +
        market_alignment_score * 0.25 +
        consensus_score * 0.15,
        2,
    )

def derive_telegram_source_profile(doc: dict) -> dict:
    source_score = float(doc.get("source_score", 50) or 0)
    verified_count = int(doc.get("verified_count", 0) or 0)
    if source_score >= 80 and verified_count >= 8:
        trust_tier = "elite"
    elif source_score >= 65 and verified_count >= 5:
        trust_tier = "proven"
    elif source_score >= 45:
        trust_tier = "developing"
    else:
        trust_tier = "speculative"

    pump_calls = int(doc.get("pump_calls", 0) or 0)
    dump_calls = int(doc.get("dump_calls", 0) or 0)
    total_calls = max(1, pump_calls + dump_calls)
    pump_share = round((pump_calls / total_calls) * 100, 2)
    dump_share = round((dump_calls / total_calls) * 100, 2)
    if pump_share >= 70:
        bias_label = "Mostly Bullish"
    elif dump_share >= 70:
        bias_label = "Mostly Bearish"
    else:
        bias_label = "Balanced Flow"

    noise_ratio = float(doc.get("noise_ratio", 0) or 0)
    structured_ratio = float(doc.get("structured_ratio", 0) or 0)
    accuracy_1h = float(doc.get("accuracy_1h", 0) or 0)
    accuracy_4h = float(doc.get("accuracy_4h", 0) or 0)
    avg_move_4h_abs = float(doc.get("avg_move_4h_abs", 0) or 0)

    _has_quality_data = doc.get("noise_ratio") is not None and doc.get("structured_ratio") is not None
    if (pump_calls + dump_calls) < 10 or not _has_quality_data:
        quality_badge = "Not enough data"
    elif accuracy_4h >= 65 and noise_ratio <= 25 and verified_count >= 6:
        quality_badge = "High Signal Quality"
    elif accuracy_4h >= 52 and avg_move_4h_abs >= 3 and verified_count >= 4:
        quality_badge = "Fast but Risky"
    elif source_score >= 55 and verified_count >= 8 and accuracy_1h >= 70:
        quality_badge = "Fast but Risky"
    elif noise_ratio >= 45 or structured_ratio <= 45:
        quality_badge = "Mostly Noise"
    elif bias_label == "Mostly Bearish" and accuracy_4h >= 55:
        quality_badge = "Reliable Bearish Source"
    else:
        quality_badge = "Mixed Quality"

    quality_summary = (
        f"{quality_badge}. "
        f"{structured_ratio:.0f}% clean structure, {noise_ratio:.0f}% weak/noisy calls, "
        f"{accuracy_4h:.0f}% 4h hit rate."
    )
    return {
        "trust_tier": trust_tier,
        "pump_calls": pump_calls,
        "dump_calls": dump_calls,
        "pump_share": pump_share,
        "dump_share": dump_share,
        "bias_label": bias_label,
        "quality_badge": quality_badge,
        "quality_summary": quality_summary,
    }

def serialize_telegram_source(doc: dict) -> dict:
    profile = derive_telegram_source_profile(doc)
    return {
        "id": str(doc["_id"]),
        "source_name": doc.get("source_name"),
        "source_handle": doc.get("source_handle"),
        "source_type": doc.get("source_type", "group"),
        "invite_link": doc.get("invite_link"),
        "enabled": doc.get("enabled", True),
        "manual_whitelist": doc.get("manual_whitelist", False),
        "notes": doc.get("notes"),
        "signal_count": doc.get("signal_count", 0),
        "verified_count": doc.get("verified_count", 0),
        "accuracy_1h": doc.get("accuracy_1h", 0),
        "accuracy_4h": doc.get("accuracy_4h", 0),
        "accuracy_24h": doc.get("accuracy_24h", 0),
        "avg_return_1h": doc.get("avg_return_1h", 0),
        "avg_return_4h": doc.get("avg_return_4h", 0),
        "avg_return_24h": doc.get("avg_return_24h", 0),
        "avg_move_1h_abs": doc.get("avg_move_1h_abs", 0),
        "avg_move_4h_abs": doc.get("avg_move_4h_abs", 0),
        "avg_move_24h_abs": doc.get("avg_move_24h_abs", 0),
        "parser_quality_avg": doc.get("parser_quality_avg", 0),
        "market_alignment_avg": doc.get("market_alignment_avg", 0),
        "structured_ratio": doc.get("structured_ratio", 0),
        "noise_ratio": doc.get("noise_ratio", 0),
        "pump_calls": profile["pump_calls"],
        "dump_calls": profile["dump_calls"],
        "pump_share": profile["pump_share"],
        "dump_share": profile["dump_share"],
        "bias_label": profile["bias_label"],
        "quality_badge": profile["quality_badge"],
        "quality_summary": profile["quality_summary"],
        "source_score": doc.get("source_score", 50),
        "trust_tier": profile["trust_tier"],
        "last_signal_at": serialize_datetime(doc.get("last_signal_at")),
        "updated_at": serialize_datetime(doc.get("updated_at")),
    }

def serialize_telegram_signal(doc: dict) -> dict:
    return {
        "id": str(doc["_id"]),
        "source_id": doc.get("source_id"),
        "source_name": doc.get("source_name"),
        "source_handle": doc.get("source_handle"),
        "source_type": doc.get("source_type", "group"),
        "symbol": doc.get("symbol"),
        "direction": doc.get("direction"),
        "chain": doc.get("chain"),
        "contract_address": doc.get("contract_address"),
        "entry_zone": doc.get("entry_zone"),
        "stop_loss": doc.get("stop_loss"),
        "targets": doc.get("targets", []),
        "reference_price": doc.get("reference_price"),
        "parser_confidence": doc.get("parser_confidence", 0),
        "market_alignment_score": doc.get("market_alignment_score", 0),
        "consensus_score": doc.get("consensus_score", 0),
        "cross_source_count": doc.get("cross_source_count", 1),
        "source_score_at_ingest": doc.get("source_score_at_ingest", 50),
        "composite_score": doc.get("composite_score", 0),
        "quality_judge": doc.get("quality_judge"),
        "status": doc.get("status", "pending"),
        "message_text": doc.get("message_text"),
        "message_url": doc.get("message_url"),
        "posted_at": serialize_datetime(doc.get("posted_at")),
        "created_at": serialize_datetime(doc.get("created_at")),
        "updated_at": serialize_datetime(doc.get("updated_at")),
        "verification": {
            key: {
                "due_at": serialize_datetime((doc.get("verification") or {}).get(key, {}).get("due_at")),
                "checked_at": serialize_datetime((doc.get("verification") or {}).get(key, {}).get("checked_at")),
                "return_pct": (doc.get("verification") or {}).get(key, {}).get("return_pct"),
                "hit": (doc.get("verification") or {}).get(key, {}).get("hit"),
                "threshold_pct": (doc.get("verification") or {}).get(key, {}).get("threshold_pct"),
            }
            for key in TELEGRAM_HORIZONS
        },
    }

async def upsert_telegram_source(payload: TelegramSourceUpsertRequest | TelegramSignalIngestRequest) -> dict:
    handle = normalize_telegram_handle(payload.source_handle)
    source_key = handle or payload.source_name.strip().lower()
    now = datetime.now(timezone.utc)
    source = await db.telegram_sources.find_one({"source_key": source_key})
    if source:
        existing_signal_count = int(source.get("signal_count") or 0)
        manual_whitelist = bool(source.get("manual_whitelist"))
        enabled = getattr(payload, "enabled", True)
        notes = getattr(payload, "notes", None)
        if manual_whitelist:
            enabled = True
            notes = source.get("notes") or "Manually whitelisted Telegram source"
        elif existing_signal_count > 0:
            enabled = True
            notes = "Retained because this source already produced parsed signals"
        await db.telegram_sources.update_one(
            {"_id": source["_id"]},
            {"$set": {
                "source_name": payload.source_name.strip(),
                "source_handle": handle,
                "source_type": getattr(payload, "source_type", "group"),
                "invite_link": getattr(payload, "invite_link", None),
                "notes": notes,
                "enabled": enabled,
                "updated_at": now,
            }},
        )
        source = await db.telegram_sources.find_one({"_id": source["_id"]})
        return source

    doc = {
        "source_key": source_key,
        "source_name": payload.source_name.strip(),
        "source_handle": handle,
        "source_type": getattr(payload, "source_type", "group"),
        "invite_link": getattr(payload, "invite_link", None),
        "notes": getattr(payload, "notes", None),
        "enabled": getattr(payload, "enabled", True),
        "signal_count": 0,
        "verified_count": 0,
        "accuracy_1h": 0,
        "accuracy_4h": 0,
        "accuracy_24h": 0,
        "avg_return_1h": 0,
        "avg_return_4h": 0,
        "avg_return_24h": 0,
        "parser_quality_avg": 0,
        "market_alignment_avg": 0,
        "source_score": 50,
        "last_signal_at": None,
        "created_at": now,
        "updated_at": now,
    }
    result = await db.telegram_sources.insert_one(doc)
    doc["_id"] = result.inserted_id
    return doc

async def recalculate_telegram_source_metrics(source_id: str) -> Optional[dict]:
    signals = await db.telegram_signals.find({"source_id": source_id}).to_list(length=1000)
    if not signals:
        return None

    parser_avg = average([signal.get("parser_confidence", 0) for signal in signals])
    alignment_avg = average([signal.get("market_alignment_score", 0) for signal in signals])
    pump_calls = sum(1 for signal in signals if signal.get("direction") == "pump")
    dump_calls = sum(1 for signal in signals if signal.get("direction") == "dump")
    structured_calls = 0
    noisy_calls = 0

    for signal in signals:
        has_plan = bool(signal.get("entry_zone") or signal.get("stop_loss") or (signal.get("targets") or []) or signal.get("contract_address"))
        parser_conf = float(signal.get("parser_confidence") or 0)
        align = float(signal.get("market_alignment_score") or 0)
        if has_plan and parser_conf >= 65:
            structured_calls += 1
        if parser_conf < 55 or align < 40:
            noisy_calls += 1

    horizon_results = {}
    for horizon_key in TELEGRAM_HORIZONS:
        checked = []
        returns = []
        for signal in signals:
            bucket = (signal.get("verification") or {}).get(horizon_key, {})
            if bucket.get("checked_at"):
                checked.append(bool(bucket.get("hit")))
                if bucket.get("return_pct") is not None:
                    returns.append(float(bucket["return_pct"]))
        horizon_results[horizon_key] = {
            "accuracy": round((sum(1 for hit in checked if hit) / len(checked)) * 100, 2) if checked else 0.0,
            "avg_return": average(returns),
            "avg_move_abs": average([abs(value) for value in returns]),
            "samples": len(checked),
        }

    verified_count = max(horizon_results["one_hour"]["samples"], horizon_results["four_hour"]["samples"], horizon_results["twenty_four_hour"]["samples"])
    confidence_factor = min(1.0, 0.35 + verified_count / 20) if verified_count else 0.35
    performance_core = (
        horizon_results["four_hour"]["accuracy"] * 0.45 +
        horizon_results["one_hour"]["accuracy"] * 0.25 +
        horizon_results["twenty_four_hour"]["accuracy"] * 0.20 +
        parser_avg * 0.05 +
        alignment_avg * 0.05
    )
    source_score = round(min(100, performance_core * confidence_factor + min(15, verified_count * 1.5)), 2)

    update = {
        "signal_count": len(signals),
        "verified_count": verified_count,
        "accuracy_1h": horizon_results["one_hour"]["accuracy"],
        "accuracy_4h": horizon_results["four_hour"]["accuracy"],
        "accuracy_24h": horizon_results["twenty_four_hour"]["accuracy"],
        "avg_return_1h": horizon_results["one_hour"]["avg_return"],
        "avg_return_4h": horizon_results["four_hour"]["avg_return"],
        "avg_return_24h": horizon_results["twenty_four_hour"]["avg_return"],
        "avg_move_1h_abs": horizon_results["one_hour"]["avg_move_abs"],
        "avg_move_4h_abs": horizon_results["four_hour"]["avg_move_abs"],
        "avg_move_24h_abs": horizon_results["twenty_four_hour"]["avg_move_abs"],
        "parser_quality_avg": parser_avg,
        "market_alignment_avg": alignment_avg,
        "structured_ratio": round((structured_calls / len(signals)) * 100, 2) if signals else 0.0,
        "noise_ratio": round((noisy_calls / len(signals)) * 100, 2) if signals else 0.0,
        "pump_calls": pump_calls,
        "dump_calls": dump_calls,
        "source_score": source_score,
        "last_signal_at": max([signal.get("posted_at") or signal.get("created_at") for signal in signals if signal.get("posted_at") or signal.get("created_at")], default=None),
        "updated_at": datetime.now(timezone.utc),
    }
    await db.telegram_sources.update_one({"_id": ObjectId(source_id)}, {"$set": update})
    return await db.telegram_sources.find_one({"_id": ObjectId(source_id)})


def classify_telegram_message_quality(message_text: str, symbol: Optional[str] = None, source_score: float = 0) -> dict:
    text = (message_text or "").strip()
    low = text.lower()
    sym = (symbol or "").strip().upper()

    trade_terms = (
        "entry", "entries", "target", "targets", "tp1", "tp2", "tp3", "take profit",
        "stop loss", "sl", "long", "short", "buy zone", "sell zone", "breakout",
        "breakdown", "support", "resistance", "leverage", "scalp", "spot entry",
    )
    promo_terms = (
        "reward", "rewards", "giveaway", "airdrop", "claim", "join us", "event period",
        "how it works", "how to participate", "buy now", "validity duration", "live session",
        "rights zone", "supported wallet", "trust wallet", "metamask", "cex wallet",
        "dex wallet", "official", "community", "announcement",
    )
    proof_terms = (
        "profit", "accurate", "premium call", "vip proof", "vip", "proof",
        "consistency", "contact @", "target achieved", "targets achieved",
        "all target", "all targets", "target -", "freetrail", "free trial",
    )

    false_symbols = {
        "FROM", "MAKE", "LIVE", "MINI", "PUMP", "DUMP", "PROFIT", "TARGET",
        "ENTRY", "SIGNAL", "CALL", "WALLET", "BUY", "SELL", "LONG", "SHORT",
        "PLEASE", "HOW", "OUI", "IT", "ONCE", "UPDATE", "WILL", "SINCE",
        "CHECK", "HELLO", "BONJOUR", "VOUS", "POUR", "OKAY", "OK",
        "IMPORTANT", "QUICK", "SCAM",
    }

    trade_hits = sum(1 for term in trade_terms if term in low)
    promo_hits = sum(1 for term in promo_terms if term in low)
    proof_hits = sum(1 for term in proof_terms if term in low)
    locked_vip = ("🔐" in text) or ("details available on vip" in low) or ("vip channel" in low and "entry" in low)

    label = "unknown"
    is_trade_signal = False
    confidence = 35
    reasons = []

    if sym in false_symbols:
        reasons.append(f"symbol looks like common word/noise: {sym}")

    if locked_vip:
        label = "vip_locked_not_actionable"
        is_trade_signal = False
        confidence = 82
        reasons.append("VIP-locked/redacted setup without actionable entry/SL/targets")

    if trade_hits >= 2 and not locked_vip:
        label = "possible_trade_signal"
        is_trade_signal = True
        confidence = 62
        reasons.append(f"trade terms detected: {trade_hits}")

    if trade_hits >= 3 and sym not in false_symbols and not locked_vip:
        label = "real_trade_signal"
        is_trade_signal = True
        confidence = 78
        reasons.append("structured trade setup detected")

    actionable_terms = (
        "entry", "entries", "buy zone", "sell zone", "stop loss", "sl",
        "long", "short", "open long", "open short", "leverage", "spot entry",
    )
    actionable_hits = sum(1 for term in actionable_terms if term in low)

    if proof_hits >= 2 and actionable_hits == 0:
        label = "performance_proof_or_marketing"
        is_trade_signal = False
        confidence = 72
        reasons.append("profit/proof marketing without actionable entry/SL setup")

    if promo_hits >= 2 and trade_hits == 0:
        label = "official_update_or_noise"
        is_trade_signal = False
        confidence = 72
        reasons.append(f"promo/official terms detected: {promo_hits}")

    if sym in false_symbols and trade_hits < 3:
        label = "noise_false_symbol"
        is_trade_signal = False
        confidence = max(confidence, 75)
        reasons.append("false ticker extraction likely")

    if source_score >= 55 and is_trade_signal:
        confidence = min(90, confidence + 8)
        reasons.append("source has useful historical score")

    return {
        "label": label,
        "is_trade_signal": is_trade_signal,
        "confidence": confidence,
        "reasons": reasons,
        "classifier": "local_v1",
    }


async def ingest_telegram_signal_payload(
    *,
    source: dict,
    message_text: str,
    message_id: Optional[str] = None,
    message_url: Optional[str] = None,
    posted_at: Optional[datetime] = None,
) -> dict:
    posted_at = posted_at or datetime.now(timezone.utc)
    parsed = parse_telegram_signal_message(message_text)
    reject_reasons = explain_telegram_signal_rejection(parsed)
    if reject_reasons:
        await record_telegram_signal_rejection(
            source=source,
            message_text=message_text,
            parsed=parsed,
            reasons=reject_reasons,
            stage="parser_gate",
            message_id=message_id,
            message_url=message_url,
            posted_at=posted_at,
        )
        raise ValueError(f"Telegram message rejected: {', '.join(reject_reasons)}")
    source_score = float(source.get("source_score", 50))
    market_alignment_score, latest_signal = await build_market_alignment(parsed.get("symbol"), parsed.get("direction") or "pump")
    reference_price = None
    coin_name = None
    if latest_signal:
        reference_price = latest_signal.get("price")
        coin_name = latest_signal.get("name")

    coin_id = resolve_coingecko_coin_id(parsed.get("symbol") or "", preferred_name=coin_name) if parsed.get("symbol") else ""
    cluster_key = f"{(parsed.get('symbol') or 'unknown').upper()}:{parsed.get('direction') or 'pump'}"
    window_start = posted_at - timedelta(minutes=30)
    window_end = posted_at + timedelta(minutes=30)
    recent_cluster = await db.telegram_signals.find({
        "cluster_key": cluster_key,
        "posted_at": {"$gte": window_start, "$lte": window_end},
    }).to_list(length=100)
    source_ids = {signal.get("source_id") for signal in recent_cluster if signal.get("source_id")}
    source_ids.add(str(source["_id"]))
    consensus_score = compute_consensus_score(len(source_ids))
    composite_score = compute_telegram_signal_score(source_score, parsed["parser_confidence"], market_alignment_score, consensus_score)
    quality_judge = classify_telegram_message_quality(
        message_text=message_text,
        symbol=parsed.get("symbol"),
        source_score=source_score,
    )

    verification = {
        key: {
            "due_at": posted_at + timedelta(hours=horizon["hours"]),
            "checked_at": None,
            "return_pct": None,
            "hit": None,
            "threshold_pct": horizon["threshold_pct"],
        }
        for key, horizon in TELEGRAM_HORIZONS.items()
    }

    doc = {
        "source_id": str(source["_id"]),
        "source_name": source.get("source_name"),
        "source_handle": source.get("source_handle"),
        "source_type": source.get("source_type", "group"),
        "message_id": message_id,
        "message_url": message_url,
        "message_text": message_text,
        "posted_at": posted_at,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
        "symbol": parsed.get("symbol"),
        "direction": parsed.get("direction"),
        "chain": parsed.get("chain"),
        "contract_address": parsed.get("contract_address"),
        "entry_zone": parsed.get("entry_zone"),
        "stop_loss": parsed.get("stop_loss"),
        "targets": parsed.get("targets", []),
        "parser_confidence": parsed.get("parser_confidence", 0),
        "market_alignment_score": market_alignment_score,
        "consensus_score": consensus_score,
        "cross_source_count": len(source_ids),
        "source_score_at_ingest": source_score,
        "composite_score": composite_score,
        "quality_judge": quality_judge,
        "status": "pending",
        "verification": verification,
        "cluster_key": cluster_key,
        "reference_price": reference_price,
        "coin_name": coin_name,
        "coin_id": coin_id,
    }
    result = await db.telegram_signals.insert_one(doc)
    doc["_id"] = result.inserted_id

    await db.telegram_signals.update_many(
        {"cluster_key": cluster_key, "posted_at": {"$gte": window_start, "$lte": window_end}},
        {"$set": {"cross_source_count": len(source_ids), "consensus_score": consensus_score}},
    )
    await recalculate_telegram_source_metrics(str(source["_id"]))
    return doc

def fetch_market_chart_points(coin_id: str, days: int = 2) -> List[dict]:
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
        params = {"vs_currency": "usd", "days": days, "interval": "hourly"}
        r = requests.get(url, params=params, headers=CG_HEADERS, timeout=15)
        if r.status_code != 200:
            return []
        prices = (r.json() or {}).get("prices", []) or []
        return [
            {
                "timestamp": datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                "price": float(price),
            }
            for ts, price in prices
        ]
    except Exception as e:
        logger.error(f"Telegram chart verification error for {coin_id}: {e}")
        return []

def nearest_price_at(points: List[dict], target_dt: datetime) -> Optional[float]:
    if not points:
        return None
    nearest = min(points, key=lambda item: abs((item["timestamp"] - target_dt).total_seconds()))
    return nearest.get("price")

async def evaluate_pending_telegram_signals():
    try:
        now = datetime.now(timezone.utc)
        signals = await db.telegram_signals.find({
            "status": {"$in": ["pending", "partially_verified"]},
            "symbol": {"$ne": None},
            "reference_price": {"$gt": 0},
        }).sort("posted_at", 1).limit(200).to_list(length=200)

        touched_sources = set()
        for signal in signals:
            verification = signal.get("verification") or {}
            due_now = [
                key for key, horizon in TELEGRAM_HORIZONS.items()
                if normalize_datetime((verification.get(key) or {}).get("due_at"))
                and normalize_datetime((verification.get(key) or {}).get("due_at")) <= now
                and not (verification.get(key) or {}).get("checked_at")
            ]
            if not due_now:
                continue

            coin_id = signal.get("coin_id") or resolve_coingecko_coin_id(signal.get("symbol") or "", signal.get("coin_name"))
            points = fetch_market_chart_points(coin_id, days=2)
            if not points:
                continue

            updates = {}
            checked_count = 0
            for key in TELEGRAM_HORIZONS:
                bucket = verification.get(key) or {}
                if bucket.get("checked_at"):
                    checked_count += 1
            for key in due_now:
                horizon = TELEGRAM_HORIZONS[key]
                due_at = normalize_datetime((verification.get(key) or {}).get("due_at"))
                if not due_at:
                    continue
                target_price = nearest_price_at(points, due_at)
                if target_price is None:
                    continue
                reference_price = float(signal.get("reference_price") or 0)
                if reference_price <= 0:
                    continue
                return_pct = round(((target_price - reference_price) / reference_price) * 100, 2)
                hit = return_pct >= horizon["threshold_pct"] if signal.get("direction") == "pump" else return_pct <= -horizon["threshold_pct"]
                updates[f"verification.{key}.checked_at"] = now
                updates[f"verification.{key}.return_pct"] = return_pct
                updates[f"verification.{key}.hit"] = hit
                updates[f"verification.{key}.threshold_pct"] = horizon["threshold_pct"]
                checked_count += 1

            if not updates:
                continue

            new_status = "verified" if checked_count >= len(TELEGRAM_HORIZONS) else "partially_verified"
            updates["status"] = new_status
            updates["coin_id"] = coin_id
            updates["updated_at"] = now
            await db.telegram_signals.update_one({"_id": signal["_id"]}, {"$set": updates})
            if signal.get("source_id"):
                touched_sources.add(signal["source_id"])

        for source_id in touched_sources:
            await recalculate_telegram_source_metrics(source_id)
    except Exception as e:
        logger.error(f"Telegram signal verification job failed: {e}")

async def handle_telegram_message_event(event):
    try:
        chat = await event.get_chat()
        if not is_telegram_collectable_chat(chat):
            return
        chat_title = getattr(chat, "title", None) or getattr(chat, "first_name", None) or getattr(chat, "username", None)
        chat_username = getattr(chat, "username", None)
        sources = await get_enabled_telegram_sources()
        source = next((item for item in sources if matches_telegram_source(item, chat_title, chat_username)), None)
        if not source:
            payload = build_telegram_source_payload(chat)
            if not payload:
                return
            source = await upsert_telegram_source(payload)
        if not source.get("enabled", True):
            return
        message_text = (event.raw_text or "").strip()
        if not message_text:
            return
        parsed = parse_telegram_signal_message(message_text)
        reject_reasons = explain_telegram_signal_rejection(parsed)
        if reject_reasons:
            message_url = f"https://t.me/{chat_username}/{event.id}" if chat_username else None
            posted_at = event.date if isinstance(event.date, datetime) else datetime.now(timezone.utc)
            if posted_at.tzinfo is None:
                posted_at = posted_at.replace(tzinfo=timezone.utc)
            await record_telegram_signal_rejection(
                source=source,
                message_text=message_text,
                parsed=parsed,
                reasons=reject_reasons,
                stage="live_ingest_gate",
                message_id=str(event.id),
                message_url=message_url,
                posted_at=posted_at,
            )
            return
        message_url = f"https://t.me/{chat_username}/{event.id}" if chat_username else None
        posted_at = event.date if isinstance(event.date, datetime) else datetime.now(timezone.utc)
        if posted_at.tzinfo is None:
            posted_at = posted_at.replace(tzinfo=timezone.utc)
        await ingest_telegram_signal_payload(
            source=source,
            message_text=message_text,
            message_id=str(event.id),
            message_url=message_url,
            posted_at=posted_at,
        )
    except Exception as e:
        logger.error(f"Telegram live ingest error: {e}")

def build_telegram_consensus_payload(signals: List[dict], sources: List[dict], hours: int) -> dict:
    symbol_map: Dict[str, dict] = {}
    bullish_mentions = 0
    bearish_mentions = 0
    allowed_quality_labels = {"real_trade_signal", "possible_trade_signal"}

    filtered_signals = []
    ignored_by_quality = 0
    for signal in signals:
        quality = signal.get("quality_judge") or {}
        if quality and quality.get("label") not in allowed_quality_labels:
            ignored_by_quality += 1
            continue
        if quality and quality.get("is_trade_signal") is False:
            ignored_by_quality += 1
            continue
        filtered_signals.append(signal)

    for signal in filtered_signals:
        symbol = (signal.get("symbol") or "").upper()
        if not symbol:
            continue
        group = symbol_map.setdefault(symbol, {
            "symbol": symbol,
            "mentions": 0,
            "bullish_mentions": 0,
            "bearish_mentions": 0,
            "source_names": set(),
            "scores": [],
            "latest_posted_at": None,
        })
        group["mentions"] += 1
        if signal.get("direction") == "dump":
            bearish_mentions += 1
            group["bearish_mentions"] += 1
        else:
            bullish_mentions += 1
            group["bullish_mentions"] += 1
        if signal.get("source_name"):
            group["source_names"].add(signal["source_name"])
        if signal.get("composite_score") is not None:
            group["scores"].append(float(signal["composite_score"]))
        posted_at = normalize_datetime(signal.get("posted_at"))
        if posted_at and (group["latest_posted_at"] is None or posted_at > group["latest_posted_at"]):
            group["latest_posted_at"] = posted_at

    hot_symbols = []
    for item in symbol_map.values():
        unique_sources = len(item["source_names"])
        avg_score = round(sum(item["scores"]) / len(item["scores"]), 2) if item["scores"] else 0.0
        if item["bullish_mentions"] and item["bearish_mentions"]:
            stance = "mixed"
        elif item["bearish_mentions"] > item["bullish_mentions"]:
            stance = "bearish"
        else:
            stance = "bullish"
        if item["mentions"] >= 4 or unique_sources >= 3:
            rumor_level = "high"
        elif item["mentions"] >= 2 or unique_sources >= 2:
            rumor_level = "medium"
        else:
            rumor_level = "low"
        hot_symbols.append({
            "symbol": item["symbol"],
            "mentions": item["mentions"],
            "bullish_mentions": item["bullish_mentions"],
            "bearish_mentions": item["bearish_mentions"],
            "unique_sources": unique_sources,
            "avg_score": avg_score,
            "stance": stance,
            "rumor_level": rumor_level,
            "source_names": sorted(item["source_names"]),
            "latest_posted_at": serialize_datetime(item["latest_posted_at"]),
        })

    hot_symbols.sort(
        key=lambda item: (
            item["mentions"],
            item["unique_sources"],
            item["avg_score"],
        ),
        reverse=True,
    )
    active_source_names = [source.get("source_name") for source in sources if source.get("source_name")]
    if hot_symbols:
        top = hot_symbols[:3]
        top_labels = ", ".join(item["symbol"] for item in top)
        headline = (
            f"Telegram chatter across {len(active_source_names)} signal-grade channels is concentrated around {top_labels} "
            f"over the last {hours}h. Treat this as crowd-flow context, not a confirmed trading signal."
        )
    else:
        headline = (
            f"No clean repeated Telegram chatter detected across the {len(active_source_names)} signal-grade channels "
            f"in the last {hours}h."
        )

    return {
        "headline": headline,
        "hours": hours,
        "active_sources": active_source_names,
        "signal_count": len(filtered_signals),
        "raw_signal_count": len(signals),
        "ignored_by_quality": ignored_by_quality,
        "bullish_mentions": bullish_mentions,
        "bearish_mentions": bearish_mentions,
        "hot_symbols": hot_symbols[:8],
    }

async def start_telegram_listener():
    global telegram_client, telegram_listener_task
    runtime = telegram_runtime_status()
    if not TelegramClient or not runtime["api_id_configured"] or not runtime["api_hash_configured"] or not runtime["phone_configured"]:
        logger.info("Telegram listener not started - credentials or Telethon missing")
        return
    if not TELEGRAM_LIVE_ENABLED:
        logger.info("Telegram listener disabled by TELEGRAM_LIVE_ENABLED")
        return
    if telegram_listener_task and not telegram_listener_task.done():
        return

    client = await create_telegram_client()
    if not client:
        return
    authorized = await client.is_user_authorized()
    telegram_auth_state["authorized"] = bool(authorized)
    if not authorized:
        await client.disconnect()
        logger.info("Telegram listener not started - session not authorized yet")
        return

    synced = await sync_telegram_dialog_sources(client)
    logger.info(f"Telegram dialog source sync complete: {synced} chats indexed")
    client.add_event_handler(handle_telegram_message_event, events.NewMessage(incoming=True))
    telegram_client = client
    telegram_listener_task = asyncio.create_task(client.run_until_disconnected())
    logger.info("Telegram live listener started")

@app.post("/api/admin/telegram/auth/request-code")
async def admin_request_telegram_code(admin=Depends(require_admin)):
    if not TelegramClient:
        raise HTTPException(status_code=503, detail=api_err("Telethon is not installed", "TELEGRAM_CLIENT_MISSING"))
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_PHONE:
        raise HTTPException(status_code=400, detail=api_err("Telegram credentials are incomplete", "TELEGRAM_CONFIG_MISSING"))

    client = await create_telegram_client()
    telegram_auth_state["client"] = client
    if await client.is_user_authorized():
        telegram_auth_state["authorized"] = True
        return api_ok({"message": "Telegram session is already authorized"})

    sent = await client.send_code_request(TELEGRAM_PHONE)
    telegram_auth_state["phone_code_hash"] = sent.phone_code_hash
    telegram_auth_state["authorized"] = False
    return api_ok({"message": "Telegram login code sent to the Telegram app for this phone number."})

@app.post("/api/admin/telegram/auth/complete")
async def admin_complete_telegram_auth(req: TelegramAuthCodeRequest, admin=Depends(require_admin)):
    client = telegram_auth_state.get("client")
    if not client:
        client = await create_telegram_client()
    if not client:
        raise HTTPException(status_code=503, detail=api_err("Telegram client is unavailable", "TELEGRAM_CLIENT_MISSING"))
    try:
        await client.sign_in(
            phone=TELEGRAM_PHONE,
            code=req.code,
            phone_code_hash=telegram_auth_state.get("phone_code_hash"),
        )
    except SessionPasswordNeededError:
        if not req.password:
            raise HTTPException(status_code=400, detail=api_err("Telegram account requires 2FA password", "TELEGRAM_PASSWORD_REQUIRED"))
        await client.sign_in(password=req.password)

    telegram_auth_state["authorized"] = await client.is_user_authorized()
    await client.disconnect()
    telegram_auth_state.pop("client", None)
    telegram_auth_state.pop("phone_code_hash", None)
    await start_telegram_listener()
    return api_ok({"message": "Telegram session authorized successfully"})

@app.get("/api/telegram/status")
async def telegram_status(user=Depends(require_active_subscription)):
    source_count = await db.telegram_sources.count_documents({})
    active_source_count = await db.telegram_sources.count_documents({"enabled": True})
    pending_count = await db.telegram_signals.count_documents({"status": {"$in": ["pending", "partially_verified"]}})
    verified_count = await db.telegram_signals.count_documents({"status": "verified"})
    return api_ok({
        "runtime": telegram_runtime_status(),
        "sources": {"total": source_count, "active": active_source_count},
        "signals": {"pending": pending_count, "verified": verified_count},
    })

@app.get("/api/telegram/sources")
async def telegram_sources(user=Depends(require_active_subscription)):
    docs = await db.telegram_sources.find({}).sort("source_score", -1).to_list(length=250)
    return api_ok({"sources": [serialize_telegram_source(doc) for doc in docs]})

@app.get("/api/telegram/signals")
async def telegram_signals(limit: int = 50, status: Optional[str] = None, user=Depends(require_active_subscription)):
    query = {}
    if status:
        query["status"] = status
    docs = await db.telegram_signals.find(query).sort("posted_at", -1).limit(limit).to_list(length=limit)
    summary = {
        "total": await db.telegram_signals.count_documents(query),
        "pending": await db.telegram_signals.count_documents({**query, "status": {"$in": ["pending", "partially_verified"]}}),
        "verified": await db.telegram_signals.count_documents({**query, "status": "verified"}),
        "pump": await db.telegram_signals.count_documents({**query, "direction": "pump"}),
        "dump": await db.telegram_signals.count_documents({**query, "direction": "dump"}),
    }
    return api_ok({"signals": [serialize_telegram_signal(doc) for doc in docs], "summary": summary})

@app.get("/api/telegram/consensus")
async def telegram_consensus(hours: int = 24, user=Depends(require_active_subscription)):
    hours = max(1, min(hours, 72))
    cache_key = f"telegram_consensus::{hours}"
    cached = get_memory_cache(TELEGRAM_CONSENSUS_CACHE, cache_key, ttl_seconds=300)
    if cached is not None:
        return api_ok(cached)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    sources = await get_enabled_telegram_sources()
    dashboard_sources = [
        source for source in sources
        if derive_telegram_source_profile(source).get("quality_badge") in {"High Signal Quality", "Fast but Risky"}
    ]
    source_ids = [str(source["_id"]) for source in dashboard_sources]
    if not source_ids:
        return api_ok(set_memory_cache(TELEGRAM_CONSENSUS_CACHE, cache_key, build_telegram_consensus_payload([], dashboard_sources, hours)))
    signals = await db.telegram_signals.find({
        "source_id": {"$in": source_ids},
        "posted_at": {"$gte": cutoff},
    }).sort("posted_at", -1).limit(300).to_list(length=300)
    return api_ok(set_memory_cache(TELEGRAM_CONSENSUS_CACHE, cache_key, build_telegram_consensus_payload(signals, dashboard_sources, hours)))

@app.post("/api/admin/telegram/sources")
async def admin_upsert_telegram_source(req: TelegramSourceUpsertRequest, admin=Depends(require_admin)):
    source = await upsert_telegram_source(req)
    source = await recalculate_telegram_source_metrics(str(source["_id"])) or source
    return api_ok({"source": serialize_telegram_source(source)})

@app.post("/api/admin/telegram/signals/ingest")
async def admin_ingest_telegram_signal(req: TelegramSignalIngestRequest, admin=Depends(require_admin)):
    posted_at = normalize_datetime(req.posted_at) or datetime.now(timezone.utc)
    source = None
    if req.source_id:
        source = await db.telegram_sources.find_one({"_id": ObjectId(req.source_id)})
    if not source:
        source = await upsert_telegram_source(req)
    try:
        doc = await ingest_telegram_signal_payload(
            source=source,
            message_text=req.message_text,
            message_id=req.message_id,
            message_url=req.message_url,
            posted_at=posted_at,
        )
    except ValueError as e:
        parsed = parse_telegram_signal_message(req.message_text)
        raise HTTPException(status_code=400, detail=api_err(str(e), "TELEGRAM_SIGNAL_REJECTED"))
    parsed = parse_telegram_signal_message(req.message_text)
    return api_ok({"signal": serialize_telegram_signal(doc), "parser": parsed})

@app.post("/api/admin/telegram/signals/{signal_id}/manual-outcome")
async def admin_manual_telegram_outcome(signal_id: str, req: TelegramManualOutcomeRequest, admin=Depends(require_admin)):
    signal = await db.telegram_signals.find_one({"_id": ObjectId(signal_id)})
    if not signal:
        raise HTTPException(status_code=404, detail=api_err("Telegram signal not found", "NOT_FOUND"))

    now = datetime.now(timezone.utc)
    updates = {"updated_at": now}
    payload = {
        "one_hour": req.return_1h_pct,
        "four_hour": req.return_4h_pct,
        "twenty_four_hour": req.return_24h_pct,
    }
    checked_count = 0
    for key, return_pct in payload.items():
        if return_pct is None:
            continue
        threshold = TELEGRAM_HORIZONS[key]["threshold_pct"]
        hit = return_pct >= threshold if signal.get("direction") == "pump" else return_pct <= -threshold
        updates[f"verification.{key}.checked_at"] = now
        updates[f"verification.{key}.return_pct"] = return_pct
        updates[f"verification.{key}.hit"] = hit
        checked_count += 1
    if checked_count == 0:
        raise HTTPException(status_code=400, detail=api_err("No outcome values were provided", "INVALID_REQUEST"))
    updates["status"] = "verified" if checked_count >= len(TELEGRAM_HORIZONS) else "partially_verified"
    await db.telegram_signals.update_one({"_id": signal["_id"]}, {"$set": updates})
    if signal.get("source_id"):
        await recalculate_telegram_source_metrics(signal["source_id"])
    signal = await db.telegram_signals.find_one({"_id": signal["_id"]})
    return api_ok({"signal": serialize_telegram_signal(signal)})

@app.post("/api/admin/telegram/recalculate")
async def admin_recalculate_telegram(admin=Depends(require_admin)):
    await evaluate_pending_telegram_signals()
    sources = await db.telegram_sources.find({}).to_list(length=500)
    for source in sources:
        await recalculate_telegram_source_metrics(str(source["_id"]))
    return api_ok({"message": "Telegram source scores recalculated"})

@app.get("/api/admin/telegram/calibration")
async def admin_telegram_calibration(hours: int = 72, admin=Depends(require_admin)):
    return api_ok(await build_telegram_calibration_summary(hours))

# ─────────────────────────────────────────────
# ADMIN ENDPOINTS
# ─────────────────────────────────────────────
async def require_admin(user=Depends(get_current_user)):
    if "admin" not in user.get("roles", []):
        raise HTTPException(status_code=403, detail=api_err("Admin access required", "FORBIDDEN"))
    return user

async def list_users_payload(skip: int = 0, limit: int = 100) -> dict:
    users = await db.users.find({}).skip(skip).limit(limit).to_list(length=limit)
    return api_ok({"users": [doc_to_user(u) for u in users], "total": await db.users.count_documents({})})

async def delete_user_by_id(user_id: str) -> dict:
    result = await db.users.delete_one({"_id": ObjectId(user_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail=api_err("User not found", "NOT_FOUND"))
    return api_ok({"message": "User deleted"})

async def update_user_by_id(user_id: str, body: dict) -> dict:
    update = {}
    if "subscription" in body:
        update["subscription"] = body["subscription"]

        duration = body.get("duration")
        if duration == "month":
            update["subscription_expiry"] = datetime.now(timezone.utc) + timedelta(days=30)
        elif duration == "year":
            update["subscription_expiry"] = datetime.now(timezone.utc) + timedelta(days=365)
        else:
            plan = SUBSCRIPTION_PLANS.get(body["subscription"])
            if plan:
                update["subscription_expiry"] = datetime.now(timezone.utc) + timedelta(days=plan["duration_days"])

    if "roles" in body:
        update["roles"] = body["roles"]
    if "name" in body:
        update["name"] = body["name"]
    if not update:
        raise HTTPException(status_code=400, detail="Nothing to update")
    await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": update})
    user = await db.users.find_one({"_id": ObjectId(user_id)})
    return api_ok({"user": doc_to_user(user)})

@app.get("/api/admin/users")
async def admin_list_users(skip: int = 0, limit: int = 100, admin=Depends(require_admin)):
    return await list_users_payload(skip, limit)

@app.delete("/api/admin/users/{user_id}")
async def admin_delete_user(user_id: str, admin=Depends(require_admin)):
    return await delete_user_by_id(user_id)

@app.patch("/api/admin/users/{user_id}")
async def admin_update_user(user_id: str, body: dict, admin=Depends(require_admin)):
    return await update_user_by_id(user_id, body)

@app.post("/api/admin/make-admin/{user_id}")
async def make_admin(user_id: str, admin=Depends(require_admin)):
    await db.users.update_one({"_id": ObjectId(user_id)}, {"$addToSet": {"roles": "admin"}})
    return api_ok({"message": "User promoted to admin"})

@app.post("/api/admin/run-signal-job")
async def run_signal_job(admin=Depends(require_admin)):
    """Force run the signal analysis job (admin only)"""
    try:
        await run_full_scan(db)
        return api_ok({"message": "Signal job completed successfully"})
    except Exception as e:
        logger.error(f"Manual signal job failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/super-admin/users")
async def super_admin_list_users(skip: int = 0, limit: int = 100, admin=Depends(get_current_super_admin)):
    return await list_users_payload(skip, limit)

@app.delete("/api/super-admin/users/{user_id}")
async def super_admin_delete_user(user_id: str, admin=Depends(get_current_super_admin)):
    return await delete_user_by_id(user_id)

@app.patch("/api/super-admin/users/{user_id}")
async def super_admin_update_user(user_id: str, body: dict, admin=Depends(get_current_super_admin)):
    return await update_user_by_id(user_id, body)

@app.post("/api/super-admin/run-signal-job")
async def super_admin_run_signal_job(admin=Depends(get_current_super_admin)):
    try:
        await run_full_scan(db)
        return api_ok({"message": "Signal job completed successfully"})
    except Exception as e:
        logger.error(f"Super admin signal job failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────
# HOME MODULE STUBS (required by Katalyst template)
# ─────────────────────────────────────────────
@app.get("/api/home/dashboard")
async def home_dashboard(user=Depends(get_current_user)):
    """Home dashboard data for Katalyst home module"""
    u = doc_to_user(user)
    return api_ok({
        "user": {"name": u.get("name", "User"), "email": u["email"], "avatarUrl": None},
        "workspace": {"name": "PumpRadar", "environment": "production"},
        "stats": {"pumpSignals": 0, "dumpSignals": 0, "users": 1},
        "checklist": [],
        "recentActivity": [],
        "apps": [],
        "tourCompleted": True,
    })

@app.patch("/api/home/checklist/{item_id}")
async def update_checklist(item_id: str, user=Depends(get_current_user)):
    return api_ok({"message": "OK"})

@app.get("/api/home/tour")
async def get_tour(user=Depends(get_current_user)):
    return api_ok({"completed": True, "skipped": True})

@app.post("/api/home/tour/{action}")
async def tour_action(action: str, user=Depends(get_current_user)):
    return api_ok({"completed": True, "skipped": True})


# ─────────────────────────────────────────────
# TOKEN OSINT LAB - PREVIEW SCAN ENDPOINT
# ─────────────────────────────────────────────
class TokenOsintScanRequest(BaseModel):
    query: str
    chain: str | None = None


def detect_osint_query_type(query: str) -> str:
    value = (query or "").strip()
    clean = value

    if not value:
        return "unknown"

    if value.lower().startswith("http://") or value.lower().startswith("https://"):
        return "project_url"

    if value.startswith("0x") and len(value) == 42:
        return "evm_contract"

    base58_chars = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    if 32 <= len(clean) <= 44 and all(ch in base58_chars for ch in clean):
        return "solana_mint"

    if len(value) <= 16 and value.replace("$", "").replace("-", "").replace("_", "").isalnum():
        return "symbol"

    return "unknown"


DEXSCREENER_BASE_URL = "https://api.dexscreener.com"


def _safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _choose_best_dex_pair(pairs):
    if not pairs:
        return None

    def score(pair):
        liquidity = _safe_float((pair.get("liquidity") or {}).get("usd"), 0) or 0
        volume = _safe_float((pair.get("volume") or {}).get("h24"), 0) or 0
        return liquidity + (volume * 0.05)

    return sorted(pairs, key=score, reverse=True)[0]


def _normalize_dex_pair(pair):
    if not pair:
        return None

    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}
    liquidity = pair.get("liquidity") or {}
    volume = pair.get("volume") or {}
    price_change = pair.get("priceChange") or {}

    return {
        "source": "DexScreener",
        "chain": pair.get("chainId"),
        "dex": pair.get("dexId"),
        "pair_address": pair.get("pairAddress"),
        "pair_url": pair.get("url"),
        "base_token": {
            "name": base.get("name"),
            "symbol": base.get("symbol"),
            "address": base.get("address"),
        },
        "quote_token": {
            "name": quote.get("name"),
            "symbol": quote.get("symbol"),
            "address": quote.get("address"),
        },
        "price_usd": _safe_float(pair.get("priceUsd")),
        "price_native": pair.get("priceNative"),
        "liquidity_usd": _safe_float(liquidity.get("usd")),
        "volume_24h": _safe_float(volume.get("h24")),
        "price_change_24h": _safe_float(price_change.get("h24")),
        "fdv": _safe_float(pair.get("fdv")),
        "market_cap": _safe_float(pair.get("marketCap")),
        "created_at": pair.get("pairCreatedAt"),
    }


def lookup_dexscreener_osint(query: str, query_type: str, chain: str):
    value = query.strip()
    normalized_chain = (chain or "ethereum").lower()

    try:
        if query_type in {"evm_contract", "solana_mint"}:
            url = f"{DEXSCREENER_BASE_URL}/token-pairs/v1/{normalized_chain}/{value}"
            response = requests.get(url, timeout=12)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            pairs = data if isinstance(data, list) else data.get("pairs", [])

        elif query_type == "symbol":
            symbol = value.replace("$", "").upper()
            url = f"{DEXSCREENER_BASE_URL}/latest/dex/search"
            response = requests.get(url, params={"q": symbol}, timeout=12)
            response.raise_for_status()
            data = response.json()
            raw_pairs = data.get("pairs", []) if isinstance(data, dict) else []

            exact_pairs = [
                pair for pair in raw_pairs
                if ((pair.get("baseToken") or {}).get("symbol") or "").upper() == symbol
            ]
            pairs = exact_pairs or raw_pairs

        else:
            return None

        best_pair = _choose_best_dex_pair(pairs)
        return _normalize_dex_pair(best_pair)

    except Exception as exc:
        logger.warning(f"Token OSINT DexScreener lookup failed for query={query}: {exc}")
        return None


def fetch_dexscreener_trending_pumps(limit: int = 50) -> List[dict]:
    """Fetch trending DEX pairs with early pump signals from DexScreener."""
    results = []
    try:
        endpoints = [
            "https://api.dexscreener.com/token-boosts/top/v1",
        ]
        boosted_addresses = set()
        boost_resp = requests.get("https://api.dexscreener.com/token-boosts/top/v1", timeout=10)
        if boost_resp.status_code == 200:
            for item in boost_resp.json():
                addr = item.get("tokenAddress")
                if addr:
                    boosted_addresses.add(addr.lower())
        chains = ["solana", "ethereum", "bsc"]
        pairs = []
        for chain in chains:
            try:
                resp = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{chain}", timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    pairs.extend(data.get("pairs", []) or [])
            except Exception:
                pass
        search_resp = requests.get("https://api.dexscreener.com/latest/dex/search", params={"q": "pump"}, timeout=10)
        if search_resp.status_code == 200:
            pairs.extend(search_resp.json().get("pairs", []) or [])
        seen = set()
        for pair in pairs:
            pair_addr = pair.get("pairAddress", "")
            if pair_addr in seen:
                continue
            seen.add(pair_addr)
            txns = pair.get("txns") or {}
            h1 = txns.get("h1") or {}
            m5 = txns.get("m5") or {}
            volume = pair.get("volume") or {}
            liquidity = pair.get("liquidity") or {}
            price_change = pair.get("priceChange") or {}
            buys_h1 = int(h1.get("buys") or 0)
            sells_h1 = int(h1.get("sells") or 0)
            buys_m5 = int(m5.get("buys") or 0)
            sells_m5 = int(m5.get("sells") or 0)
            vol_h1 = float(volume.get("h1") or 0)
            vol_h24 = float(volume.get("h24") or 0)
            liq_usd = float(liquidity.get("usd") or 0)
            pc_h1 = float(price_change.get("h1") or 0)
            pc_h24 = float(price_change.get("h24") or 0)
            base = pair.get("baseToken") or {}
            symbol = (base.get("symbol") or "").upper()
            if not symbol or liq_usd < 5000:
                continue
            buy_pressure = buys_h1 > max(sells_h1 * 1.5, 3)
            vol_acceleration = vol_h24 > 0 and (vol_h1 / vol_h24) > 0.12
            price_moving = pc_h1 > 1.5
            m5_active = buys_m5 > sells_m5 and buys_m5 >= 2
            is_boosted = (base.get("address") or "").lower() in boosted_addresses
            score = (
                (20 if buy_pressure else 0) +
                (20 if vol_acceleration else 0) +
                (20 if price_moving else 0) +
                (15 if m5_active else 0) +
                (15 if is_boosted else 0) +
                (10 if liq_usd > 50000 else 0)
            )
            if score < 35:
                continue
            results.append({
                "symbol": symbol,
                "name": base.get("name", symbol),
                "contract_address": base.get("address"),
                "chain": pair.get("chainId"),
                "dex": pair.get("dexId"),
                "pair_url": pair.get("url"),
                "price_usd": float(pair.get("priceUsd") or 0),
                "price_change_1h": pc_h1,
                "price_change_24h": pc_h24,
                "volume_h1": vol_h1,
                "volume_h24": vol_h24,
                "liquidity_usd": liq_usd,
                "buys_h1": buys_h1,
                "sells_h1": sells_h1,
                "buys_m5": buys_m5,
                "sells_m5": sells_m5,
                "is_boosted": is_boosted,
                "dex_score": score,
                "source": "dexscreener_trending",
            })
        results.sort(key=lambda x: x["dex_score"], reverse=True)
        return results[:limit]
    except Exception as e:
        logger.warning(f"DexScreener trending fetch failed: {e}")
        return []


def _liquidity_health_score(liquidity_usd):
    liquidity = _safe_float(liquidity_usd, 0) or 0
    if liquidity >= 1_000_000:
        return 85
    if liquidity >= 250_000:
        return 70
    if liquidity >= 75_000:
        return 55
    if liquidity >= 20_000:
        return 40
    if liquidity > 0:
        return 25
    return None


def _basic_osint_verdict(dex_data):
    if not dex_data:
        return {
            "label": "Pending",
            "confidence": 0,
            "summary": "No external OSINT market source returned data yet. More sources are needed before a verdict."
        }

    liquidity = _safe_float(dex_data.get("liquidity_usd"), 0) or 0
    volume = _safe_float(dex_data.get("volume_24h"), 0) or 0
    change = _safe_float(dex_data.get("price_change_24h"), 0) or 0

    if liquidity < 20_000:
        return {
            "label": "Risky",
            "confidence": 55,
            "summary": "DexScreener found the token, but liquidity is very low. Treat this as high risk until contract safety and holder checks are added."
        }

    if volume > liquidity * 2 and abs(change) > 20:
        return {
            "label": "Monitor",
            "confidence": 62,
            "summary": "DexScreener shows active trading and strong movement versus liquidity. This deserves monitoring, but safety and holder checks are still pending."
        }

    return {
        "label": "Watch",
        "confidence": 58,
        "summary": "DexScreener returned a valid market pair. This is a partial OSINT result; contract, holder, social, and liquidity-lock checks are still pending."
    }



ETHERSCAN_API_BY_CHAIN = {
    "ethereum": {"url": "https://api.etherscan.io/v2/api", "chainid": "1", "explorer": "https://etherscan.io"},
    "eth": {"url": "https://api.etherscan.io/v2/api", "chainid": "1", "explorer": "https://etherscan.io"},
}


def lookup_etherscan_contract_creator(chain: str, contract: str) -> dict:
    normalized_chain = (chain or "").strip().lower()
    chain_cfg = ETHERSCAN_API_BY_CHAIN.get(normalized_chain)

    if not chain_cfg or not contract or not str(contract).lower().startswith("0x"):
        return {
            "available": False,
            "provider": "Etherscan",
            "reason": "unsupported_chain_or_contract",
        }

    if not ETHERSCAN_API_KEY:
        return {
            "available": False,
            "provider": "Etherscan",
            "reason": "api_key_missing",
        }

    try:
        params = {
            "chainid": chain_cfg["chainid"],
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": contract,
            "apikey": ETHERSCAN_API_KEY,
        }
        response = requests.get(chain_cfg["url"], params=params, timeout=18)
        response.raise_for_status()
        payload = response.json()

        result = payload.get("result") or []

        if isinstance(result, str):
            return {
                "available": False,
                "provider": "Etherscan",
                "reason": "api_result_string",
                "raw_status": payload.get("status"),
                "raw_message": payload.get("message"),
                "raw_result": result[:240],
            }

        if isinstance(result, dict):
            result = [result]

        if not isinstance(result, list) or not result:
            return {
                "available": False,
                "provider": "Etherscan",
                "reason": "empty_result",
                "raw_status": payload.get("status"),
                "raw_message": payload.get("message"),
            }

        item = result[0] or {}
        if not isinstance(item, dict):
            return {
                "available": False,
                "provider": "Etherscan",
                "reason": "unexpected_result_item",
                "raw_status": payload.get("status"),
                "raw_message": payload.get("message"),
                "raw_result": str(item)[:240],
            }

        creator = item.get("contractCreator") or item.get("creatorAddress")
        tx_hash = item.get("txHash") or item.get("contractCreatorTxHash")

        if not creator and not tx_hash:
            return {
                "available": False,
                "provider": "Etherscan",
                "reason": "creator_not_found",
                "raw_status": payload.get("status"),
                "raw_message": payload.get("message"),
            }

        return {
            "available": True,
            "provider": "Etherscan",
            "chain": normalized_chain,
            "deployer_wallet": creator,
            "contract_creation_tx": tx_hash,
            "contract_address": contract,
            "block_number": item.get("blockNumber"),
            "created_timestamp": item.get("timestamp"),
            "contract_factory": item.get("contractFactory"),
            "etherscan_contract_url": f"{chain_cfg['explorer']}/address/{contract}",
            "etherscan_tx_url": f"{chain_cfg['explorer']}/tx/{tx_hash}" if tx_hash else None,
            "raw_message": payload.get("message"),
        }

    except Exception as exc:
        logger.warning(f"Etherscan OSINT creator lookup failed for {chain}:{contract}: {exc}")
        return {
            "available": False,
            "provider": "Etherscan",
            "reason": "request_failed",
            "error": str(exc),
        }


COINGECKO_PLATFORM_BY_CHAIN = {
    "ethereum": "ethereum",
    "eth": "ethereum",
    "bsc": "binance-smart-chain",
    "binance-smart-chain": "binance-smart-chain",
    "polygon": "polygon-pos",
    "polygon-pos": "polygon-pos",
    "arbitrum": "arbitrum-one",
    "arbitrum-one": "arbitrum-one",
    "optimism": "optimistic-ethereum",
    "base": "base",
    "avalanche": "avalanche",
    "solana": "solana",
}


def lookup_coingecko_contract_metadata(chain: str, contract: str) -> dict:
    platform = COINGECKO_PLATFORM_BY_CHAIN.get((chain or "").strip().lower())

    if not platform or not contract:
        return {
            "available": False,
            "provider": "CoinGecko",
            "reason": "unsupported_chain_or_contract",
        }

    if platform != "solana" and not str(contract).lower().startswith("0x"):
        return {
            "available": False,
            "provider": "CoinGecko",
            "reason": "unsupported_chain_or_contract",
        }

    try:
        url = f"https://api.coingecko.com/api/v3/coins/{platform}/contract/{contract}"
        response = requests.get(url, headers=CG_HEADERS, timeout=18)

        if response.status_code == 404:
            return {
                "available": False,
                "provider": "CoinGecko",
                "reason": "contract_not_found",
                "platform": platform,
            }

        response.raise_for_status()
        data = response.json() if response.content else {}

        links = data.get("links") or {}
        homepage = [x for x in (links.get("homepage") or []) if x]
        blockchain_site = [x for x in (links.get("blockchain_site") or []) if x]
        official_forum_url = [x for x in (links.get("official_forum_url") or []) if x]
        chat_url = [x for x in (links.get("chat_url") or []) if x]
        announcement_url = [x for x in (links.get("announcement_url") or []) if x]

        socials = {
            "twitter": links.get("twitter_screen_name"),
            "telegram": links.get("telegram_channel_identifier"),
            "subreddit": links.get("subreddit_url"),
            "github": (links.get("repos_url") or {}).get("github") or [],
            "chat": chat_url,
            "forum": official_forum_url,
            "announcement": announcement_url,
        }

        market_data = data.get("market_data") or {}

        return {
            "available": True,
            "provider": "CoinGecko",
            "source": "contract_lookup",
            "platform": platform,
            "coin_id": data.get("id"),
            "name": data.get("name"),
            "symbol": (data.get("symbol") or "").upper() if data.get("symbol") else None,
            "asset_platform_id": data.get("asset_platform_id"),
            "contract": contract,
            "website": homepage[0] if homepage else None,
            "homepage": homepage,
            "blockchain_site": blockchain_site,
            "categories": data.get("categories") or [],
            "description": ((data.get("description") or {}).get("en") or "")[:800],
            "genesis_date": data.get("genesis_date"),
            "sentiment_votes_up_percentage": data.get("sentiment_votes_up_percentage"),
            "sentiment_votes_down_percentage": data.get("sentiment_votes_down_percentage"),
            "watchlist_portfolio_users": data.get("watchlist_portfolio_users"),
            "market_cap_rank": data.get("market_cap_rank"),
            "coingecko_rank": data.get("coingecko_rank"),
            "liquidity_score": data.get("liquidity_score"),
            "developer_score": data.get("developer_score"),
            "community_score": data.get("community_score"),
            "public_interest_score": data.get("public_interest_score"),
            "market_data": {
                "current_price_usd": (market_data.get("current_price") or {}).get("usd"),
                "market_cap_usd": (market_data.get("market_cap") or {}).get("usd"),
                "total_volume_usd": (market_data.get("total_volume") or {}).get("usd"),
                "price_change_24h": market_data.get("price_change_percentage_24h"),
            },
            "socials": socials,
        }

    except Exception as exc:
        logger.warning(f"CoinGecko OSINT metadata lookup failed for {chain}:{contract}: {exc}")
        return {
            "available": False,
            "provider": "CoinGecko",
            "reason": "request_failed",
            "error": str(exc),
            "platform": platform,
        }


GOPLUS_CHAIN_IDS = {
    "ethereum": "1",
    "eth": "1",
    "bsc": "56",
    "binance-smart-chain": "56",
    "polygon": "137",
    "arbitrum": "42161",
    "optimism": "10",
    "base": "8453",
    "avalanche": "43114",
}


def _truthy_flag(value):
    return str(value).strip().lower() in {"1", "true", "yes"}


def lookup_goplus_osint(chain: str, contract: str):
    normalized_chain = (chain or "").lower()
    chain_id = GOPLUS_CHAIN_IDS.get(normalized_chain)

    if not contract or not str(contract).lower().startswith("0x"):
        return {
            "available": False,
            "provider": "GoPlus",
            "reason": "evm_contract_required",
            "checks": {},
            "risk_score": None,
            "red_flags": [],
        }

    if not chain_id:
        return {
            "available": False,
            "provider": "GoPlus",
            "reason": "unsupported_chain",
            "checks": {},
            "risk_score": None,
            "red_flags": [],
        }

    try:
        url = f"https://api.gopluslabs.io/api/v1/token_security/{chain_id}"
        response = requests.get(url, params={"contract_addresses": contract}, timeout=15)
        response.raise_for_status()
        payload = response.json()

        token_map = payload.get("result") or {}
        token_data = token_map.get(contract) or token_map.get(contract.lower()) or {}

        if not token_data:
            return {
                "available": False,
                "provider": "GoPlus",
                "reason": "empty_result",
                "checks": {},
                "risk_score": None,
                "red_flags": [],
            }

        checks = {
            "open_source": _truthy_flag(token_data.get("is_open_source")),
            "proxy": _truthy_flag(token_data.get("is_proxy")),
            "honeypot": _truthy_flag(token_data.get("is_honeypot")),
            "blacklist": _truthy_flag(token_data.get("is_blacklisted") or token_data.get("blacklist")),
            "malicious_address": _truthy_flag(token_data.get("malicious_address")),
            "hidden_owner": _truthy_flag(token_data.get("hidden_owner")),
            "owner_can_change_balance": _truthy_flag(token_data.get("owner_change_balance")),
            "cannot_sell_all": _truthy_flag(token_data.get("cannot_sell_all")),
            "trading_cooldown": _truthy_flag(token_data.get("trading_cooldown")),
            "selfdestruct": _truthy_flag(token_data.get("selfdestruct")),
        }

        buy_tax = _safe_float(token_data.get("buy_tax"), 0) or 0
        sell_tax = _safe_float(token_data.get("sell_tax"), 0) or 0
        checks["buy_tax"] = buy_tax
        checks["sell_tax"] = sell_tax

        risk_score = 20
        red_flags = []

        if not checks["open_source"]:
            risk_score += 18
            red_flags.append("contract_not_open_source")
        if checks["proxy"]:
            risk_score += 8
            red_flags.append("proxy_contract")
        if checks["honeypot"]:
            risk_score += 45
            red_flags.append("honeypot")
        if checks["blacklist"]:
            risk_score += 25
            red_flags.append("blacklist")
        if checks["malicious_address"]:
            risk_score += 40
            red_flags.append("malicious_address")
        if checks["hidden_owner"]:
            risk_score += 12
            red_flags.append("hidden_owner")
        if checks["owner_can_change_balance"]:
            risk_score += 18
            red_flags.append("owner_can_change_balance")
        if checks["cannot_sell_all"]:
            risk_score += 35
            red_flags.append("cannot_sell_all")
        if max(buy_tax, sell_tax) >= 20:
            risk_score += 20
            red_flags.append("high_tax")
        elif max(buy_tax, sell_tax) >= 8:
            risk_score += 8
            red_flags.append("elevated_tax")

        risk_score = max(0, min(100, int(risk_score)))

        return {
            "available": True,
            "provider": "GoPlus",
            "reason": None,
            "checks": checks,
            "risk_score": risk_score,
            "red_flags": sorted(set(red_flags)),
        }

    except Exception as exc:
        logger.warning(f"Token OSINT GoPlus lookup failed for chain={chain} contract={contract}: {exc}")
        return {
            "available": False,
            "provider": "GoPlus",
            "reason": "request_failed",
            "error": str(exc),
            "checks": {},
            "risk_score": None,
            "red_flags": [],
        }



def _dex_activity_social_score(dex_data):
    if not dex_data:
        return {
            "available": False,
            "score": 0,
            "volume_liquidity_ratio": None,
            "reason": "no_dex_market_data",
        }

    liquidity = _safe_float(dex_data.get("liquidity_usd"), 0) or 0
    volume = _safe_float(dex_data.get("volume_24h"), 0) or 0
    change = abs(_safe_float(dex_data.get("price_change_24h"), 0) or 0)

    ratio = volume / liquidity if liquidity > 0 else 0

    score = 0
    if liquidity > 0:
        score += 15
    if ratio >= 2:
        score += 45
    elif ratio >= 0.75:
        score += 35
    elif ratio >= 0.25:
        score += 25
    elif ratio >= 0.05:
        score += 15
    elif ratio > 0:
        score += 5

    if change >= 40:
        score += 25
    elif change >= 15:
        score += 18
    elif change >= 5:
        score += 10
    elif change > 0:
        score += 4

    return {
        "available": True,
        "score": max(0, min(100, int(score))),
        "volume_liquidity_ratio": round(ratio, 4),
        "volume_24h": volume,
        "liquidity_usd": liquidity,
        "price_change_24h": dex_data.get("price_change_24h"),
    }


async def lookup_telegram_social_heat(symbol: str):
    sym = (symbol or "").replace("$", "").upper().strip()

    if not sym:
        return {
            "available": False,
            "score": 0,
            "reason": "missing_symbol",
            "mentions_72h": 0,
            "useful_signals": 0,
        }

    since = datetime.utcnow() - timedelta(hours=72)

    try:
        docs = await db.telegram_signals.find({
            "symbol": sym,
            "posted_at": {"$gte": since},
        }).sort("posted_at", -1).limit(100).to_list(length=100)

        mentions = len(docs)
        useful = 0
        verified = 0
        pending = 0
        pump = 0
        dump = 0
        quality_confidences = []

        for doc in docs:
            status = (doc.get("status") or "").lower()
            direction = (doc.get("direction") or "").lower()
            qj = doc.get("quality_judge") or {}

            if status == "verified":
                verified += 1
            if status in {"pending", "partially_verified"}:
                pending += 1
            if direction == "pump":
                pump += 1
            if direction == "dump":
                dump += 1

            if qj.get("is_trade_signal") is True:
                useful += 1
                try:
                    quality_confidences.append(float(qj.get("confidence") or 0))
                except Exception:
                    pass

        score = 0
        if mentions >= 10:
            score += 35
        elif mentions >= 5:
            score += 25
        elif mentions >= 2:
            score += 15
        elif mentions == 1:
            score += 8

        if useful >= 5:
            score += 35
        elif useful >= 2:
            score += 25
        elif useful == 1:
            score += 15

        if verified >= 2:
            score += 15
        elif verified == 1:
            score += 8

        if quality_confidences:
            avg_conf = sum(quality_confidences) / len(quality_confidences)
            if avg_conf >= 75:
                score += 12
            elif avg_conf >= 55:
                score += 6

        return {
            "available": True,
            "score": max(0, min(100, int(score))),
            "symbol": sym,
            "window_hours": 72,
            "mentions_72h": mentions,
            "useful_signals": useful,
            "verified": verified,
            "pending": pending,
            "pump": pump,
            "dump": dump,
            "avg_quality_confidence": round(sum(quality_confidences) / len(quality_confidences), 2) if quality_confidences else None,
            "sample": [
                {
                    "direction": doc.get("direction"),
                    "status": doc.get("status"),
                    "quality": (doc.get("quality_judge") or {}).get("label"),
                    "confidence": (doc.get("quality_judge") or {}).get("confidence"),
                    "posted_at": str(doc.get("posted_at")),
                }
                for doc in docs[:5]
            ],
        }

    except Exception as exc:
        logger.warning(f"Token OSINT Telegram social heat lookup failed for symbol={sym}: {exc}")
        return {
            "available": False,
            "score": 0,
            "reason": "telegram_lookup_failed",
            "error": str(exc),
            "mentions_72h": 0,
            "useful_signals": 0,
        }



HONEYPOT_CHAIN_IDS = {
    "ethereum": 1,
    "eth": 1,
    "bsc": 56,
    "binance-smart-chain": 56,
    "base": 8453,
}


def lookup_honeypot_osint(chain: str, contract: str, pair_address: str | None = None):
    normalized_chain = (chain or "").lower()
    chain_id = HONEYPOT_CHAIN_IDS.get(normalized_chain)

    if not contract or not str(contract).lower().startswith("0x"):
        return {
            "available": False,
            "provider": "Honeypot.is",
            "reason": "evm_contract_required",
            "checks": {},
            "red_flags": [],
        }

    if not chain_id:
        return {
            "available": False,
            "provider": "Honeypot.is",
            "reason": "unsupported_chain",
            "checks": {},
            "red_flags": [],
        }

    try:
        params = {
            "address": contract,
            "chainID": chain_id,
        }
        if pair_address:
            params["pair"] = pair_address

        response = requests.get("https://api.honeypot.is/v2/IsHoneypot", params=params, timeout=18)
        response.raise_for_status()
        payload = response.json()

        honeypot_result = payload.get("honeypotResult") or {}
        simulation_result = payload.get("simulationResult") or {}
        summary = payload.get("summary") or {}

        is_honeypot = honeypot_result.get("isHoneypot") is True
        buy_tax = _safe_float(simulation_result.get("buyTax"), 0) or 0
        sell_tax = _safe_float(simulation_result.get("sellTax"), 0) or 0
        transfer_tax = _safe_float(simulation_result.get("transferTax"), 0)
        summary_risk = (summary.get("risk") or "").strip().lower() or None

        red_flags = []
        if is_honeypot:
            red_flags.append("honeypot")
        if max(buy_tax, sell_tax) >= 20:
            red_flags.append("high_tax")
        elif max(buy_tax, sell_tax) >= 8:
            red_flags.append("elevated_tax")
        if summary_risk in {"high", "very_high"}:
            red_flags.append(f"honeypot_summary_{summary_risk}")

        return {
            "available": True,
            "provider": "Honeypot.is",
            "reason": None,
            "checks": {
                "is_honeypot": is_honeypot,
                "buy_tax": buy_tax,
                "sell_tax": sell_tax,
                "transfer_tax": transfer_tax,
                "summary_risk": summary_risk,
            },
            "red_flags": sorted(set(red_flags)),
        }

    except Exception as exc:
        logger.warning(f"Token OSINT Honeypot lookup failed for chain={chain} contract={contract}: {exc}")
        return {
            "available": False,
            "provider": "Honeypot.is",
            "reason": "request_failed",
            "error": str(exc),
            "checks": {},
            "red_flags": [],
        }


def merge_osint_safety(goplus_data, honeypot_data):
    red_flags = sorted(set((goplus_data or {}).get("red_flags") or []) | set((honeypot_data or {}).get("red_flags") or []))

    checks = dict((goplus_data or {}).get("checks") or {})
    hp_checks = (honeypot_data or {}).get("checks") or {}

    if honeypot_data and honeypot_data.get("available"):
        checks["honeypot_is_honeypot"] = hp_checks.get("is_honeypot")
        checks["honeypot_buy_tax"] = hp_checks.get("buy_tax")
        checks["honeypot_sell_tax"] = hp_checks.get("sell_tax")
        checks["honeypot_summary_risk"] = hp_checks.get("summary_risk")

    risk_score = (goplus_data or {}).get("risk_score")
    if risk_score is None:
        risk_score = 20

    if "honeypot" in red_flags:
        risk_score += 40
    if "high_tax" in red_flags:
        risk_score += 18
    elif "elevated_tax" in red_flags:
        risk_score += 8
    if any(flag.startswith("honeypot_summary_") for flag in red_flags):
        risk_score += 12

    risk_score = max(0, min(100, int(risk_score)))

    return {
        "available": bool((goplus_data or {}).get("available") or (honeypot_data or {}).get("available")),
        "provider": "GoPlus + Honeypot.is",
        "goplus": goplus_data,
        "honeypot": honeypot_data,
        "checks": checks,
        "risk_score": risk_score,
        "red_flags": red_flags,
    }



def lookup_honeypot_holders_osint(chain: str, contract: str):
    normalized_chain = (chain or "").lower()
    chain_id = HONEYPOT_CHAIN_IDS.get(normalized_chain)

    if not contract or not str(contract).lower().startswith("0x"):
        return {
            "available": False,
            "provider": "Honeypot.is TopHolders",
            "reason": "evm_contract_required",
            "source_mode": "unavailable",
            "risks": [],
        }

    if not chain_id:
        return {
            "available": False,
            "provider": "Honeypot.is TopHolders",
            "reason": "unsupported_chain",
            "source_mode": "unavailable",
            "risks": [],
        }

    try:
        response = requests.get(
            "https://api.honeypot.is/v1/TopHolders",
            params={"address": contract, "chainID": chain_id},
            timeout=18,
        )
        response.raise_for_status()
        payload = response.json()

        total_supply = _safe_float(payload.get("totalSupply"), 0) or 0
        holders = payload.get("holders") or []

        if total_supply <= 0 or not holders:
            return {
                "available": False,
                "provider": "Honeypot.is TopHolders",
                "reason": "empty_holders",
                "source_mode": "empty",
                "risks": [],
            }

        shares = []
        normalized_holders = []
        for holder in holders:
            balance = _safe_float(holder.get("balance"), 0) or 0
            share = balance / total_supply if total_supply else 0
            shares.append(share)
            normalized_holders.append({
                "address": holder.get("address"),
                "balance": balance,
                "share": round(share, 6),
            })

        top_1_share = sum(shares[:1])
        top_5_share = sum(shares[:5])
        top_10_share = sum(shares[:10])

        risks = []
        if top_1_share >= 0.2:
            risks.append("single_holder_dominance")
        if top_10_share >= 0.35:
            risks.append("holder_concentration_high")
        elif top_10_share >= 0.2:
            risks.append("holder_concentration_elevated")

        holder_risk = 15
        if top_1_share >= 0.2:
            holder_risk += 35
        elif top_1_share >= 0.1:
            holder_risk += 20

        if top_10_share >= 0.35:
            holder_risk += 35
        elif top_10_share >= 0.2:
            holder_risk += 20
        elif top_10_share >= 0.1:
            holder_risk += 10

        holder_risk = max(0, min(100, int(holder_risk)))

        return {
            "available": True,
            "provider": "Honeypot.is TopHolders",
            "reason": None,
            "source_mode": "live",
            "holder_count_sample": len(holders),
            "top_1_share": round(top_1_share, 4),
            "top_5_share": round(top_5_share, 4),
            "top_10_share": round(top_10_share, 4),
            "holder_risk": holder_risk,
            "risks": sorted(set(risks)),
            "sample": normalized_holders[:10],
        }

    except Exception as exc:
        logger.warning(f"Token OSINT holders lookup failed for chain={chain} contract={contract}: {exc}")
        return {
            "available": False,
            "provider": "Honeypot.is TopHolders",
            "reason": "request_failed",
            "error": str(exc),
            "source_mode": "unavailable",
            "risks": [],
        }



@app.post("/api/osint/scan")
async def token_osint_scan(payload: TokenOsintScanRequest, user=Depends(get_optional_user)):
    query = payload.query.strip()

    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    query_type = detect_osint_query_type(query)

    if query_type not in {"evm_contract", "solana_mint"}:
        raise HTTPException(
            status_code=400,
            detail="Token OSINT requires an official EVM contract address or Solana mint address. Symbol search is disabled to avoid clones and ticker collisions."
        )

    chain = payload.chain or ("solana" if query_type == "solana_mint" else "ethereum")

    dex_data = lookup_dexscreener_osint(query, query_type, chain)
    verdict = _basic_osint_verdict(dex_data)
    liquidity_health = _liquidity_health_score(dex_data.get("liquidity_usd") if dex_data else None)

    identity = {
        "name": "Pending OSINT lookup",
        "symbol": query.upper() if query_type == "symbol" else "UNKNOWN",
        "contract": query if query_type in {"evm_contract", "solana_mint"} else None,
        "website": query if query_type == "project_url" else None,
        "category": "pending",
    }

    if dex_data:
        base = dex_data.get("base_token") or {}
        identity.update({
            "name": base.get("name") or identity["name"],
            "symbol": base.get("symbol") or identity["symbol"],
            "contract": base.get("address") or identity["contract"],
            "category": "dex_market_pair",
        })
        chain = dex_data.get("chain") or chain

    metadata = lookup_coingecko_contract_metadata(chain, identity.get("contract"))

    if metadata.get("available"):
        categories = metadata.get("categories") or []
        identity.update({
            "name": metadata.get("name") or identity["name"],
            "symbol": metadata.get("symbol") or identity["symbol"],
            "website": metadata.get("website") or identity.get("website"),
            "category": categories[0] if categories else identity.get("category"),
            "coin_id": metadata.get("coin_id"),
            "launch_date": metadata.get("genesis_date"),
        })

    if query_type == "solana_mint" or (chain or "").lower() == "solana":
        safety = {
            "available": False,
            "provider": "EVM safety providers",
            "reason": "not_applicable_for_solana_v1",
            "checks": {},
            "risk_score": None,
            "red_flags": [],
        }
        creator = {
            "available": False,
            "provider": "Etherscan",
            "reason": "not_applicable_for_solana",
        }
        holders = {
            "available": False,
            "provider": "Solana holder source",
            "reason": "solana_holder_source_not_connected",
            "source_mode": "not_connected",
            "risks": [],
        }
    else:
        goplus_safety = lookup_goplus_osint(chain, identity.get("contract"))
        honeypot_safety = lookup_honeypot_osint(
            chain,
            identity.get("contract"),
            dex_data.get("pair_address") if dex_data else None,
        )
        safety = merge_osint_safety(goplus_safety, honeypot_safety)
        creator = lookup_etherscan_contract_creator(chain, identity.get("contract"))
        holders = lookup_honeypot_holders_osint(chain, identity.get("contract"))

    telegram_social = await lookup_telegram_social_heat(identity.get("symbol"))
    dex_social = _dex_activity_social_score(dex_data)

    social_heat = max(
        telegram_social.get("score") or 0,
        dex_social.get("score") or 0,
    )

    holder_risk = holders.get("holder_risk") if holders.get("available") else None

    if query_type == "solana_mint" or (chain or "").lower() == "solana":
        verdict = {
            **verdict,
            "label": verdict.get("label", "Watch"),
            "confidence": max(verdict.get("confidence", 0), 58),
            "summary": "Solana v1 scan connected through DexScreener and CoinGecko metadata. Solana-specific holder, deployer and safety sources are not connected yet."
        }
    elif safety.get("available"):
        verdict = {
            **verdict,
            "label": "Risky" if (safety.get("risk_score") or 0) >= 65 else verdict.get("label", "Watch"),
            "confidence": max(verdict.get("confidence", 0), 65),
            "summary": f"{verdict.get('summary', '')} GoPlus safety check is connected and returned {len(safety.get('red_flags') or [])} active risk flags."
        }

    result = {
        "status": "partial" if dex_data or safety.get("available") else "preview",
        "sample_data": False if dex_data or safety.get("available") else True,
        "query": query,
        "query_type": query_type,
        "chain": chain,
        "identity": identity,
        "metadata": metadata,
        "creator": creator,
        "market": dex_data,
        "contract_safety": safety,
        "social": {
            "heat_score": social_heat,
            "telegram": telegram_social,
            "dex_activity": dex_social,
            "sources": ["telegram_signals", "dexscreener_activity"],
        },
        "holders": holders,
        "scores": {
            "risk_score": safety.get("risk_score"),
            "social_heat": social_heat,
            "holder_risk": holder_risk,
            "liquidity_health": liquidity_health,
        },
        "red_flags": sorted(set((safety.get("red_flags") or ([] if dex_data else ["no_market_pair_found"])) + (holders.get("risks") or []))),
        "ai_verdict": verdict,
        "full_ai_analysis": None,
        "report": None,
        "next_sources": [
            "GeckoTerminal",
            "TokenSniffer",
            "Etherscan/BscScan/Solscan",
            "AI judge layer"
        ],
        "scan_id": None,
        "saved": False,
    }

    if user:
        try:
            scan_doc = {
                "user_id": str(user["_id"]),
                "user_email": user.get("email"),
                "query": query,
                "query_type": query_type,
                "chain": chain,
                "symbol": identity.get("symbol"),
                "contract": identity.get("contract"),
                "created_at": datetime.utcnow(),
                "result": result,
                "scores": result.get("scores"),
                "red_flags": result.get("red_flags"),
                "verdict": (result.get("ai_verdict") or {}).get("label"),
                "full_ai_analysis": None,
                "report": None,
            }
            insert_result = await db.osint_scans.insert_one(scan_doc)
            result["scan_id"] = str(insert_result.inserted_id)
            result["saved"] = True
        except Exception as exc:
            logger.warning(f"Token OSINT scan save failed: {exc}")
            result["save_error"] = "scan_not_saved"

    return api_ok(result)


def build_local_osint_ai_analysis(scan_result: dict) -> dict:
    identity = scan_result.get("identity") or {}
    market = scan_result.get("market") or {}
    safety = scan_result.get("contract_safety") or {}
    social = scan_result.get("social") or {}
    holders = scan_result.get("holders") or {}
    scores = scan_result.get("scores") or {}
    red_flags = scan_result.get("red_flags") or []

    symbol = identity.get("symbol") or scan_result.get("query") or "Token"
    risk_score = scores.get("risk_score")
    social_heat = scores.get("social_heat")
    holder_risk = scores.get("holder_risk")
    liquidity_health = scores.get("liquidity_health")

    flag_text = ", ".join(red_flags) if red_flags else "No active red flags from connected sources"

    if "honeypot" in red_flags or "cannot_sell_all" in red_flags:
        verdict = "Reject"
    elif (risk_score or 0) >= 70 or (holder_risk or 0) >= 75:
        verdict = "Risky"
    elif red_flags:
        verdict = "Watch"
    else:
        verdict = "Monitor"

    confidence = 70
    if safety.get("available"):
        confidence += 8
    if holders.get("available"):
        confidence += 8
    if market:
        confidence += 6
    confidence = min(92, confidence)

    is_solana = (
        str(scan_result.get("chain") or "").lower() == "solana"
        or scan_result.get("query_type") == "solana_mint"
    )

    market_text = (
        f"DexScreener shows liquidity of ${float(market.get('liquidity_usd') or 0):,.0f}, "
        f"24h volume of ${float(market.get('volume_24h') or 0):,.0f}, "
        f"and 24h change of {market.get('price_change_24h')}%."
    ) if market else "Market source not connected."

    social_text = (
        f"Social Heat v1 is {social.get('heat_score')}. "
        f"Telegram mentions in 72h: {(social.get('telegram') or {}).get('mentions_72h', 0)}; "
        f"Dex activity score: {(social.get('dex_activity') or {}).get('score', 0)}."
    ) if social else "Social source not connected."

    if is_solana:
        return {
            "source": "local_fallback",
            "verdict": verdict,
            "confidence": confidence,
            "executive_summary": (
                f"{symbol} has live Solana v1 market, metadata and social-heat data available. "
                f"Current scores are social heat {social_heat} and liquidity health {liquidity_health}. "
                f"Solana-specific safety, holder and deployer sources are not connected yet. "
                f"Active flags: {flag_text}."
            ),
            "risk_breakdown": {
                "market": market_text,
                "contract": "EVM safety providers are not applicable for Solana v1. Use Solscan, RugCheck, Helius or Birdeye in the next Solana layer.",
                "holders": "Solana holder and wallet-cluster source is not connected yet.",
                "social": social_text,
                "liquidity": f"Liquidity health score is {liquidity_health}/100.",
            },
            "what_is_confirmed": [
                "DexScreener Solana market pair data is connected" if market else None,
                "CoinGecko metadata is connected when not rate-limited",
                "Telegram + Dex activity Social Heat v1 is connected" if social else None,
            ],
            "what_is_pending": [
                "Solscan / Helius holder and deployer data",
                "RugCheck Solana safety layer",
                "Birdeye / Jupiter / Pump.fun / Raydium launch and trading context",
                "Reddit and richer social intelligence",
            ],
            "red_flags_explained": [
                "No active red flags from connected Solana v1 sources." if not red_flags else None,
            ],
            "recommended_action": "monitor",
            "next_checks": [
                "Add Solscan or Helius for deployer, holders and wallet structure",
                "Add RugCheck for Solana safety checks",
                "Add Birdeye/Jupiter/Pump.fun/Raydium for Solana trading and launch context",
                "Add Reddit and X confirmation layers for narrative/caller validation",
            ],
        }

    return {
        "source": "local_fallback",
        "verdict": verdict,
        "confidence": confidence,
        "executive_summary": (
            f"{symbol} has live market, contract-safety, social-heat and holder data available. "
            f"Current scores are risk {risk_score}, social heat {social_heat}, holder risk {holder_risk}, "
            f"and liquidity health {liquidity_health}. Active flags: {flag_text}."
        ),
        "risk_breakdown": {
            "market": market_text,
            "contract": (
                f"GoPlus/Honeypot safety is connected. Honeypot flag is "
                f"{safety.get('checks', {}).get('honeypot_is_honeypot', safety.get('checks', {}).get('honeypot'))}, "
                f"buy tax is {safety.get('checks', {}).get('buy_tax')}%, sell tax is {safety.get('checks', {}).get('sell_tax')}%, "
                f"and active safety flags are {', '.join(safety.get('red_flags') or []) or 'none'}."
            ) if safety.get("available") else "Contract safety source not connected.",
            "holders": (
                f"TopHolders sample size is {holders.get('holder_count_sample')}. "
                f"Top 1 holder controls {(holders.get('top_1_share') or 0) * 100:.2f}%, "
                f"top 5 control {(holders.get('top_5_share') or 0) * 100:.2f}%, "
                f"and top 10 control {(holders.get('top_10_share') or 0) * 100:.2f}%."
            ) if holders.get("available") else "Holder source not connected.",
            "social": social_text,
            "liquidity": f"Liquidity health score is {liquidity_health}/100.",
        },
        "what_is_confirmed": [
            "DexScreener market pair data is connected" if market else None,
            "GoPlus and Honeypot.is contract-safety checks are connected" if safety.get("available") else None,
            "TopHolders concentration data is connected" if holders.get("available") else None,
            "Telegram + Dex activity Social Heat v1 is connected" if social else None,
        ],
        "what_is_pending": [
            "Deep wallet/deployer history",
            "CoinGecko/GeckoTerminal trending expansion",
            "Reddit and richer social intelligence",
            "Manual verification of provider-specific blacklist flags",
        ],
        "red_flags_explained": [
            "GoPlus returned a blacklist-related flag; treat it as a review flag, not an absolute verdict." if "blacklist" in red_flags else None,
            "Top 10 holder concentration is high based on the TopHolders sample." if "holder_concentration_high" in red_flags else None,
            "Tax risk was detected by GoPlus or Honeypot.is." if ("high_tax" in red_flags or "elevated_tax" in red_flags) else None,
        ],
        "recommended_action": "request_more_data" if red_flags else "monitor",
        "next_checks": [
            "Verify blacklist flag manually in GoPlus/explorer context",
            "Add deployer wallet and previous-launch history",
            "Add deeper holder cluster and whale movement checks",
            "Add CoinGecko/GeckoTerminal trending and Reddit layer",
        ],
    }


async def generate_osint_ai_analysis(scan_result: dict) -> dict:
    fallback = build_local_osint_ai_analysis(scan_result)

    try:
        import json as json_lib

        compact_payload = {
            "identity": scan_result.get("identity"),
            "market": scan_result.get("market"),
            "contract_safety": scan_result.get("contract_safety"),
            "social": scan_result.get("social"),
            "holders": scan_result.get("holders"),
            "scores": scan_result.get("scores"),
            "red_flags": scan_result.get("red_flags"),
            "preliminary_verdict": scan_result.get("ai_verdict"),
        }

        system_instruction = (
            "You are a crypto OSINT and token-risk analyst. Respond in English only. "
            "Use only the supplied facts. Do not invent missing data. "
            "Distinguish confirmed findings from pending checks. "
            "Return strict JSON only."
        )

        prompt = f"""Analyze this Token OSINT scan and produce a practical due-diligence report.

Input JSON:
{json_lib.dumps(compact_payload, default=str)[:24000]}

Return strict JSON with this shape:
{{
  "source": "openrouter",
  "verdict": "Monitor|Watch|Risky|Reject|Request More Data",
  "confidence": 0-100,
  "executive_summary": "concise but useful summary",
  "risk_breakdown": {{
    "market": "...",
    "contract": "...",
    "holders": "...",
    "social": "...",
    "liquidity": "..."
  }},
  "what_is_confirmed": ["..."],
  "what_is_pending": ["..."],
  "red_flags_explained": ["..."],
  "recommended_action": "monitor|watch|request_more_data|reject",
  "next_checks": ["..."]
}}"""

        ai_result = await call_claude_haiku_json(
            system_instruction=system_instruction,
            user_prompt=prompt,
            temperature=0.1,
            max_tokens=2200,
        )

        if ai_result.get("ok") and ai_result.get("json"):
            parsed = ai_result["json"]
            parsed["source"] = ai_result.get("provider") or parsed.get("source") or "openai_compatible"
            parsed["model"] = ai_result.get("model")
            return parsed

        logger.warning(
            "OpenAI/OpenRouter OSINT analysis failed - using local fallback. provider=%s error=%s",
            ai_result.get("provider"),
            ai_result.get("error"),
        )
        fallback["source"] = "local_fallback"
        return fallback

    except Exception as exc:
        logger.warning(f"OpenAI/OpenRouter OSINT analysis error - using local fallback: {exc}")
        fallback["source"] = "local_fallback"
        return fallback


def build_osint_email_report_html(scan_result: dict, analysis: dict, user_email: str) -> str:
    identity = scan_result.get("identity") or {}
    market = scan_result.get("market") or {}
    safety = scan_result.get("contract_safety") or {}
    checks = safety.get("checks") or {}
    social = scan_result.get("social") or {}
    holders = scan_result.get("holders") or {}
    scores = scan_result.get("scores") or {}
    red_flags = scan_result.get("red_flags") or []

    symbol = identity.get("symbol") or scan_result.get("query") or "Token"
    name = identity.get("name") or symbol

    def safe(value):
        if value is None or value == "":
            return "—"
        return str(value)

    def money(value):
        try:
            return f"${float(value):,.2f}"
        except Exception:
            return "—"

    def pct_from_share(value):
        try:
            return f"{float(value) * 100:.2f}%"
        except Exception:
            return "—"

    def list_html(items):
        clean = [x for x in (items or []) if x]
        if not clean:
            clean = ["—"]
        return "".join(f"<li style='margin:4px 0'>{safe(x)}</li>" for x in clean)

    risk_breakdown = analysis.get("risk_breakdown") or {}

    return f"""
    <div style="font-family:Arial,sans-serif;max-width:760px;margin:0 auto;background:#08111f;color:#e5e7eb;padding:24px;border-radius:16px">
      <div style="border:1px solid rgba(148,163,184,.25);background:#0f1b2d;border-radius:16px;padding:20px">
        <h1 style="margin:0;color:#fff;font-size:24px">Token OSINT Report — {safe(name)} / {safe(symbol)}</h1>
        <p style="margin:8px 0 0;color:#94a3b8;font-size:13px">Sent to {safe(user_email)} · Chain: {safe(scan_result.get("chain"))} · Scan ID: {safe(scan_result.get("scan_id"))}</p>
        <p style="margin:14px 0 0">
          <span style="display:inline-block;border:1px solid #334155;border-radius:999px;padding:6px 10px;margin-right:6px;color:#bfdbfe">Verdict: {safe(analysis.get("verdict") or (scan_result.get("ai_verdict") or {}).get("label"))}</span>
          <span style="display:inline-block;border:1px solid #334155;border-radius:999px;padding:6px 10px;margin-right:6px;color:#bfdbfe">Confidence: {safe(analysis.get("confidence") or (scan_result.get("ai_verdict") or {}).get("confidence"))}%</span>
          <span style="display:inline-block;border:1px solid #334155;border-radius:999px;padding:6px 10px;color:#bfdbfe">Source: {"Rules-based fallback" if analysis.get("source") == "local_fallback" else safe(analysis.get("source") or "preliminary")}</span>
        </p>
      </div>

      <h2 style="color:#60a5fa;margin:24px 0 10px;font-size:18px">Executive Summary</h2>
      <div style="border:1px solid rgba(148,163,184,.25);background:#0f1b2d;border-radius:14px;padding:16px">
        <p style="color:#cbd5e1;line-height:1.5;margin:0">{safe(analysis.get("executive_summary") or (scan_result.get("ai_verdict") or {}).get("summary"))}</p>
      </div>

      <h2 style="color:#60a5fa;margin:24px 0 10px;font-size:18px">Scores</h2>
      <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:10px">
        <div style="background:#0f1b2d;border:1px solid #334155;border-radius:12px;padding:14px"><div style="color:#94a3b8;font-size:12px">Risk Score</div><div style="font-size:28px;font-weight:bold;color:#fbbf24">{safe(scores.get("risk_score"))}</div></div>
        <div style="background:#0f1b2d;border:1px solid #334155;border-radius:12px;padding:14px"><div style="color:#94a3b8;font-size:12px">Social Heat</div><div style="font-size:28px;font-weight:bold;color:#60a5fa">{safe(scores.get("social_heat"))}</div></div>
        <div style="background:#0f1b2d;border:1px solid #334155;border-radius:12px;padding:14px"><div style="color:#94a3b8;font-size:12px">Holder Risk</div><div style="font-size:28px;font-weight:bold;color:#fb7185">{safe(scores.get("holder_risk"))}</div></div>
        <div style="background:#0f1b2d;border:1px solid #334155;border-radius:12px;padding:14px"><div style="color:#94a3b8;font-size:12px">Liquidity Health</div><div style="font-size:28px;font-weight:bold;color:#34d399">{safe(scores.get("liquidity_health"))}</div></div>
      </div>

      <h2 style="color:#60a5fa;margin:24px 0 10px;font-size:18px">Token Identity</h2>
      <div style="background:#0f1b2d;border:1px solid #334155;border-radius:12px;padding:14px;color:#cbd5e1">
        <p><strong>Name:</strong> {safe(identity.get("name"))}</p>
        <p><strong>Symbol:</strong> {safe(identity.get("symbol"))}</p>
        <p><strong>Contract:</strong> <span style="word-break:break-all">{safe(identity.get("contract"))}</span></p>
        <p><strong>Category:</strong> {safe(identity.get("category"))}</p>
      </div>

      <h2 style="color:#60a5fa;margin:24px 0 10px;font-size:18px">Market</h2>
      <div style="background:#0f1b2d;border:1px solid #334155;border-radius:12px;padding:14px;color:#cbd5e1">
        <p><strong>DEX:</strong> {safe(market.get("dex"))}</p>
        <p><strong>Price:</strong> {safe(market.get("price_usd"))}</p>
        <p><strong>Liquidity:</strong> {money(market.get("liquidity_usd"))}</p>
        <p><strong>24h Volume:</strong> {money(market.get("volume_24h"))}</p>
        <p><strong>24h Change:</strong> {safe(market.get("price_change_24h"))}%</p>
        {f'<p><strong>Pair:</strong> <a href="{market.get("pair_url")}" style="color:#60a5fa">DexScreener</a></p>' if market.get("pair_url") else ""}
      </div>

      <h2 style="color:#60a5fa;margin:24px 0 10px;font-size:18px">Contract Safety</h2>
      <div style="background:#0f1b2d;border:1px solid #334155;border-radius:12px;padding:14px;color:#cbd5e1">
        <p><strong>Provider:</strong> {safe(safety.get("provider"))}</p>
        <p><strong>Honeypot:</strong> {safe(checks.get("honeypot_is_honeypot", checks.get("honeypot")))}</p>
        <p><strong>Open Source:</strong> {safe(checks.get("open_source"))}</p>
        <p><strong>Blacklist Flag:</strong> {safe(checks.get("blacklist"))}</p>
        <p><strong>Buy/Sell Tax:</strong> {safe(checks.get("buy_tax"))}% / {safe(checks.get("sell_tax"))}%</p>
      </div>

      <h2 style="color:#60a5fa;margin:24px 0 10px;font-size:18px">Social & Holders</h2>
      <div style="background:#0f1b2d;border:1px solid #334155;border-radius:12px;padding:14px;color:#cbd5e1">
        <p><strong>Social Heat:</strong> {safe(social.get("heat_score"))}</p>
        <p><strong>Telegram 72h:</strong> {safe((social.get("telegram") or {}).get("mentions_72h"))} mentions · {safe((social.get("telegram") or {}).get("useful_signals"))} useful</p>
        <p><strong>Holder Provider:</strong> {safe(holders.get("provider"))}</p>
        <p><strong>Top 1 / Top 5 / Top 10:</strong> {pct_from_share(holders.get("top_1_share"))} / {pct_from_share(holders.get("top_5_share"))} / {pct_from_share(holders.get("top_10_share"))}</p>
      </div>

      <h2 style="color:#60a5fa;margin:24px 0 10px;font-size:18px">Red Flags</h2>
      <div style="background:#0f1b2d;border:1px solid #334155;border-radius:12px;padding:14px;color:#cbd5e1">
        <ul>{list_html(red_flags or ["No active red flags from connected sources."])}</ul>
      </div>

      <h2 style="color:#60a5fa;margin:24px 0 10px;font-size:18px">Full Analysis</h2>
      <div style="background:#0f1b2d;border:1px solid #334155;border-radius:12px;padding:14px;color:#cbd5e1">
        <p><strong>Market:</strong> {safe(risk_breakdown.get("market"))}</p>
        <p><strong>Contract:</strong> {safe(risk_breakdown.get("contract"))}</p>
        <p><strong>Holders:</strong> {safe(risk_breakdown.get("holders"))}</p>
        <p><strong>Social:</strong> {safe(risk_breakdown.get("social"))}</p>
        <p><strong>Liquidity:</strong> {safe(risk_breakdown.get("liquidity"))}</p>
      </div>

      <h2 style="color:#60a5fa;margin:24px 0 10px;font-size:18px">Confirmed / Pending / Next Checks</h2>
      <div style="background:#0f1b2d;border:1px solid #334155;border-radius:12px;padding:14px;color:#cbd5e1">
        <p><strong>Confirmed:</strong></p><ul>{list_html(analysis.get("what_is_confirmed") or [])}</ul>
        <p><strong>Pending:</strong></p><ul>{list_html(analysis.get("what_is_pending") or [])}</ul>
        <p><strong>Red Flags Explained:</strong></p><ul>{list_html(analysis.get("red_flags_explained") or [])}</ul>
        <p><strong>Next Checks:</strong></p><ul>{list_html(analysis.get("next_checks") or [])}</ul>
      </div>

      <p style="color:#64748b;font-size:12px;margin-top:22px">
        Generated by PumpRadar Token OSINT Lab. This report is for due diligence and informational purposes only. It is not financial advice.
      </p>
    </div>
    """


@app.post("/api/osint/email-report/{scan_id}")
async def token_osint_email_report(scan_id: str, user=Depends(get_current_user)):
    try:
        doc = await db.osint_scans.find_one({
            "_id": ObjectId(scan_id),
            "user_id": str(user["_id"]),
        })
    except Exception:
        raise HTTPException(status_code=400, detail="invalid scan_id")

    if not doc:
        raise HTTPException(status_code=404, detail="scan not found")

    result = doc.get("result") or {}
    result["scan_id"] = str(doc["_id"])

    analysis = doc.get("full_ai_analysis") or result.get("full_ai_analysis")
    if not analysis:
        analysis = await generate_osint_ai_analysis(result)
        await db.osint_scans.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "full_ai_analysis": analysis,
                "full_ai_analysis_updated_at": datetime.utcnow(),
                "result.full_ai_analysis": analysis,
            }}
        )

    email = user.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="user email not found")

    html = build_osint_email_report_html(result, analysis, email)
    symbol = ((result.get("identity") or {}).get("symbol") or result.get("query") or "Token").upper()

    try:
        await asyncio.to_thread(resend.Emails.send, {
            "from": f"PumpRadar OSINT <{SENDER_EMAIL}>",
            "to": [email],
            "subject": f"Token OSINT Report: {symbol}",
            "html": html,
        })
        await db.osint_scans.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "report_emailed_at": datetime.utcnow(),
                "report_emailed_to": email,
            }}
        )
    except Exception as exc:
        logger.error(f"OSINT report email error: {exc}")
        raise HTTPException(status_code=500, detail="email send failed")

    return api_ok({
        "message": "OSINT report sent",
        "email": email,
        "scan_id": str(doc["_id"]),
    })


@app.post("/api/osint/analyze/{scan_id}")
async def token_osint_full_ai_analysis(scan_id: str, user=Depends(get_current_user)):
    try:
        doc = await db.osint_scans.find_one({
            "_id": ObjectId(scan_id),
            "user_id": str(user["_id"]),
        })
    except Exception:
        raise HTTPException(status_code=400, detail="invalid scan_id")

    if not doc:
        raise HTTPException(status_code=404, detail="scan not found")

    result = doc.get("result") or {}
    analysis = await generate_osint_ai_analysis(result)

    await db.osint_scans.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "full_ai_analysis": analysis,
            "full_ai_analysis_updated_at": datetime.utcnow(),
            "result.full_ai_analysis": analysis,
        }}
    )

    return api_ok({
        "scan_id": str(doc["_id"]),
        "analysis": analysis,
    })


@app.get("/api/osint/history")
async def token_osint_history(limit: int = 25, user=Depends(get_current_user)):
    limit = max(1, min(int(limit or 25), 100))

    docs = await db.osint_scans.find({
        "user_id": str(user["_id"]),
    }).sort("created_at", -1).limit(limit).to_list(length=limit)

    items = []
    for doc in docs:
        result = doc.get("result") or {}
        created_at = doc.get("created_at")
        items.append({
            "id": str(doc.get("_id")),
            "created_at": created_at.isoformat() if isinstance(created_at, datetime) else created_at,
            "query": doc.get("query"),
            "query_type": result.get("query_type") or doc.get("query_type"),
            "symbol": doc.get("symbol"),
            "contract": doc.get("contract"),
            "chain": doc.get("chain"),
            "verdict": doc.get("verdict"),
            "scores": doc.get("scores") or {},
            "red_flags": doc.get("red_flags") or [],
            "risk_score": (doc.get("scores") or {}).get("risk_score"),
            "social_heat": (doc.get("scores") or {}).get("social_heat"),
            "holder_risk": (doc.get("scores") or {}).get("holder_risk"),
            "liquidity_health": (doc.get("scores") or {}).get("liquidity_health"),
        })

    return api_ok({
        "items": items,
        "total": len(items),
    })


@app.get("/api/osint/history/{scan_id}")
async def token_osint_history_detail(scan_id: str, user=Depends(get_current_user)):
    try:
        doc = await db.osint_scans.find_one({
            "_id": ObjectId(scan_id),
            "user_id": str(user["_id"]),
        })
    except Exception:
        raise HTTPException(status_code=400, detail="invalid scan_id")

    if not doc:
        raise HTTPException(status_code=404, detail="scan not found")

    result = doc.get("result") or {}
    result["scan_id"] = str(doc["_id"])
    result["saved"] = True
    result["created_at"] = doc.get("created_at").isoformat() if isinstance(doc.get("created_at"), datetime) else doc.get("created_at")

    return api_ok(result)


# ─────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok", "app": "PumpRadar", "version": "1.0.0"}


# ─────────────────────────────────────────────
# NEW ARCHITECTURE v2 - PumpRadar Schema 2026
# ─────────────────────────────────────────────
from snapshot import run_full_scan, get_latest_snapshot
from enricher import fetch_geckoterminal_by_address as _gt_by_address

_coin_live_cache: Dict[str, tuple] = {}

@app.get("/api/crypto/coin-live/{network}/{address}")
async def get_coin_live_market(network: str, address: str, symbol: str = ""):
    """Date LIVE de piata din GeckoTerminal pentru pagina coin-ului (cu cache 10s per token)."""
    import time as _t
    import httpx as _httpx
    cache_key = f"{network}:{address}"
    cached = _coin_live_cache.get(cache_key)
    if cached and (_t.time() - cached[0]) < 10:
        return api_ok(cached[1])
    try:
        async with _httpx.AsyncClient(follow_redirects=True) as client:
            market = await _gt_by_address(client, network, address, symbol or address)
        if not market:
            payload = {"live": False}
            _coin_live_cache[cache_key] = (_t.time(), payload)
            return api_ok(payload)
        pc = market.get("price_change_pct") or {}
        vol = market.get("volume_usd") or {}
        tx = market.get("transactions") or {}
        payload = {
            "live": True,
            "price_usd": market.get("price_usd"),
            "price_change_h1": pc.get("h1"),
            "price_change_h6": pc.get("h6"),
            "price_change_h24": pc.get("h24"),
            "volume_h24": vol.get("h24"),
            "reserve_usd": market.get("reserve_usd"),
            "buy_sell_ratio_h1": tx.get("h1_buy_sell_ratio"),
            "pool_url": market.get("pool_url"),
        }
        _coin_live_cache[cache_key] = (_t.time(), payload)
        return api_ok(payload)
    except Exception as e:
        return api_ok({"live": False, "error": str(e)})


_cex_price_cache: Dict[str, tuple] = {}

@app.get("/api/crypto/cex-prices/{symbol}")
async def get_cex_prices(symbol: str):
    """Pret real de pe Binance + Coinbase pentru un simbol (cache 10s). null daca nu e listat."""
    import time as _t
    import httpx as _httpx
    sym = (symbol or "").upper().strip()
    if not sym:
        return api_ok({"binance": None, "coinbase": None})
    cached = _cex_price_cache.get(sym)
    if cached and (_t.time() - cached[0]) < 10:
        return api_ok(cached[1])
    binance_price = None
    coinbase_price = None
    try:
        async with _httpx.AsyncClient(timeout=8) as client:
            # Binance
            try:
                r = await client.get("https://api.binance.com/api/v3/ticker/price", params={"symbol": f"{sym}USDT"})
                if r.status_code == 200:
                    p = (r.json() or {}).get("price")
                    if p is not None:
                        binance_price = float(p)
            except Exception:
                pass
            # Coinbase
            try:
                r = await client.get(f"https://api.coinbase.com/v2/prices/{sym}-USD/spot")
                if r.status_code == 200:
                    p = (((r.json() or {}).get("data") or {}).get("amount"))
                    if p is not None:
                        coinbase_price = float(p)
            except Exception:
                pass
    except Exception:
        pass
    payload = {"binance": binance_price, "coinbase": coinbase_price}
    _cex_price_cache[sym] = (_t.time(), payload)
    return api_ok(payload)


_cex_price_cache: Dict[str, tuple] = {}

@app.get("/api/crypto/cex-prices/{symbol}")
async def get_cex_prices(symbol: str):
    """Pret real de pe Binance + Coinbase pentru un simbol (cache 10s). null daca nu e listat."""
    import time as _t
    import httpx as _httpx
    sym = (symbol or "").upper().strip()
    if not sym:
        return api_ok({"binance": None, "coinbase": None})
    cached = _cex_price_cache.get(sym)
    if cached and (_t.time() - cached[0]) < 10:
        return api_ok(cached[1])
    binance_price = None
    coinbase_price = None
    try:
        async with _httpx.AsyncClient(timeout=8) as client:
            # Binance
            try:
                r = await client.get("https://api.binance.com/api/v3/ticker/price", params={"symbol": f"{sym}USDT"})
                if r.status_code == 200:
                    p = (r.json() or {}).get("price")
                    if p is not None:
                        binance_price = float(p)
            except Exception:
                pass
            # Coinbase
            try:
                r = await client.get(f"https://api.coinbase.com/v2/prices/{sym}-USD/spot")
                if r.status_code == 200:
                    p = (((r.json() or {}).get("data") or {}).get("amount"))
                    if p is not None:
                        coinbase_price = float(p)
            except Exception:
                pass
    except Exception:
        pass
    payload = {"binance": binance_price, "coinbase": coinbase_price}
    _cex_price_cache[sym] = (_t.time(), payload)
    return api_ok(payload)

_holders_cache: Dict[str, tuple] = {}

_MORALIS_CHAINS = {
    "eth": "0x1", "ethereum": "0x1",
    "bsc": "0x38", "binance-smart-chain": "0x38",
    "polygon": "0x89", "polygon-pos": "0x89", "polygon_pos": "0x89",
    "arbitrum": "0xa4b1", "arbitrum-one": "0xa4b1",
    "base": "0x2105",
    "optimism": "0xa", "avalanche": "0xa86a", "avax": "0xa86a",
    "solana": "solana", "sol": "solana",
}

@app.get("/api/crypto/holders/{chain}/{address}")
async def get_token_holders(chain: str, address: str):
    """Top holderi + distributie (balene/rechini/...) via Moralis. EVM + Solana. Cache 5 min."""
    import time as _t
    import httpx as _httpx
    key = os.getenv("MORALIS_API_KEY")
    if not key:
        return api_ok({"available": False, "error": "no_api_key"})
    ch = (chain or "").lower().strip()
    mch = _MORALIS_CHAINS.get(ch)
    if not mch or not address:
        return api_ok({"available": False, "error": "unsupported_chain"})

    cache_key = mch + ":" + address
    cached = _holders_cache.get(cache_key)
    if cached and (_t.time() - cached[0]) < 300:
        return api_ok(cached[1])

    headers = {"X-API-Key": key, "accept": "application/json"}
    payload = {"available": True, "chain": ch, "total_holders": None,
               "distribution": None, "top_holders": [], "concentration_top10": None}
    try:
        async with _httpx.AsyncClient(timeout=15) as client:
            if mch == "solana":
                base = "https://solana-gateway.moralis.io/token/mainnet/" + address
                try:
                    rs = await client.get(base + "/holders", headers=headers)
                    if rs.status_code == 200:
                        j = rs.json() or {}
                        payload["total_holders"] = j.get("totalHolders")
                        payload["distribution"] = j.get("holderDistribution")
                except Exception:
                    pass
                try:
                    rt = await client.get(base + "/top-holders", headers=headers, params={"limit": 20})
                    if rt.status_code == 200:
                        jt = rt.json() or {}
                        rows = jt.get("result") or jt.get("holders") or []
                        payload["top_holders"] = [{
                            "address": h.get("ownerAddress") or h.get("address"),
                            "pct": h.get("percentageRelativeToTotalSupply") or h.get("percentage"),
                            "amount": h.get("balanceFormatted") or h.get("amount"),
                            "is_contract": h.get("isContract"),
                        } for h in rows[:20]]
                except Exception:
                    pass
            else:
                base = "https://deep-index.moralis.io/api/v2.2/erc20/" + address
                try:
                    ra = await client.get(base + "/holders", headers=headers, params={"chain": mch})
                    if ra.status_code == 200:
                        j = ra.json() or {}
                        payload["total_holders"] = j.get("totalHolders")
                        payload["distribution"] = j.get("holderDistribution")
                        sup = j.get("holderSupply") or {}
                        top10 = sup.get("top10") or {}
                        payload["concentration_top10"] = top10.get("supplyPercent")
                except Exception:
                    pass
                try:
                    rt = await client.get(base + "/owners", headers=headers,
                                          params={"chain": mch, "order": "DESC", "limit": 20})
                    if rt.status_code == 200:
                        jt = rt.json() or {}
                        rows = jt.get("result") or []
                        payload["top_holders"] = [{
                            "address": h.get("owner_address"),
                            "pct": h.get("percentage_relative_to_total_supply"),
                            "amount": h.get("balance_formatted"),
                            "label": h.get("owner_address_label"),
                            "is_contract": h.get("is_contract"),
                        } for h in rows[:20]]
                except Exception:
                    pass
    except Exception as e:
        return api_ok({"available": False, "error": str(e)})

    _holders_cache[cache_key] = (_t.time(), payload)
    return api_ok(payload)

_GP_PLATFORM_CANDIDATES = {
    "eth": ["ethereum", "eth"], "ethereum": ["ethereum", "eth"],
    "bsc": ["binance-smart-chain", "bsc"], "binance-smart-chain": ["binance-smart-chain", "bsc"],
    "polygon": ["polygon-pos", "polygon", "matic-network"], "polygon-pos": ["polygon-pos", "polygon"],
    "arbitrum": ["arbitrum-one", "arbitrum"], "arbitrum-one": ["arbitrum-one", "arbitrum"],
    "optimism": ["optimistic-ethereum", "optimism"],
    "avalanche": ["avalanche", "avalanche-2"], "avax": ["avalanche"],
    "base": ["base"],
    "solana": ["solana"], "sol": ["solana"],
}

def _osint_normalize_security(raw: dict) -> dict:
    out = {"available": False, "source": "goplus"}
    if not isinstance(raw, dict) or not raw.get("available"):
        if isinstance(raw, dict) and raw.get("error"):
            out["error"] = raw["error"]
        return out
    td = raw.get("data") or {}
    if not isinstance(td, dict) or not td:
        return out
    def _st(v):
        if isinstance(v, dict):
            return str(v.get("status", "")).strip()
        return str(v).strip() if v is not None else ""
    def _flag(k):
        return _st(td.get(k)) == "1"
    def _tax(k):
        x = _st(td.get(k))
        if x == "":
            return None
        try:
            f = float(x)
        except Exception:
            return None
        return round(f * 100, 2) if abs(f) <= 1 else round(f, 2)
    buy_tax = _tax("buy_tax")
    sell_tax = _tax("sell_tax")
    is_honeypot = _flag("is_honeypot")
    is_mintable = _flag("is_mintable") or _flag("mintable")
    pausable = _flag("transfer_pausable") or _flag("freezable")
    open_source = _flag("is_open_source")
    score = 65.0
    if is_honeypot: score -= 60
    if _flag("cannot_sell_all"): score -= 45
    if _flag("malicious_address"): score -= 40
    if _st(td.get("is_open_source")) != "" and not open_source: score -= 20
    if is_mintable: score -= 10
    if pausable: score -= 10
    if buy_tax and buy_tax >= 20: score -= 18
    if sell_tax and sell_tax >= 20: score -= 18
    score = max(0.0, min(100.0, score))
    contract_risk = "HIGH" if score < 40 else ("MEDIUM" if score < 65 else "LOW")
    out.update({
        "available": True, "source": "goplus",
        "is_honeypot": is_honeypot,
        "buy_tax": buy_tax, "sell_tax": sell_tax,
        "is_mintable": is_mintable, "transfer_pausable": pausable,
        "is_open_source": open_source,
        "contract_risk": contract_risk, "score": round(score, 1),
    })
    return out


def _osint_normalize_social(raw) -> dict:
    out = {"available": False, "source": "lunarcrush"}
    if not isinstance(raw, dict) or not raw:
        return out
    def _n(k):
        v = raw.get(k)
        try:
            return float(v) if v is not None else None
        except Exception:
            return None
    mentions = _n("mentions_24h")
    engagements = _n("engagements_24h")
    creators = _n("creators_24h")
    galaxy = _n("galaxy_score")
    alt_rank = _n("alt_rank")
    sentiment_pct = _n("sentiment_pct")
    social_dom = _n("social_dominance_pct")
    summary = (raw.get("summary") or "").strip()
    if (mentions is None and engagements is None and galaxy is None
            and sentiment_pct is None and social_dom is None and not summary):
        return out
    if sentiment_pct is None:
        sentiment = None
    elif sentiment_pct >= 60:
        sentiment = "Bullish"
    elif sentiment_pct <= 40:
        sentiment = "Bearish"
    else:
        sentiment = "Neutral"
    out.update({
        "available": True, "source": "lunarcrush",
        "mentions_24h": int(mentions) if mentions is not None else None,
        "engagements_24h": int(engagements) if engagements is not None else None,
        "creators_24h": int(creators) if creators is not None else None,
        "galaxy_score": galaxy,
        "alt_rank": int(alt_rank) if alt_rank is not None else None,
        "sentiment_pct": int(sentiment_pct) if sentiment_pct is not None else None,
        "sentiment": sentiment,
        "social_dominance_pct": social_dom,
        "summary": (raw.get("summary") or "")[:280] or None,
        "limited_mode": bool(raw.get("limited_mode")),
    })
    return out


_ETHERSCAN_CHAINIDS = {
    "eth": 1, "ethereum": 1, "bsc": 56, "binance-smart-chain": 56,
    "polygon": 137, "polygon-pos": 137, "arbitrum": 42161, "arbitrum-one": 42161,
    "optimism": 10, "avalanche": 43114, "avax": 43114, "base": 8453,
}
_deployer_cache = {}

def _osint_deployer(chain, address, goplus_raw=None) -> dict:
    import requests as _rq
    import time
    out = {"available": False, "source": "etherscan"}
    ch = (chain or "").lower().strip()
    addr = (address or "").strip()
    cid = _ETHERSCAN_CHAINIDS.get(ch)
    if not cid or not addr:
        out["error"] = "unsupported_chain"
        return out
    key = os.getenv("ETHERSCAN_API_KEY", "")
    if not key:
        out["error"] = "no_api_key"
        return out
    ck = str(cid) + ":" + addr.lower()
    cached = _deployer_cache.get(ck)
    if cached and (time.time() - cached[0]) < 1800:
        return cached[1]
    base = "https://api.etherscan.io/v2/api"
    try:
        r = _rq.get(base, params={"chainid": cid, "module": "contract",
                    "action": "getcontractcreation", "contractaddresses": addr, "apikey": key}, timeout=15)
        j = r.json() or {}
        rows = j.get("result") or []
        if j.get("status") != "1" or not rows:
            out["error"] = "no_creation_data"
            return out
        row = rows[0]
        creator = (row.get("contractCreator") or "").lower()
        created_ts = int(row.get("timestamp") or 0)
    except Exception as e:
        out["error"] = str(e)
        return out
    age_days = int((time.time() - created_ts) / 86400) if created_ts else None
    deployed_count = None
    try:
        rt = _rq.get(base, params={"chainid": cid, "module": "account", "action": "txlist",
                     "address": creator, "startblock": 0, "endblock": 99999999,
                     "page": 1, "offset": 100, "sort": "asc", "apikey": key}, timeout=15)
        jt = rt.json() or {}
        txs = jt.get("result") or []
        if isinstance(txs, list):
            deployed_count = sum(1 for t in txs if (t.get("to") in (None, "", "0x")) and t.get("contractAddress"))
    except Exception:
        deployed_count = None
    gp = (goplus_raw or {}).get("data") or {}
    def _flag(k):
        v = gp.get(k)
        return str(v.get("status") if isinstance(v, dict) else v) == "1"
    same_creator_honeypot = _flag("honeypot_with_same_creator")
    try:
        creator_pct = float(gp.get("creator_percent") or 0) * 100
    except Exception:
        creator_pct = None
    score = 70.0
    flags = []
    if same_creator_honeypot:
        score -= 50; flags.append("creator_made_honeypots")
    if age_days is not None:
        if age_days < 7: score -= 25; flags.append("contract_very_new")
        elif age_days < 30: score -= 12; flags.append("contract_new")
        elif age_days > 365: score += 10
    if deployed_count is not None and deployed_count >= 20:
        score -= 12; flags.append("prolific_deployer")
    if creator_pct is not None and creator_pct >= 5:
        score -= 15; flags.append("creator_holds_supply")
    score = max(0.0, min(100.0, score))
    risk_pct = round(100 - score, 1)
    risk_level = "HIGH" if risk_pct >= 60 else "MEDIUM" if risk_pct >= 35 else "LOW"
    out.update({
        "available": True, "source": "etherscan",
        "deployer": creator, "age_days": age_days,
        "deployed_contracts": deployed_count,
        "creator_percent": round(creator_pct, 4) if creator_pct is not None else None,
        "same_creator_honeypot": same_creator_honeypot,
        "risk_pct": risk_pct, "risk_level": risk_level, "flags": flags,
    })
    _deployer_cache[ck] = (time.time(), out)
    return out


def _osint_coingecko(symbol, name=None) -> dict:
    out = {"available": False, "source": "coingecko"}
    coin_id = ""
    try:
        if symbol:
            coin_id = resolve_coingecko_coin_id(symbol, preferred_name=name) or ""
            snap = get_coingecko_market_snapshot(symbol, preferred_name=name, preferred_coin_id=coin_id)
        else:
            snap = {}
    except Exception:
        return out
    if coin_id:
        out["coin_id"] = coin_id
    if not isinstance(snap, dict) or not snap:
        return out
    out.update({
        "available": True, "source": "coingecko",
        "coin_id": snap.get("id") or coin_id or None,
        "name": snap.get("name"),
        "image": snap.get("image"),
        "market_cap": snap.get("market_cap"),
        "market_cap_rank": snap.get("market_cap_rank"),
        "price_usd": snap.get("current_price"),
        "price_change_h24": snap.get("price_change_percentage_24h"),
    })
    return out


async def _osint_recent_with_logos(snap, limit=6):
    """Primele semnale unice + logo CoinGecko (paralel, cache-uit). Best-effort."""
    if not snap:
        return []
    seen = set()
    flat = []
    for c in ("pump_signals", "risk_signals", "watch_signals", "dump_signals", "dex_signals", "early_signals"):
        for s in (snap.get(c) or []):
            sym = (s.get("symbol") or "").upper()
            if sym and sym not in seen:
                seen.add(sym)
                flat.append(s)
            if len(flat) >= limit:
                break
        if len(flat) >= limit:
            break
    tasks = [asyncio.to_thread(_osint_coingecko, s.get("symbol"), s.get("name")) for s in flat]
    cgs = await asyncio.gather(*tasks, return_exceptions=True)
    out = []
    for s, cg in zip(flat, cgs):
        img = cg.get("image") if isinstance(cg, dict) else None
        out.append({
            "symbol": s.get("symbol"), "name": s.get("name"),
            "network": s.get("network"), "token_address": s.get("token_address"),
            "image": img,
        })
    return out


_CG_PLATFORM_TO_CHAIN = {
    "ethereum": "eth", "binance-smart-chain": "bsc", "polygon-pos": "polygon",
    "arbitrum-one": "arbitrum", "optimistic-ethereum": "optimism",
    "avalanche": "avalanche", "base": "base", "solana": "solana",
}

def _osint_resolve_token(query: str) -> dict:
    out = {"found": False}
    q = (query or "").strip()
    if not q:
        return out
    try:
        coin_id = resolve_coingecko_coin_id(q) or ""
        if not coin_id:
            return out
        details = get_coin_extended_details(coin_id)
        if not isinstance(details, dict) or not details:
            return out
        platform, address = pick_primary_contract(details)
        if not platform or not address:
            return out
        chain = _CG_PLATFORM_TO_CHAIN.get(platform, platform)
        out.update({
            "found": True, "chain": chain, "address": address,
            "coin_id": coin_id, "name": details.get("name"),
            "symbol": (details.get("symbol") or "").upper(),
        })
    except Exception:
        return {"found": False}
    return out


@app.get("/api/crypto/osint-resolve/{query}")
async def osint_resolve(query: str):
    """Rezolva orice symbol/coin -> chain+address via CoinGecko (pt tokenuri din afara snapshotului)."""
    res = await asyncio.to_thread(_osint_resolve_token, query)
    return api_ok(res)


@app.get("/api/crypto/osint/{chain}/{address}")
async def get_osint_overview(chain: str, address: str):
    """OSINT Lab Faza 1: agregare read-only (snapshot v2 + holders Moralis).
    NU atinge scanner/judge/categorii - doar citeste si combina."""
    addr = (address or "").strip()
    addr_l = addr.lower()
    ch = (chain or "").lower().strip()

    sig = None
    snap = await get_latest_snapshot(db)
    last_updated = snap.get("timestamp") if snap else None
    if snap:
        for cat in ("pump_signals", "dump_signals", "risk_signals",
                    "watch_signals", "dex_signals", "early_signals"):
            for s in (snap.get(cat) or []):
                ta = (s.get("token_address") or "").lower()
                sym = (s.get("symbol") or "").lower()
                if (addr_l and ta == addr_l) or (sym and sym == addr_l):
                    sig = s
                    break
            if sig:
                break

    holders = {}
    try:
        hres = await get_token_holders(ch, addr)
        if isinstance(hres, dict):
            holders = hres.get("data", {}) or {}
    except Exception:
        holders = {}

    security = {"available": False, "source": "goplus"}
    try:
        _gp_raw = {}
        for _plat in _GP_PLATFORM_CANDIDATES.get(ch, [ch]):
            _r = await asyncio.to_thread(get_goplus_security, _plat, addr)
            if isinstance(_r, dict) and _r.get("available"):
                _gp_raw = _r
                break
            _gp_raw = _r or _gp_raw
        security = _osint_normalize_security(_gp_raw)
    except Exception:
        security = {"available": False, "source": "goplus"}

    market_extra = {"available": False, "source": "coingecko"}
    try:
        _sym0 = (sig or {}).get("symbol")
        _nm0 = (sig or {}).get("name")
        if _sym0:
            market_extra = await asyncio.to_thread(_osint_coingecko, _sym0, _nm0)
    except Exception:
        market_extra = {"available": False, "source": "coingecko"}

    social = {"available": False, "source": "lunarcrush"}
    try:
        _sym = (sig or {}).get("symbol")
        _nm = (market_extra.get("name") if isinstance(market_extra, dict) else None) or (sig or {}).get("name")
        if _sym:
            _lc = await asyncio.to_thread(get_lunarcrush_topic_intelligence, _sym, _nm)
            social = _osint_normalize_social(_lc)
    except Exception:
        social = {"available": False, "source": "lunarcrush"}

    deployer = {"available": False, "source": "etherscan"}
    try:
        _gp_dep = await asyncio.to_thread(get_goplus_security, _GP_PLATFORM_CANDIDATES.get(ch, [ch])[0], addr)
        deployer = await asyncio.to_thread(_osint_deployer, ch, addr, _gp_dep)
    except Exception:
        deployer = {"available": False, "source": "etherscan"}

    recent = []
    try:
        recent = await _osint_recent_with_logos(snap)
    except Exception:
        recent = []

    def _risk_level(s):
        if not s:
            return "UNKNOWN"
        mp = s.get("manipulation_probability") or 0
        dr = (s.get("dump_risk_level") or "").lower()
        if dr == "high" or mp >= 60:
            return "HIGH"
        if dr == "medium" or mp >= 30:
            return "MEDIUM"
        return "LOW"

    s = sig or {}
    payload = {
        "found": sig is not None,
        "query": {"chain": ch, "address": addr},
        "last_updated": last_updated,
        "token": {
            "symbol": s.get("symbol"),
            "name": s.get("name"),
            "chain": s.get("network") or ch,
            "price_usd": s.get("price_usd"),
            "price_change_h24": s.get("price_change_h24"),
            "price_change_h1": s.get("price_change_h1"),
            "total_holders": holders.get("total_holders"),
        },
        "market": {
            "liquidity_usd": s.get("reserve_usd"),
            "volume_h24": s.get("volume_h24"),
            "buy_sell_ratio_h1": s.get("buy_sell_ratio_h1"),
            "pool_url": s.get("pool_url"),
        },
        "verdict": {
            "risk_level": _risk_level(sig),
            "verdict": s.get("verdict"),
            "confidence": s.get("confidence"),
            "reason": s.get("reason"),
            "ai_source": s.get("ai_source"),
            "dump_risk_level": s.get("dump_risk_level"),
            "manipulation_probability": s.get("manipulation_probability"),
        },
        "holders": holders,
        "signal_meta": {
            "sources": s.get("sources") or [],
            "mentions": s.get("mentions"),
            "multi_source": s.get("multi_source"),
            "pre_pump_activity": s.get("pre_pump_activity"),
            "red_flags": s.get("red_flags") or [],
            "whale_score": s.get("whale_score"),
            "whale_accumulation": s.get("whale_accumulation"),
            "whale_dump_risk": s.get("whale_dump_risk"),
            "whale_unique_buyers": s.get("whale_unique_buyers"),
            "whale_unique_sellers": s.get("whale_unique_sellers"),
        },
        "security": security,
        "social": social,
        "market_extra": market_extra,
        "recent": recent,
        "news": {"available": False, "source": "cryptopanic", "phase": 2},
        "deployer": deployer,
    }

    try:
        cgm = await asyncio.to_thread(lookup_coingecko_contract_metadata, ch, addr)
    except Exception:
        cgm = {}
    if not isinstance(cgm, dict):
        cgm = {}

    _sym = (sig or {}).get("symbol") or cgm.get("symbol")
    _nm = cgm.get("name") or (sig or {}).get("name")

    if sig is None and cgm.get("available"):
        md = cgm.get("market_data") or {}
        payload["token"]["symbol"] = payload["token"].get("symbol") or _sym
        payload["token"]["name"] = payload["token"].get("name") or _nm
        if payload["token"].get("price_usd") is None:
            payload["token"]["price_usd"] = md.get("current_price_usd")
        if payload["token"].get("price_change_h24") is None:
            payload["token"]["price_change_h24"] = md.get("price_change_24h")
        if payload["market"].get("volume_h24") is None:
            payload["market"]["volume_h24"] = md.get("total_volume_usd")
        try:
            cg2 = await asyncio.to_thread(_osint_coingecko, _sym, _nm)
        except Exception:
            cg2 = {}
        if isinstance(cg2, dict) and cg2.get("available"):
            payload["market_extra"] = cg2
        else:
            payload["market_extra"] = {
                "available": True, "source": "coingecko",
                "coin_id": cgm.get("coin_id"), "name": _nm,
                "market_cap": md.get("market_cap_usd"),
            }

    if (not isinstance(social, dict) or not social.get("available")) and _sym:
        try:
            _lc2 = await asyncio.to_thread(get_lunarcrush_topic_intelligence, _sym, _nm)
            _s2 = _osint_normalize_social(_lc2)
            if isinstance(_s2, dict) and _s2.get("available"):
                payload["social"] = _s2
        except Exception:
            pass

    if cgm.get("available"):
        md = cgm.get("market_data") or {}
        _soc = cgm.get("socials") or {}
        _links = []
        if cgm.get("website"):
            _links.append({"label": "Website", "url": cgm.get("website")})
        if _soc.get("twitter"):
            _links.append({"label": "Twitter/X", "url": "https://x.com/" + str(_soc["twitter"])})
        if _soc.get("telegram"):
            _links.append({"label": "Telegram", "url": "https://t.me/" + str(_soc["telegram"])})
        if _soc.get("subreddit"):
            _links.append({"label": "Reddit", "url": _soc["subreddit"]})
        payload["links"] = _links
        _cid = (payload.get("market_extra") or {}).get("coin_id") or cgm.get("coin_id")
        _snap = {}
        try:
            _snap = await asyncio.to_thread(get_coingecko_market_snapshot, _sym, _nm, _cid) or {}
        except Exception:
            _snap = {}
        payload["cg_metrics"] = {
            "available": True,
            "rank": _snap.get("market_cap_rank"),
            "ath": _snap.get("ath"),
            "ath_change_pct": _snap.get("ath_change_percentage"),
            "change_7d": _snap.get("price_change_percentage_7d_in_currency"),
            "circulating": _snap.get("circulating_supply"),
            "total_supply": _snap.get("total_supply"),
            "watchlist_users": cgm.get("watchlist_portfolio_users"),
        }
        payload["about"] = {
            "available": True,
            "categories": (cgm.get("categories") or [])[:4],
            "description": (cgm.get("description") or "")[:240],
        }

    if not payload["market"].get("pool_url"):
        try:
            _plat = COINGECKO_PLATFORM_BY_CHAIN.get((ch or "").strip().lower())
            _pools = await asyncio.to_thread(fetch_geckoterminal_token_pools, _plat, addr) if _plat else []
            _pools = sorted(_pools, key=lambda x: x.get("volume_usd") or 0, reverse=True)
            if _pools and _pools[0].get("url"):
                _best = _pools[0]
                payload["market"]["pool_url"] = _best["url"]
                payload["chart_meta"] = {
                    "dex": _best.get("name"),
                    "pair": _best.get("pair"),
                    "contract": addr,
                    "verified_source": "CoinGecko" if cgm.get("coin_id") else None,
                }
        except Exception:
            pass
    return api_ok(payload)



@app.post("/api/admin/trigger-scan-v2")
async def trigger_scan_v2(request: Request):
    """Trigger scan nou arhitectura v2 - localhost only"""
    client_host = request.client.host if request.client else ""
    if client_host not in ("127.0.0.1", "::1", "localhost"):
        return api_err("Forbidden", "FORBIDDEN")
    asyncio.create_task(run_full_scan(db))
    return api_ok({"started": True, "message": "Scan v2 triggered"})

@app.get("/api/crypto/signals-v2")
async def get_signals_v2(user=Depends(get_optional_user)):
    """Returneaza semnalele din noua arhitectura v2"""
    snap = await get_latest_snapshot(db)
    if not snap:
        return api_ok({
            "pump_signals": [],
            "dump_signals": [],
            "risk_signals": [],
            "watch_signals": [],
            "market_summary": "No v2 scan yet. Trigger /api/admin/trigger-scan-v2",
            "last_updated": None,
        })
    return api_ok({
        "pump_signals": snap.get("pump_signals", []),
        "dump_signals": snap.get("dump_signals", []),
        "risk_signals": snap.get("risk_signals", []),
        "watch_signals": snap.get("watch_signals", []),
        "dex_signals": snap.get("dex_signals", []),
        "early_signals": snap.get("early_signals", []),
        "market_summary": snap.get("market_summary", ""),
        "coins_analyzed": snap.get("coins_analyzed", 0),
        "last_updated": snap.get("timestamp"),
    })


@app.post("/api/crypto/ai-market-analysis")
async def ai_market_analysis(request: Request):
    try:
        body = await request.json()
        signals_summary = body.get("signals_summary", "")
        if not signals_summary:
            return api_ok({"analysis": "No signals available."})

        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            system="You are an expert crypto market analyst. Analyze the signals and provide a concise market direction outlook. Respond in English, max 4 clear sentences. Focus on: overall market direction, most imminent moves, key risks.",
            messages=[{"role": "user", "content": f"Current PumpRadar signals:\n{signals_summary}\n\nWhat is the market direction now? What moves are most imminent?"}]
        )
        analysis = message.content[0].text
        return api_ok({"analysis": analysis})
    except Exception as e:
        return api_err(f"Analysis failed: {str(e)}")


# ─── Signal Alert Email ────────────────────────────────────────────────────
def build_signal_card_html(signal: dict, app_url: str) -> str:
    cat = signal.get("category", "watch")
    sym = signal.get("symbol", "")
    name = signal.get("name", sym)
    network = signal.get("network", "")
    confidence = signal.get("confidence", 0)
    reason = signal.get("reason", "")
    price = signal.get("price_usd", 0)
    h1 = signal.get("price_change_h1", 0)
    h24 = signal.get("price_change_h24", 0)
    vol = signal.get("volume_h24", 0)
    whale_score = signal.get("whale_score", 0)
    manip = signal.get("manipulation_probability", 0)
    bs = signal.get("buy_sell_ratio_h1", 0)
    pre_pump = signal.get("pre_pump_activity", False)
    coin_url = f"{app_url}/coin/{sym}"

    def fmt_price(n):
        if not n: return "n/a"
        return f"${n:.2f}" if n > 1 else f"${n:.6f}"
    def fmt_vol(n):
        if not n: return "n/a"
        if n >= 1_000_000: return f"${n/1_000_000:.1f}M"
        if n >= 1_000: return f"${n/1_000:.0f}K"
        return f"${n:.0f}"
    def fmt_pct(n):
        arrow = "▲" if n > 0 else "▼" if n < 0 else "→"
        color = "#4ade80" if n > 0 else "#f87171" if n < 0 else "#94a3b8"
        return f'<span style="color:{color}">{arrow} {abs(n):.2f}%</span>'

    if cat == "early":
        bg = "#1c133a"
        badge_style = "background-color:rgba(147,51,234,0.3);color:#c084fc;"
        icon_style = "background-color:#6366f1;color:#fff;"
        text_color = "#a5b4fc"
        conf_color = "#818cf8"
        label_color = "#4f46e5"
        btn_style = "background-color:#6366f1;color:#fff;"
        badge_text = "⚡ Early Detection"
        headline = "Early movement detected — market hasn't reacted yet"
        desc = f"Our online scan detected unusual buying activity on {sym} before any price movement. Buy/sell ratio {bs:.1f} signals accumulation in progress. This is an early estimate based on real-time data — not a confirmed pump."
        btn_text = f"⚡ Monitor {sym} on PumpRadar →"
    elif cat == "pump":
        bg = "#0a2419"
        badge_style = "background-color:rgba(34,197,94,0.2);color:#4ade80;"
        icon_style = "background-color:#10b981;color:#fff;"
        text_color = "#86efac"
        conf_color = "#34d399"
        label_color = "#10b981"
        btn_style = "background-color:#10b981;color:#0d0f1a;"
        badge_text = "🔥 Pump Signal"
        headline = f"Pump imminent on {sym}" + (" — pre-pump activity confirmed" if pre_pump else "")
        desc = f"Our scan detected strong pump signals on {sym}. Whale score {whale_score}/100. {reason}"
        btn_text = f"🔥 View {sym} Signal →"
    else:
        bg = "#2a1215"
        badge_style = "background-color:rgba(239,68,68,0.2);color:#f87171;"
        icon_style = "background-color:#ef4444;color:#fff;"
        text_color = "#fca5a5"
        conf_color = "#f87171"
        label_color = "#ef4444"
        btn_style = "background-color:#dc2626;color:#fff;"
        badge_text = "☠️ Dump Warning"
        headline = f"Distribution risk detected on {sym} — high reversal probability"
        desc = f"Our scan detected dump/distribution signals on {sym}. Manipulation {manip}%. {reason}"
        btn_text = f"☠ View {sym} Risk →"

    return f"""
<tr>
  <td style="padding:0 16px 16px 16px;">
    <table width="100%" style="border-radius:12px;overflow:hidden;border-collapse:separate;">
      <tr>
        <td style="background-color:{bg};padding:20px;border-radius:12px;">
          <div style="display:inline-block;padding:4px 10px;border-radius:20px;font-size:10px;font-weight:bold;text-transform:uppercase;letter-spacing:1px;margin-bottom:15px;{badge_style}">{badge_text}</div>
          <table width="100%">
            <tr>
              <td width="54" valign="top">
                <div style="width:44px;height:44px;border-radius:50%;text-align:center;line-height:44px;font-weight:bold;font-size:14px;display:inline-block;{icon_style}">{sym[:2].upper()}</div>
              </td>
              <td valign="top">
                <div style="font-size:26px;font-weight:800;color:#fff;line-height:1;">{sym}</div>
                <div style="font-size:11px;margin-top:2px;color:{text_color};">{network}</div>
              </td>
              <td valign="top" align="right">
                <div style="font-size:32px;font-weight:800;text-align:right;line-height:1;color:{conf_color};">{confidence}%</div>
                <div style="font-size:10px;text-align:right;margin-top:2px;color:{text_color};">confidence</div>
              </td>
            </tr>
          </table>
          <div style="font-size:15px;font-weight:700;margin:20px 0 10px 0;line-height:1.4;color:{text_color};">{headline}</div>
          <div style="font-size:12px;line-height:1.5;margin-bottom:20px;color:{text_color};">{desc}</div>
          <table width="100%" cellspacing="5" style="margin-bottom:15px;">
            <tr>
              <td style="background:rgba(0,0,0,0.25);border-radius:6px;padding:10px;text-align:center;width:23%;">
                <div style="font-size:9px;text-transform:uppercase;font-weight:bold;margin-bottom:4px;color:{label_color};">Price</div>
                <div style="font-size:13px;font-weight:700;">{fmt_price(price)}</div>
              </td>
              <td style="background:rgba(0,0,0,0.25);border-radius:6px;padding:10px;text-align:center;width:23%;">
                <div style="font-size:9px;text-transform:uppercase;font-weight:bold;margin-bottom:4px;color:{label_color};">1h</div>
                <div style="font-size:13px;font-weight:700;">{fmt_pct(h1)}</div>
              </td>
              <td style="background:rgba(0,0,0,0.25);border-radius:6px;padding:10px;text-align:center;width:23%;">
                <div style="font-size:9px;text-transform:uppercase;font-weight:bold;margin-bottom:4px;color:{label_color};">24h</div>
                <div style="font-size:13px;font-weight:700;">{fmt_pct(h24)}</div>
              </td>
              <td style="background:rgba(0,0,0,0.25);border-radius:6px;padding:10px;text-align:center;width:23%;">
                <div style="font-size:9px;text-transform:uppercase;font-weight:bold;margin-bottom:4px;color:{label_color};">Vol 24h</div>
                <div style="font-size:13px;font-weight:700;">{fmt_vol(vol)}</div>
              </td>
            </tr>
          </table>
          <a href="{coin_url}" style="display:block;text-align:center;text-decoration:none;padding:14px;font-size:14px;font-weight:700;border-radius:8px;margin-top:5px;{btn_style}">{btn_text}</a>
        </td>
      </tr>
    </table>
  </td>
</tr>"""


async def send_signal_alert_emails(db, signals: list):
    """Send signal alert email to all active subscribers."""
    if not signals:
        return

    early = [s for s in signals if s.get("category") == "early" and s.get("pre_pump_activity")]
    pumps = [s for s in signals if s.get("category") == "pump" and s.get("confidence", 0) >= 75]
    dumps = [s for s in signals if s.get("category") in ("dump", "risk") and s.get("confidence", 0) >= 70]

    alert_signals = early + pumps + dumps
    if not alert_signals:
        return

    # get active subscribers
    try:
        users = await db.users.find(
            {"subscription": {"$in": ["pro", "trial", "active"]}, "email": {"$exists": True}}
        ).to_list(length=1000)
    except Exception as e:
        logger.error(f"Signal alert: failed to fetch users: {e}")
        return

    if not users:
        return

    from datetime import datetime as dt
    now_str = dt.now().strftime("%A, %B %d · %H:%M")

    cards_html = "".join([build_signal_card_html(s, APP_URL) for s in alert_signals[:5]])

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Signal Alert - PumpRadar V2</title>
</head>
<body style="margin:0;padding:0;background-color:#0d0f1a;font-family:'Segoe UI',-apple-system,BlinkMacSystemFont,Arial,sans-serif;color:#ffffff;">
<center style="width:100%;background-color:#0d0f1a;padding:20px 0;">
<table style="background-color:#121424;margin:0 auto;width:100%;max-width:550px;border-spacing:0;border-radius:16px;overflow:hidden;">

  <!-- HEADER -->
  <tr>
    <td style="padding:35px 20px 25px 20px;text-align:center;background:linear-gradient(180deg,#161933 0%,#121424 100%);">
      <div style="color:#707599;font-size:11px;text-transform:uppercase;letter-spacing:2px;font-weight:bold;margin-bottom:5px;">Arbitrajz · PumpRadar V2</div>
      <h1 style="margin:0;font-size:32px;color:#ffffff;font-weight:800;letter-spacing:-0.5px;">Signal Alert</h1>
      <div style="color:#525875;font-size:11px;margin-top:6px;">{now_str} · AI powered detection</div>
    </td>
  </tr>

  {cards_html}

  <!-- DASHBOARD BUTTON -->
  <tr>
    <td style="padding:20px 16px;">
      <a href="{APP_URL}/dashboard" style="background-color:#4f46e5;color:#ffffff;text-decoration:none;display:block;text-align:center;padding:16px;border-radius:8px;font-weight:700;font-size:15px;">Open Full Dashboard →</a>
    </td>
  </tr>

  <!-- FOOTER -->
  <tr>
    <td style="text-align:center;padding:30px 20px;font-size:11px;color:#414666;line-height:1.6;background-color:#0b0c16;">
      <div>ArbitrajZ · PumpRadar V2 · AI-powered crypto signal detection</div>
      <div style="margin-top:4px;">AI-generated signals. Not financial advice. Trade responsibly.</div>
    </td>
  </tr>

</table>
</center>
</body>
</html>"""

    subject_parts = []
    if pumps: subject_parts.append(f"🔥 {pumps[0]['symbol']} Pump")
    if early: subject_parts.append(f"⚡ {early[0]['symbol']} Early")
    if dumps: subject_parts.append(f"☠️ {dumps[0]['symbol']} Risk")
    subject = "PumpRadar: " + " · ".join(subject_parts[:3])

    sent = 0
    for user in users:
        email = user.get("email")
        if not email:
            continue
        try:
            await asyncio.to_thread(resend.Emails.send, {
                "from": f"PumpRadar <{SENDER_EMAIL}>",
                "to": [email],
                "subject": subject,
                "html": html,
            })
            sent += 1
        except Exception as e:
            logger.error(f"Signal alert email error for {email}: {e}")

    logger.info(f"Signal alert: sent to {sent}/{len(users)} subscribers — {subject}")


@app.get("/api/crypto/history-v2")
async def get_history_v2(limit: int = 48):
    """Get historical signals from signal_snapshots_v2"""
    snapshots = await db.signal_snapshots_v2.find({}).sort("timestamp", -1).limit(limit).to_list(length=limit)
    result = []
    for snap in snapshots:
        snap.pop("_id", None)
        ts = snap.get("timestamp")
        if isinstance(ts, datetime):
            ts = ts.isoformat()
        pump = snap.get("pump_signals", [])
        dump = snap.get("dump_signals", [])
        risk = snap.get("risk_signals", [])
        watch = snap.get("watch_signals", [])
        early = snap.get("early_signals", [])
        dex = snap.get("dex_signals", [])
        result.append({
            "timestamp": ts,
            "pump_count": len(pump),
            "dump_count": len(dump),
            "risk_count": len(risk),
            "watch_count": len(watch),
            "early_count": len(early),
            "dex_count": len(dex),
            "coins_analyzed": snap.get("coins_analyzed", 0),
            "market_summary": snap.get("market_summary", ""),
            "signals": {
                "pump": [{"symbol": s.get("symbol"), "confidence": s.get("confidence"), "verdict": s.get("verdict"), "price_usd": s.get("price_usd"), "price_change_h1": s.get("price_change_h1"), "price_change_h24": s.get("price_change_h24"), "whale_accumulation": s.get("whale_accumulation"), "pre_pump_activity": s.get("pre_pump_activity")} for s in pump],
                "dump": [{"symbol": s.get("symbol"), "confidence": s.get("confidence"), "verdict": s.get("verdict")} for s in dump],
                "risk": [{"symbol": s.get("symbol"), "confidence": s.get("confidence"), "verdict": s.get("verdict"), "manipulation_probability": s.get("manipulation_probability")} for s in risk],
                "early": [{"symbol": s.get("symbol"), "confidence": s.get("confidence"), "verdict": s.get("verdict"), "pre_pump_activity": s.get("pre_pump_activity")} for s in early],
                "watch": [{"symbol": s.get("symbol"), "confidence": s.get("confidence")} for s in watch],
                "dex": [{"symbol": s.get("symbol"), "confidence": s.get("confidence")} for s in dex],
            }
        })
    return api_ok({"history": result, "total": len(result)})


@app.get("/api/crypto/coingecko-search")
async def coingecko_search(query: str):
    """Proxy CoinGecko search pentru frontend"""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"https://api.coingecko.com/api/v3/search?query={query}",
                timeout=8
            )
            return r.json()
    except Exception as e:
        return {"coins": [], "error": str(e)}


@app.get("/api/crypto/coingecko-coin/{coin_id}")
async def coingecko_coin(coin_id: str):
    """Proxy CoinGecko coin details pentru frontend"""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false",
                timeout=10
            )
            return r.json()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/crypto/token-contract/{symbol}")
async def get_token_contract(symbol: str):
    """Cauta contract address via DexScreener pentru tokenuri fara contract in CoinGecko"""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"https://api.dexscreener.com/latest/dex/search?q={symbol}",
                timeout=8
            )
            data = r.json()
            pairs = data.get("pairs", [])
            if not pairs:
                return {"contract": None, "chain": None}
            
            # Gasim perechea cu cel mai mare volum care are simbolul exact
            best = None
            for p in pairs:
                base = p.get("baseToken", {})
                if base.get("symbol", "").upper() == symbol.upper():
                    if not best or (p.get("volume", {}).get("h24", 0) > best.get("volume", {}).get("h24", 0)):
                        best = p
            
            if not best:
                best = pairs[0]
            
            base = best.get("baseToken", {})
            chain = best.get("chainId", "")
            
            chain_map = {
                "ethereum": "ethereum", "eth": "ethereum",
                "bsc": "binance-smart-chain", "solana": "solana",
                "polygon": "polygon-pos", "arbitrum": "arbitrum-one",
                "base": "base",
            }
            
            return {
                "contract": base.get("address"),
                "chain": chain_map.get(chain.lower(), chain),
                "chain_raw": chain,
                "name": base.get("name"),
                "symbol": base.get("symbol"),
                "volume_h24": best.get("volume", {}).get("h24", 0),
            }
    except Exception as e:
        return {"contract": None, "chain": None, "error": str(e)}



KNOWN_CONTRACTS = {
    "pepe": {"contract": "0x6982508145454Ce325dDbE47a25d4ec3d2311933", "chain": "ethereum"},
    "dogecoin": {"contract": None, "chain": None},
    "orca": {"contract": "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE", "chain": "solana"},
    "shiba-inu": {"contract": "0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE", "chain": "ethereum"},
    "floki": {"contract": "0xcf0C122c6b73ff809C693DB761e7BaeBe62b6a2E", "chain": "ethereum"},
    "baby-doge-coin": {"contract": "0xc748673057861a797275CD8A068AbB95A902e8de", "chain": "binance-smart-chain"},
}

@app.get("/api/crypto/token-contract-by-id/{coin_id}")
async def get_token_contract_by_id(coin_id: str):
    """Cauta contract via CoinGecko coin ID direct"""
    # Check known contracts first
    if coin_id.lower() in KNOWN_CONTRACTS:
        return KNOWN_CONTRACTS[coin_id.lower()]
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&market_data=false&community_data=false&developer_data=false",
                timeout=10
            )
            data = r.json()
            platforms = data.get("platforms", {})
            priority = ['ethereum','binance-smart-chain','solana','polygon-pos','arbitrum-one','base']
            for chain in priority:
                if platforms.get(chain):
                    return {"contract": platforms[chain], "chain": chain}
            entries = [(k,v) for k,v in platforms.items() if v]
            if entries:
                return {"contract": entries[0][1], "chain": entries[0][0]}
            return {"contract": None, "chain": None}
    except Exception as e:
        return {"contract": None, "chain": None, "error": str(e)}
