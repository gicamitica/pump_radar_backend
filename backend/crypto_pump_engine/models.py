from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class PairTokenRef(BaseModel):
    address: str = ""
    name: str = ""
    symbol: str = ""


class IntakeResult(BaseModel):
    chain: str
    pair_address: str
    token_address: str
    candidate_id: str
    pair_url: Optional[str] = None
    dex_id: Optional[str] = None
    base_token: PairTokenRef = Field(default_factory=PairTokenRef)
    quote_token: PairTokenRef = Field(default_factory=PairTokenRef)
    price_usd: Optional[float] = None
    liquidity_usd: float = 0.0
    volume_24h_usd: float = 0.0
    buys_24h: int = 0
    sells_24h: int = 0
    price_change_24h_pct: float = 0.0
    pair_created_at_ms: Optional[int] = None
    boost_count: int = 0
    paid_order_count: int = 0
    profile_links: List[Dict[str, Any]] = Field(default_factory=list)
    websites: List[str] = Field(default_factory=list)
    socials: List[Dict[str, str]] = Field(default_factory=list)
    labels: List[str] = Field(default_factory=list)
    raw_pair: Dict[str, Any] = Field(default_factory=dict)
    stage_mode: str = "live"
    risks: List[str] = Field(default_factory=list)


class MarketResult(BaseModel):
    liquidity_usd: float = 0.0
    volume_24h_usd: float = 0.0
    volume_liquidity_ratio: float = 0.0
    buy_sell_ratio: float = 0.0
    pair_age_minutes: Optional[float] = None
    momentum_score: float = 0.0
    market_score: float = 0.0
    market_verdict: str = "neutral"
    risks: List[str] = Field(default_factory=list)
    notes: List[str] = Field(default_factory=list)


class SafetyProviderResult(BaseModel):
    provider: str
    available: bool = False
    score: Optional[float] = None
    hard_veto: bool = False
    risks: List[str] = Field(default_factory=list)
    details: Dict[str, Any] = Field(default_factory=dict)


class SafetyResult(BaseModel):
    safety_score: float = 0.0
    risk_level: str = "unknown"
    hard_veto: bool = False
    tax_conflict: bool = False
    risks: List[str] = Field(default_factory=list)
    providers: Dict[str, SafetyProviderResult] = Field(default_factory=dict)


class HoldersResult(BaseModel):
    concentration: float = 0.35
    top_1_share: Optional[float] = None
    top_5_share: Optional[float] = None
    top_10_share: Optional[float] = None
    holder_count_sample: int = 0
    source_mode: str = "mock"
    risks: List[str] = Field(default_factory=list)


class SocialResult(BaseModel):
    social_score: float = 0.0
    source_mode: str = "fallback_demo"
    link_count: int = 0
    website_count: int = 0
    boost_count: int = 0
    paid_order_count: int = 0
    risks: List[str] = Field(default_factory=list)


class RulesResult(BaseModel):
    action: str
    verdict: str
    score: float
    risks: List[str] = Field(default_factory=list)


class PumpEngineFeatures(BaseModel):
    liquidity_usd: float
    safety_score: float
    concentration: float
    volume_24h_usd: Optional[float] = None
    volume_liquidity_ratio: Optional[float] = None
    buy_sell_ratio: Optional[float] = None
    pair_age_minutes: Optional[float] = None
    social_score: Optional[float] = None
    boost_count: Optional[int] = None
    suspect: Optional[bool] = None
    suspect_reasons: List[str] = Field(default_factory=list)


class PumpEngineResult(BaseModel):
    candidate_id: str
    token_address: str
    pair_address: str
    chain: str
    action: str
    verdict: str
    score: float
    features: PumpEngineFeatures
    risks: List[str] = Field(default_factory=list)
    timestamp: str = Field(default_factory=utc_now_iso)
