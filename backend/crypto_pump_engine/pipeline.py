from __future__ import annotations

import asyncio
from typing import Dict, Iterable, List, Optional

import httpx

from .config import CONFIG, normalize_chain
from .logger import EngineLogger
from .models import PumpEngineFeatures, PumpEngineResult
from .stage1_intake import resolve_pair_for_token
from .stage1_intake import run_stage as run_stage1
from .stage2_market import run_stage as run_stage2
from .stage3_safety import run_stage as run_stage3
from .stage4_holders import run_stage as run_stage4
from .stage5_social import run_stage as run_stage5
from .stage6_rules import run_stage as run_stage6
from .ai_judge import (
    build_llm_evidence_payload,
    run_llm_decision_layer,
    merge_rule_and_ai,
    should_run_ai_judge,
)


class PumpEngineError(Exception):
    def __init__(self, message: str, status_code: int = 400, code: str = "PUMP_ENGINE_ERROR"):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code


class Pipeline:
    def __init__(self, logger: Optional[EngineLogger] = None, config: Optional[Dict[str, object]] = None):
        self.config = config or CONFIG
        self.logger = logger or EngineLogger(str(self.config["db_path"]))

    async def run_single(
        self,
        chain: str,
        pair_address: str,
        token_address: Optional[str] = None,
        use_ai_judge: bool = False,
    ) -> Dict[str, object]:
        normalized_chain = normalize_chain(chain)
        timeout = httpx.Timeout(float(self.config["timeout_seconds"]))

        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                intake = await run_stage1(client, normalized_chain, pair_address, token_address)
                market = run_stage2(intake)
                safety = await run_stage3(client, intake)
                holders = await run_stage4(client, intake)
                social = run_stage5(intake)
                rules = run_stage6(market, safety, holders, social)
        except httpx.HTTPStatusError as exc:
            raise PumpEngineError(
                f"External API error while analyzing {normalized_chain}/{pair_address}: {exc.response.status_code}",
                status_code=502,
                code="PUMP_ENGINE_UPSTREAM_ERROR",
            ) from exc
        except httpx.HTTPError as exc:
            raise PumpEngineError(
                f"Network error while analyzing {normalized_chain}/{pair_address}: {exc}",
                status_code=502,
                code="PUMP_ENGINE_NETWORK_ERROR",
            ) from exc
        except ValueError as exc:
            raise PumpEngineError(str(exc), status_code=404, code="PUMP_ENGINE_NOT_FOUND") from exc

        result = PumpEngineResult(
            candidate_id=intake.candidate_id,
            token_address=intake.token_address,
            pair_address=intake.pair_address,
            chain=intake.chain,
            action=rules.action,
            verdict=rules.verdict,
            score=round(rules.score, 2),
            features=PumpEngineFeatures(
                liquidity_usd=round(market.liquidity_usd, 2),
                safety_score=round(safety.safety_score, 2),
                concentration=round(holders.concentration, 4),
                volume_24h_usd=round(market.volume_24h_usd, 2),
                volume_liquidity_ratio=round(market.volume_liquidity_ratio, 4),
                buy_sell_ratio=round(market.buy_sell_ratio, 4),
                pair_age_minutes=market.pair_age_minutes,
                social_score=round(social.social_score, 2),
                boost_count=social.boost_count,
                suspect=(
                    "hard_veto" in rules.risks
                    or "low_safety_score" in rules.risks
                    or "holder_concentration_high" in rules.risks
                    or "holder_concentration_elevated" in rules.risks
                    or "thin_liquidity" in rules.risks
                    or "market_distribution" in rules.risks
                    or "market_dump_breakdown" in rules.risks
                    or "market_seller_pressure" in rules.risks
                    or "market_no_liquidity" in rules.risks
                ),
                suspect_reasons=[
                    r for r in rules.risks if r in {
                        "hard_veto",
                        "low_safety_score",
                        "holder_concentration_high",
                        "holder_concentration_elevated",
                        "thin_liquidity",
                        "market_distribution",
                        "market_dump_breakdown",
                        "market_seller_pressure",
                        "market_no_liquidity",
                    }
                ],
            ),
            risks=rules.risks,
        )
        payload = result.model_dump(mode="json")
        diagnostics = {
            "market": market.model_dump(mode="json"),
            "safety": safety.model_dump(mode="json"),
            "holders": holders.model_dump(mode="json"),
            "social": social.model_dump(mode="json"),
        }

        final_response: Dict[str, object] = payload
        ai_evidence = None
        ai_result = None

        if use_ai_judge:
            ai_evidence = build_llm_evidence_payload(
                chain=normalized_chain,
                pair_address=pair_address,
                token_address=token_address,
                result_payload=payload,
                diagnostics=diagnostics,
            )
            if should_run_ai_judge(ai_evidence):
                ai_result = run_llm_decision_layer(ai_evidence)
            final_response = merge_rule_and_ai(payload, ai_result, ai_evidence)

        await self.logger.log_run(
            {
                "request": {
                    "chain": normalized_chain,
                    "pair_address": pair_address,
                    "token_address": token_address,
                    "use_ai_judge": use_ai_judge,
                },
                "result": payload,
                "diagnostics": diagnostics,
                "ai_evidence": ai_evidence,
                "ai_result": ai_result,
                "final_result": final_response,
            }
        )
        return final_response

    async def run_batch(self, items: Iterable[Dict[str, str]]) -> List[Dict[str, object]]:
        requests = list(items)
        if len(requests) > int(self.config["max_batch_size"]):
            raise PumpEngineError(
                f"Batch too large. Maximum supported size is {self.config['max_batch_size']}.",
                status_code=400,
                code="PUMP_ENGINE_BATCH_TOO_LARGE",
            )

        tasks = [
            self.run_single(
                chain=item["chain"],
                pair_address=item["pair_address"],
                token_address=item.get("token_address"),
            )
            for item in requests
        ]
        return await asyncio.gather(*tasks)

    async def run_from_token(
        self,
        chain: str,
        token_address: str,
        use_ai_judge: bool = False,
    ) -> Dict[str, object]:
        normalized_chain = normalize_chain(chain)
        timeout = httpx.Timeout(float(self.config["timeout_seconds"]))

        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                pair = await resolve_pair_for_token(client, normalized_chain, token_address)
        except httpx.HTTPStatusError as exc:
            raise PumpEngineError(
                f"External API error while resolving pair for {normalized_chain}/{token_address}: {exc.response.status_code}",
                status_code=502,
                code="PUMP_ENGINE_UPSTREAM_ERROR",
            ) from exc
        except httpx.HTTPError as exc:
            raise PumpEngineError(
                f"Network error while resolving pair for {normalized_chain}/{token_address}: {exc}",
                status_code=502,
                code="PUMP_ENGINE_NETWORK_ERROR",
            ) from exc

        if not pair or not pair.get("pairAddress"):
            raise PumpEngineError(
                f"No DexScreener pair found for chain={normalized_chain} token={token_address}",
                status_code=404,
                code="PUMP_ENGINE_PAIR_NOT_FOUND",
            )

        return await self.run_single(
            chain=normalized_chain,
            pair_address=pair["pairAddress"],
            token_address=token_address,
            use_ai_judge=use_ai_judge,
        )
