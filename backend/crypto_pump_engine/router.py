from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .config import CONFIG
from .logger import EngineLogger
from .pipeline import Pipeline, PumpEngineError

router = APIRouter(tags=["pump-engine"])


class AnalyzeRequest(BaseModel):
    token_address: Optional[str] = None
    use_ai_judge: bool = False


_pump_engine: Optional[Pipeline] = None


def get_pipeline() -> Pipeline:
    global _pump_engine
    if _pump_engine is None:
        logger = EngineLogger(str(CONFIG["db_path"]))
        _pump_engine = Pipeline(logger=logger, config=CONFIG)
    return _pump_engine


@router.post("/api/pump-engine/analyze/{chain}/{pair_address}")
async def pump_engine_analyze(chain: str, pair_address: str, body: Optional[AnalyzeRequest] = None):
    try:
        return await get_pipeline().run_single(
            chain=chain,
            pair_address=pair_address,
            token_address=body.token_address if body else None,
            use_ai_judge=body.use_ai_judge if body else False,
        )
    except PumpEngineError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"code": exc.code, "message": exc.message},
        ) from exc
