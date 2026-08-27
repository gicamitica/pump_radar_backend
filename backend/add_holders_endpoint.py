#!/usr/bin/env python3
# Adauga endpoint-ul /api/crypto/holders/{chain}/{address} in server.py
# Ruleaza pe cloud:  cd /srv/data/pump_radar/backend && .venv/bin/python add_holders_endpoint.py
import sys, time, shutil, os

P = "/srv/data/pump_radar/backend/server.py"

s = open(P, encoding="utf-8").read()

if "api/crypto/holders" in s:
    print("DEJA EXISTA endpoint-ul holders. Nu fac nimic.")
    sys.exit(0)

anchor = '@app.post("/api/admin/trigger-scan-v2")'
n = s.count(anchor)
if n != 1:
    print("EROARE: anchor gasit de %d ori (astept 1). Nu modific." % n)
    sys.exit(1)

# backup
bak = P + ".bak-holders-" + time.strftime("%Y%m%d_%H%M")
shutil.copy(P, bak)
print("Backup:", bak)

block = r'''_holders_cache: Dict[str, tuple] = {}

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

'''

s2 = s.replace(anchor, block + anchor, 1)

# verifica sintaxa inainte de a scrie
import ast
try:
    ast.parse(s2)
except SyntaxError as e:
    print("EROARE de sintaxa dupa modificare, NU am scris:", e)
    sys.exit(1)

open(P, "w", encoding="utf-8").write(s2)
print("OK - endpoint /api/crypto/holders adaugat si sintaxa valida.")
