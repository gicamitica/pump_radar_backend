"""
funding_routes.py - ETH gas-funding source tracing. NEW module, isolated.
"""
import os
import time
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict

import requests
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api/crypto/funding", tags=["funding"])

ETHERSCAN_BASE = "https://api.etherscan.io/v2/api"
INFRA_TX_THRESHOLD = 500
CACHE_TTL = 1800
OWN_OSINT_BASE = "http://127.0.0.1:8020"

def _get_key() -> str:
    return os.environ.get("ETHERSCAN_API_KEY", "")

KNOWN_CEX = {
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "Binance",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "Binance",
    "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": "Binance",
    "0x9696f59e4d72e237be84ffd425dcad154bf96976": "Binance",
    "0xf977814e90da44bfa03b6295a0616a897441acec": "Binance",
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "Binance",
    "0x030e37ddd7df1b43db172b23916d523f1599c6cc": "Binance",
    "0x00799bbc833d5b168f0410312d2a8fd9e0e3079c": "Binance",
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "Coinbase",
    "0x503828976d22510aad0201ac7ec88293211d23da": "Coinbase",
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "Coinbase",
    "0x3cd751e6b0078be393132286c442345e5dc49699": "Coinbase",
    "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0": "Kraken",
    "0xa910f92acdaf488fa6ef02174fb86208ad7722ba": "Kraken",
    "0xe93381fb4c4f14bda253907b18fad305d799241a": "Huobi",
    "0x46340b20830761efd32832a74d7169b29feb9758": "Crypto.com",
    # --- Binance additional hot wallets ---
    "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8": "Binance",
    "0x8894e0a0c962cb723c1976a4421c95949be2d4e3": "Binance",
    "0xe2fc31f816a9b94326492132018c3aecc4a93ae1": "Binance",
    "0x564286362092d8e7936f0549571a803b203aaced": "Binance",
    "0x0681d8db095565fe8a346fa0277bffde9c0edbbf": "Binance",
    "0xfe9e8709d3215310075d67e3ed32a380ccf451c8": "Binance",
    "0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67": "Binance",
    "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8": "Binance",
    "0xd551234ae421e3bcba99a0da6d736074f22192ff": "Binance",
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance",
    # --- Coinbase additional ---
    "0xa090e606e30bd747d4e6245a1517ebe430f0057e": "Coinbase",
    "0x77696bb39917c91a0c3908d577d5e322095425ca": "Coinbase",
    "0x95a9bd206ae52c4ba8eecfc93d18eacdd41c88cc": "Coinbase",
    "0xb739d0895772dbb71a89a3754a160269068f0d45": "Coinbase",
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "Coinbase",
    "0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511": "Coinbase",
    "0xeb2629a2734e272bcc07bda959863f316f4bd4cf": "Coinbase",
    "0xd688aea8f7d450909ade10c47faa95707b0682d9": "Coinbase",
    "0x02466e547bfdab679fc49e96bbfc62b9747d997c": "Coinbase",
    # --- Kraken additional ---
    "0xda9dfa130df4de4673b89022ee50ff26f6ea73cf": "Kraken",
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": "Kraken",
    "0xe853c56864a2ebe4576a807d26fdc4a0ada51919": "Kraken",
    "0xfa52274dd61e1643d2205169732f29114bc240b3": "Kraken",
    "0x53d284357ec70ce289d6d64134dfac8e511c8a3d": "Kraken",
    "0xae2d4617c862309a3d75a0ffb358c7a5009c673f": "Kraken",
    # --- OKX ---
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "OKX",
    "0x236f9f97e0e62388479bf9e5ba4889e46b0273c3": "OKX",
    "0xa7efae728d2936e78bda97dc267687568dd593f3": "OKX",
    "0x2c8fbb630289363ac80705a1a61273f76fd5a161": "OKX",
    "0x868daB0b8E21EC0a48b76A7D8e5F0B29CE39F2f6": "OKX",
    "0x5041ed759dd4afc3a72b8192c143f72f4724081a": "OKX",
    # --- Bybit ---
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40": "Bybit",
    "0xee5b5b923ffce93a870b3104b7ca09c3db80047a": "Bybit",
    # --- Gate.io ---
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "Gate.io",
    "0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c": "Gate.io",
    # --- KuCoin ---
    "0x2b5634c42055806a59e9107ed44d43c426e58258": "KuCoin",
    "0x689c56aef474df92d44a1b70850f808488f9769c": "KuCoin",
    "0xa1d8d972560c2f8144af871db508f0b0b10a3fbf": "KuCoin",
    "0x4ad64983349c49defe8d7a4686202d24b25d0ce8": "KuCoin",
    "0xd6216fc19db775df9774a6e33526131da7d19a2c": "KuCoin",
    # --- MEXC ---
    "0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88": "MEXC",
    "0x9642b23ed1e01df1092b92641051881a322f5d4e": "MEXC",
    # --- Bitfinex ---
    "0x1151314c646ce4e0efd76d1af4760ae66a9fe30f": "Bitfinex",
    "0x876eabf441b2ee5b5b0554fd502a8e0600950cfa": "Bitfinex",
    "0x742d35cc6634c0532925a3b844bc454e4438f44e": "Bitfinex",
    "0x1a8c53147e7b61c015159723408762fc60a34d17": "Bitfinex",
    # --- Huobi/HTX additional ---
    "0xdc76cd25977e0a5ae17155770273ad58648900d3": "Huobi",
    "0xab5c66752a9e8167967685f1450532fb96d5d24f": "Huobi",
    "0xfdb16996831753d5331ff813c29a93c76834a0ad": "Huobi",
    "0xeee28d484628d41a82d01e21d12e2e78d69920da": "Huobi",
    # --- Crypto.com additional ---
    "0x72a53cdbbcc1b9efa39c834a540550e23463aacb": "Crypto.com",
    "0xcffad3200574698b78f32232aa9d63eabd290703": "Crypto.com",
    # --- Bitget ---
    "0x5bdf85216ec1e38d6458c870992a69e38e03f7ef": "Bitget",
    "0x0639556f03714a74a5feeaf5736a4a64ff70d206": "Bitget",
}

_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_infra_cache: Dict[str, bool] = {}


def _api_get(params: dict, retries: int = 2) -> dict:
    for _ in range(retries + 1):
        try:
            r = requests.get(ETHERSCAN_BASE, params=params, timeout=20)
            return r.json()
        except Exception:
            time.sleep(0.4)
    return {}


def _funding_source(wallet: str, key: str) -> Tuple[Optional[str], float]:
    wallet = wallet.lower()
    d = _api_get({
        "chainid": 1, "module": "account", "action": "txlist", "address": wallet,
        "startblock": 0, "endblock": 99999999, "page": 1, "offset": 20,
        "sort": "asc", "apikey": key,
    })
    if d.get("status") == "1":
        for tx in d.get("result", []):
            if tx.get("to", "").lower() == wallet and int(tx.get("value", 0)) > 0:
                return tx["from"].lower(), int(tx["value"]) / 1e18
    return None, 0.0


def _is_infrastructure(addr: str, key: str) -> bool:
    if addr in _infra_cache:
        return _infra_cache[addr]
    d = _api_get({
        "chainid": 1, "module": "account", "action": "txlist", "address": addr,
        "startblock": 0, "endblock": 99999999, "page": 1, "offset": INFRA_TX_THRESHOLD + 1,
        "sort": "asc", "apikey": key,
    })
    n = len(d.get("result", [])) if d.get("status") == "1" else 0
    infra = n > INFRA_TX_THRESHOLD
    _infra_cache[addr] = infra
    time.sleep(0.2)
    return infra


def _get_holders(token: str, limit: int = 15) -> List[str]:
    try:
        r = requests.get(f"{OWN_OSINT_BASE}/api/crypto/osint/eth/{token}", timeout=30)
        d = r.json().get("data", {})
        th = d.get("holders", {}).get("top_holders", []) or []
        addrs = [h.get("address", "").lower() for h in th if h.get("address")]
        addrs = [a for a in addrs if a and not a.startswith("0x00000000000000")]
        return addrs[:limit]
    except Exception:
        return []


def _analyze_sync(token: str) -> Dict[str, Any]:
    key = _get_key()
    if not key:
        return {"available": False, "error": "no_etherscan_key"}
    holders = _get_holders(token)
    if not holders:
        return {"available": False, "error": "no_holders"}
    sources: Dict[str, List[str]] = defaultdict(list)
    cex_count = infra_count = no_source = 0
    for w in holders:
        src, _eth = _funding_source(w, key)
        time.sleep(0.2)
        if src is None:
            no_source += 1
        elif src in KNOWN_CEX:
            cex_count += 1
        elif _is_infrastructure(src, key):
            infra_count += 1
        else:
            sources[src].append(w)
    clusters = [
        {"source": s, "wallet_count": len(ws), "wallets": ws}
        for s, ws in sources.items() if len(ws) >= 2
    ]
    clusters.sort(key=lambda c: -c["wallet_count"])
    clustered_wallets = sum(c["wallet_count"] for c in clusters)
    if clustered_wallets == 0:
        risk = "LOW"
    elif clustered_wallets >= max(3, len(holders) // 3):
        risk = "HIGH"
    else:
        risk = "MEDIUM"
    return {
        "available": True,
        "holders_checked": len(holders),
        "exchange_funded": cex_count + infra_count,
        "no_source": no_source,
        "private_sources": len(sources),
        "clusters": clusters,
        "clustered_wallets": clustered_wallets,
        "coordination_risk": risk,
    }


@router.get("/eth/{address}")
async def funding_eth(address: str):
    address = address.lower().strip()
    if not (address.startswith("0x") and len(address) == 42):
        raise HTTPException(status_code=400, detail="invalid eth address")
    now = time.time()
    hit = _cache.get(address)
    if hit and now - hit[0] < CACHE_TTL:
        return {"success": True, "cached": True, "data": hit[1]}
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _analyze_sync, address)
    if data.get("available"):
        _cache[address] = (now, data)
    return {"success": True, "cached": False, "data": data}
