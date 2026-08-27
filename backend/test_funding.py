#!/usr/bin/env python3
"""
test_funding.py - SCRIPT IZOLAT DE TEST pentru gas tracing (ETH). v2
"""
import os, sys, time, requests
from collections import defaultdict

def get_key():
    try:
        with open(".env") as f:
            for line in f:
                if line.startswith("ETHERSCAN_API_KEY="):
                    return line.strip().split("=", 1)[1]
    except FileNotFoundError:
        pass
    return os.environ.get("ETHERSCAN_API_KEY", "")

API_KEY = get_key()
BASE = "https://api.etherscan.io/v2/api"

KNOWN_CEX = {
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "Binance",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "Binance",
    "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": "Binance",
    "0x9696f59e4d72e237be84ffd425dcad154bf96976": "Binance",
    "0xf977814e90da44bfa03b6295a0616a897441acec": "Binance",
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "Binance",
    "0x030e37ddd7df1b43db172b23916d523f1599c6cc": "Binance",
    "0x00799bbc833d5b168f0410312d2a8fd9e0e3079c": "Binance (deposit funder)",
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "Coinbase",
    "0x503828976d22510aad0201ac7ec88293211d23da": "Coinbase",
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "Coinbase",
    "0x3cd751e6b0078be393132286c442345e5dc49699": "Coinbase",
    "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0": "Kraken",
    "0xa910f92acdaf488fa6ef02174fb86208ad7722ba": "Kraken",
    "0xe93381fb4c4f14bda253907b18fad305d799241a": "Huobi",
    "0x46340b20830761efd32832a74d7169b29feb9758": "Crypto.com",
}

INFRA_TX_THRESHOLD = 500

def api_get(params, retries=2):
    for _ in range(retries + 1):
        try:
            r = requests.get(BASE, params=params, timeout=15)
            return r.json()
        except Exception as e:
            print(f"  ! api err: {e}")
            time.sleep(0.5)
    return {}

def funding_source(wallet):
    wallet = wallet.lower()
    d = api_get({
        "chainid":1,"module":"account","action":"txlist","address":wallet,
        "startblock":0,"endblock":99999999,"page":1,"offset":20,"sort":"asc","apikey":API_KEY,
    })
    if d.get("status") == "1":
        for tx in d.get("result", []):
            if tx.get("to","").lower()==wallet and int(tx.get("value",0))>0:
                return tx["from"].lower(), int(tx["value"])/1e18
    return None, 0

_infra_cache = {}
def is_infrastructure(addr):
    if addr in _infra_cache:
        return _infra_cache[addr]
    d = api_get({
        "chainid":1,"module":"account","action":"txlist","address":addr,
        "startblock":0,"endblock":99999999,"page":1,"offset":INFRA_TX_THRESHOLD+1,"sort":"asc","apikey":API_KEY,
    })
    n = len(d.get("result", [])) if d.get("status")=="1" else 0
    infra = n > INFRA_TX_THRESHOLD
    _infra_cache[addr] = infra
    time.sleep(0.25)
    return infra

def get_token_holders(token, limit=15):
    try:
        r = requests.get(f"https://pump.arbitrajz.com/api/crypto/osint/eth/{token}", timeout=30)
        d = r.json().get("data", {})
        th = d.get("holders", {}).get("top_holders", [])
        addrs = [h.get("address","").lower() for h in th if h.get("address")]
        addrs = [a for a in addrs if a and not a.startswith("0x00000000000000")]
        return addrs[:limit]
    except Exception as e:
        print(f"  ! eroare la OSINT holders: {e}")
        return []

def analyze(wallets):
    sources = defaultdict(list)
    cex_count = infra_count = no_source = 0
    for i, w in enumerate(wallets):
        src, eth = funding_source(w)
        time.sleep(0.25)
        if src is None:
            no_source += 1
            print(f"  {i+1}. {w[:12]} -> (nicio sursa)")
        elif src in KNOWN_CEX:
            cex_count += 1
            print(f"  {i+1}. {w[:12]} -> CEX: {KNOWN_CEX[src]}")
        elif is_infrastructure(src):
            infra_count += 1
            print(f"  {i+1}. {w[:12]} -> infra/exchange (auto-detected)")
        else:
            sources[src].append(w)
            print(f"  {i+1}. {w[:12]} -> {src[:12]} ({eth:.3f} ETH) [private]")
    print("\n=== REZULTAT ===")
    print(f"Total: {len(wallets)} | CEX: {cex_count} | infra auto: {infra_count} | fara sursa: {no_source}")
    print(f"Surse private distincte: {len(sources)}")
    clusters = {s:ws for s,ws in sources.items() if len(ws)>=2}
    if clusters:
        print("\n!!! CLUSTERE PRIVATE (posibila coordonare):")
        for s,ws in sorted(clusters.items(), key=lambda x:-len(x[1])):
            print(f"   {s[:16]} -> {len(ws)} wallet-uri: {[w[:8] for w in ws]}")
        total_clustered = sum(len(ws) for ws in clusters.values())
        print(f"\n   {total_clustered}/{len(wallets)} wallet-uri sunt in clustere private.")
    else:
        print("\nNiciun cluster privat. Holderi par independenti (sanatos).")

if __name__ == "__main__":
    if not API_KEY:
        print("EROARE: nu gasesc ETHERSCAN_API_KEY"); sys.exit(1)
    if len(sys.argv) > 1 and sys.argv[1].startswith("0x"):
        token = sys.argv[1].lower()
        print(f"Iau holderi pentru token {token[:12]}...")
        holders = get_token_holders(token, limit=15)
        if not holders:
            print("Nu am putut lua holderi (endpoint poate cere plan platit pe Etherscan).")
            sys.exit(1)
        print(f"Am {len(holders)} holderi. Analizez funding...\n")
        analyze(holders)
    else:
        print("Test demo (foloseste: python3 test_funding.py 0xTOKEN pentru token real)\n")
        analyze([
            "0xf977814e90da44bfa03b6295a0616a897441acec",
            "0x28c6c06298d514db089934071355e5743bf21d60",
            "0x5a52e96bacdabb82fd05763e25335261b270efcb",
        ])
