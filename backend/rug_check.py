from __future__ import annotations
import os, time
from datetime import datetime, timezone
from typing import Any
import httpx
from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/api/crypto", tags=["rug-check"])
GT="https://api.geckoterminal.com/api/v2"; GP="https://api.gopluslabs.io/api/v1"
CHAINS={"solana":"solana","eth":"eth","ethereum":"eth","bsc":"bsc","base":"base",
        "polygon":"polygon_pos","arbitrum":"arbitrum","avalanche":"avax","zksync":"zksync"}
GP_ID={"eth":"1","ethereum":"1","bsc":"56","base":"8453","polygon":"137",
       "arbitrum":"42161","avalanche":"43114","zksync":"324"}
ANON,AUTH=30,100; TTL_NEW,TTL_OLD=300,3600
W={"contract":25,"liquidity":25,"holders":25,"activity":15,"creator":10}
_rate={}; _mongo=None

def _db(request):
    global _mongo
    db=getattr(request.app.state,"db",None)
    if db is not None: return db
    if _mongo is None:
        from motor.motor_asyncio import AsyncIOMotorClient
        _mongo=AsyncIOMotorClient(os.getenv("MONGO_URL","mongodb://localhost:27017"))
    return _mongo[os.getenv("DB_NAME","pumpradar")]

def _ok(k,lim):
    now=time.time(); h=[t for t in _rate.get(k,[]) if now-t<3600]
    if len(h)>=lim: _rate[k]=h; return False
    h.append(now); _rate[k]=h; return True

def _f(v,d=0.0):
    try: return float(v)
    except (TypeError,ValueError): return d

async def _get(c,url,**kw):
    import asyncio
    for i in range(3):
        try:
            r=await c.get(url,timeout=12,**kw)
            if r.status_code==200: return r.json()
            if r.status_code in (429,502,503):
                await asyncio.sleep(1.5*(i+1)); continue
            return None
        except Exception:
            await asyncio.sleep(1.0*(i+1))
    return None

async def fetch_gp(c,addr,chain="solana"):
    if chain=="solana":
        d=await _get(c,f"{GP}/solana/token_security",params={"contract_addresses":addr})
    else:
        cid=GP_ID.get(chain)
        if not cid: return None
        d=await _get(c,f"{GP}/token_security/{cid}",params={"contract_addresses":addr.lower()})
    if not d or str(d.get("code"))!="1": return None
    res=d.get("result") or {}
    for k,v in res.items():
        if k.lower()==addr.lower(): return v
    return next(iter(res.values()),None)

async def fetch_token(c,net,addr):
    d=await _get(c,f"{GT}/networks/{net}/tokens/{addr}")
    return ((d or {}).get("data") or {}).get("attributes")

async def fetch_pool(c,net,addr):
    d=await _get(c,f"{GT}/networks/{net}/tokens/{addr}/pools",params={"page":1})
    pools=(d or {}).get("data") or []
    if not pools: return None
    return max(pools,key=lambda p:_f((p.get("attributes") or {}).get("reserve_in_usd"))).get("attributes")

def b_contract(gp,mature=False):
    if not gp: return None,[{"level":"unknown","text":"Contract properties could not be verified"}]
    s=100; f=[]
    mint=(gp.get("mintable") or {}).get("status"); frz=(gp.get("freezable") or {}).get("status")
    meta=(gp.get("metadata_mutable") or {}).get("status"); fee=gp.get("transfer_fee")
    hook=gp.get("transfer_hook") if isinstance(gp.get("transfer_hook"),dict) else {}
    if mint=="1": s-=45; f.append({"level":"red","text":"New tokens can still be minted"})
    elif mint=="0": f.append({"level":"green","text":"Minting permanently disabled"})
    elif gp.get("is_mintable") is None and mint is None and not mature:
        f.append({"level":"unknown","text":"Mint status could not be verified"})
    if frz=="1": s-=35; f.append({"level":"red","text":"Accounts can be frozen by the authority holder"})
    if meta=="1": s-=10; f.append({"level":"amber","text":"Name and symbol can be changed at any time"})
    if isinstance(fee,dict): fee=fee.get("fee_rate") or fee.get("status")
    if fee not in (None,"","0",0,{},"0%"): s-=15; f.append({"level":"amber","text":f"Transfer fee active: {fee}"})
    if hook.get("status")=="1": s-=20; f.append({"level":"red","text":"Transfer hook active — transfers can be blocked by external code"})
    if str(gp.get("non_transferable"))=="1": s-=60; f.append({"level":"red","text":"Token cannot be transferred"})
    if str(gp.get("is_honeypot"))=="1": s-=70; f.append({"level":"red","text":"Honeypot — selling is blocked"})
    elif gp.get("is_honeypot")=="0": f.append({"level":"green","text":"No honeypot detected"})
    for k,lbl in (("buy_tax","Buy tax"),("sell_tax","Sell tax")):
        t=_f(gp.get(k),-1)
        if t>0.10: s-=30; f.append({"level":"red","text":f"{lbl}: {t*100:.0f}%"})
        elif t>0.05: s-=15; f.append({"level":"amber","text":f"{lbl}: {t*100:.0f}%"})
    if str(gp.get("is_mintable"))=="1":
        if mature: f.append({"level":"amber","text":"Token supply can still be expanded (by design in some protocols)"})
        else: s-=40; f.append({"level":"red","text":"New tokens can still be minted"})
    if str(gp.get("can_take_back_ownership"))=="1": s-=25; f.append({"level":"red","text":"Ownership can be reclaimed"})
    if str(gp.get("is_open_source"))=="0": s-=30; f.append({"level":"red","text":"Contract source code is not public"})
    if s>=100 and not mature:
        s=75; f.append({"level":"unknown","text":"Standard launch contract — nothing to distinguish it"})
    return max(0,s),f

def b_liq(p,mature=False):
    if not p: return None,[{"level":"unknown","text":"No active liquidity pool found"}]
    s=100; f=[]; liq=_f(p.get("reserve_in_usd")); fdv=_f(p.get("fdv_usd"))
    if liq<5000: s-=50; f.append({"level":"red","text":f"Very low liquidity: ${liq:,.0f}"})
    elif liq<25000: s-=25; f.append({"level":"amber","text":f"Low liquidity: ${liq:,.0f}"})
    else: f.append({"level":"green","text":f"Liquidity: ${liq:,.0f}"})
    if fdv>0 and liq>0 and not mature:
        r=liq/fdv
        if r<0.02: s-=30; f.append({"level":"red","text":f"Liquidity covers only {r*100:.1f}% of market cap"})
        elif r<0.05: s-=15; f.append({"level":"amber","text":f"Liquidity covers {r*100:.1f}% of market cap"})
    return max(0,s),f

def b_hold(gp,mature=False):
    h=(gp or {}).get("holders")
    if not h or not isinstance(h,list): return None,[{"level":"unknown","text":"Holder distribution could not be verified"}]
    s=100; f=[]; pc=sorted((_f(x.get("percent")) for x in h),reverse=True)
    if pc and pc[0]<=1.0: pc=[x*100 for x in pc]
    t10=sum(pc[:10]); t1=pc[0] if pc else 0
    if mature:
        if t10>95: s-=20; f.append({"level":"amber","text":f"Top 10 holders control {t10:.0f}% (may include exchanges and contracts)"})
        else: f.append({"level":"green","text":f"Top 10 holders: {t10:.0f}%"})
    elif t10>80: s-=50; f.append({"level":"red","text":f"Top 10 holders control {t10:.0f}%"})
    elif t10>50: s-=25; f.append({"level":"amber","text":f"Top 10 holders control {t10:.0f}%"})
    elif t10<1: f.append({"level":"green","text":"Holdings are widely distributed"})
    else: f.append({"level":"green","text":f"Top 10 holders: {t10:.0f}%"})
    if t1>30 and not mature: s-=25; f.append({"level":"red","text":f"A single wallet holds {t1:.0f}%"})
    return max(0,s),f

def b_act(p,mature=False):
    if not p: return None,[{"level":"unknown","text":"Trading activity could not be verified"}]
    s=100; f=[]; tx=(p.get("transactions") or {}).get("h24") or {}
    b=int(tx.get("buys") or 0); sl=int(tx.get("sells") or 0); tot=b+sl
    vol=_f((p.get("volume_usd") or {}).get("h24")); liq=_f(p.get("reserve_in_usd")); age=None
    if p.get("pool_created_at"):
        try:
            dt=datetime.fromisoformat(str(p["pool_created_at"]).replace("Z","+00:00"))
            age=(datetime.now(timezone.utc)-dt).total_seconds()/3600
        except ValueError: pass
    if age is None: f.append({"level":"unknown","text":"Pool age unknown"})
    elif age<1: s-=35; f.append({"level":"red","text":f"Pool created {age*60:.0f} minutes ago"})
    elif age<24: s-=20; f.append({"level":"amber","text":f"Pool created {age:.0f} hours ago"})
    else:
        d=age/24
        if d<7 and not mature: s-=10; f.append({"level":"amber","text":f"Pool is only {d:.0f} day" + ("" if d<2 else "s") + " old"})
        else: f.append({"level":"green","text":f"Pool is {d:.0f} day" + ("" if d<2 else "s") + " old"})
    if tot==0: s-=70; f.append({"level":"red","text":"No transactions in the last 24h"})
    elif tot<20: s-=25; f.append({"level":"amber","text":f"Only {tot} transactions in 24h"})
    if tot>0 and b/max(1,tot)>0.9: s-=20; f.append({"level":"red","text":"Almost only buys — selling may be blocked"})
    if liq>0 and vol/liq>20: s-=15; f.append({"level":"amber","text":"Volume disproportionate to liquidity"})
    return max(0,s),f

def _verdict(sc,flags,nver):
    if nver<3: return "insufficient data"
    reds=sum(1 for f in flags if f["level"]=="red")
    if reds>=2: return "very high risk"
    if reds==1: return "high risk"
    if sum(1 for f in flags if f["level"]=="unknown")>=2: return "unverified"
    return "low risk" if sc>=80 else "medium risk" if sc>=60 else "high risk" if sc>=40 else "very high risk"

@router.get("/rug-check/{chain}/{address}")
async def rug_check(chain:str,address:str,request:Request):
    chain=chain.lower()
    if chain not in CHAINS: raise HTTPException(400,f"unsupported chain: {chain}")
    if address.startswith("0x"):
        if len(address)!=42: raise HTTPException(400,"invalid address")
    elif not (32<=len(address)<=44): raise HTTPException(400,"invalid address")
    authed=bool(request.headers.get("authorization"))
    ip=(request.headers.get("cf-connecting-ip") or request.headers.get("x-forwarded-for","").split(",")[0].strip()
        or (request.client.host if request.client else "?"))
    if not _ok(f"{ip}:{authed}",AUTH if authed else ANON):
        raise HTTPException(429,"Too many checks. Create an account for a higher limit.")
    db=_db(request); key=f"{chain}:{address}"
    cached=await db.rug_checks.find_one({"_id":key})
    if cached and cached.get("expires_at",0)>time.time(): return cached["payload"]
    async with httpx.AsyncClient(headers={"User-Agent":"PumpRadar/1.0"}) as c:
        gp=await fetch_gp(c,address,chain); pool=await fetch_pool(c,CHAINS[chain],address)
        tok=await fetch_token(c,CHAINS[chain],address)
    blocks={}; flags=[]; ver=[]
    mature=False
    if pool and pool.get("pool_created_at"):
        try:
            _dt=datetime.fromisoformat(str(pool["pool_created_at"]).replace("Z","+00:00"))
            mature=(datetime.now(timezone.utc)-_dt).total_seconds()>30*86400
        except ValueError: pass
    for n,(sc,fl) in {"contract":b_contract(gp,mature),"liquidity":b_liq(pool,mature),
                      "holders":b_hold(gp,mature),"activity":b_act(pool,mature)}.items():
        flags.extend(fl); blocks[n]=sc
        if sc is not None: ver.append(n)
    blocks["creator"]=None
    if not ver: raise HTTPException(404,"This address is not known on the selected chain. Check that the chain is correct.")
    score=round(sum(blocks[n]*W[n] for n in ver)/sum(W[n] for n in ver))
    order={"red":0,"amber":1,"unknown":2,"green":3}
    flags.sort(key=lambda x:order.get(x["level"],9))
    sym=(tok or {}).get("symbol")
    if not sym and gp and isinstance(gp.get("metadata"),dict): sym=gp["metadata"].get("symbol")
    _v=_verdict(score,flags,len(ver))
    payload={"chain":chain,"address":address,"symbol":sym,"score":(None if _v=="insufficient data" else score),"verdict":_v,
        "coverage":f"{len(ver)}/5",
        "coverage_note":"The score is based only on what could be verified. Anything unverified is marked unknown, never assumed safe.",
        "blocks":blocks,"flags":flags,"checked_at":datetime.now(timezone.utc).isoformat()}
    ttl=TTL_NEW
    if pool and pool.get("pool_created_at"):
        try:
            dt=datetime.fromisoformat(str(pool["pool_created_at"]).replace("Z","+00:00"))
            if (datetime.now(timezone.utc)-dt).total_seconds()>86400: ttl=TTL_OLD
        except ValueError: pass
    await db.rug_checks.update_one({"_id":key},{"$set":{"payload":payload,"expires_at":time.time()+ttl}},upsert=True)
    return payload
