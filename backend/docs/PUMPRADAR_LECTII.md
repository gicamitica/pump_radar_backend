# PumpRadar — Pilde, reguli si TODO

## Regula de aur
Nu atinge algoritmul de clasificare. SYSTEM_PROMPT, praguri, categorii, logica de decizie raman neatinse.
Orice optimizare (cache, fallback) se face in jurul apelului AI, nu in logica lui.

## Pilde (lectii din sesiunile de debugging)
1. Estimari de cost pe date reale, nu din burta. (Gresit candva: "$5 = 4 ani". Real: ~5 zile fara cache, ~10-12 cu cache.)
2. Editare cod: NU base64 (se strica din escape-uri / caractere chirilice), NU regex orb. DA: heredoc single-quote (cat > x << 'EOF'), injectare prin slice pe linii cu assert pe start/sfarsit.
3. Verifica inainte de restart cu .venv/bin/python (NU python3 de sistem): ast.parse + import. httpx/motor exista doar in venv.
4. Backup inainte de ORICE: cp fis fis.bak-<scop>-$(date +%Y%m%d_%H%M).
5. Cand reconstruiesti un obiect (semnal), pastreaza TOATE campurile. Cache-ul a scurtat semnalul, au lipsit 12 campuri (red_flags, whale_score, dump_risk_level...), frontend-ul a crapat pe .includes/.length pe undefined.
6. Deploy frontend: scp -r dist/* (fara -r, folderul assets/ NU se copiaza). Hard-refresh dupa.
7. Backend se editeaza direct pe cloud (/srv/data/pump_radar/backend/). Frontend: sursa pe Pi, build pe Pi, scp -r pe cloud.
8. Scan automat: asyncio.create_task(delayed_fetch()) la startup. Daca e comentat, nu ruleaza la pornire. Confirma cu "Scan DONE" in log, nu cu UI.
9. Diagnostic onest: confirma din log/DB/curl, nu din UI. Eroare fara mesaj = exceptie fara str() util (timeout sau import lipsa).
10. Fallback: Haiku = principal (calitate). Qwen local (1.5B, port 8088) = plasa de avarie, prea slab pt semnale bune. Deterministic = ultim resort.

## Ce e implementat
- Cache Haiku: colectie haiku_signal_cache, cheie symbol, doar verdict AI. TTL early 60min, rest 180min.
- Fallback Qwen: in judge.py (scan) si server.py (ai-market-analysis).
- coin-live: endpoint /api/crypto/coin-live/{network}/{address} cu cache 10s per token.
- Email dedup "pana dispare": colectie sent_signal_alerts. Email o data per semnal cat e activ; se sterge cand semnalul dispare (poate redeclansa la revenire). Filtru: early(pre_pump) / pump(conf>=75) / dump-risk(conf>=70).
- Cronometru dashboard: inel dublu (verde=varsta urca, violet=next scan scade), flash la scanare. Derivat din nextRefresh (3600 - nextRefresh).

## [DONE 8 iun] Auto-refresh + scan vs live pe pagina coin - IMPLEMENTAT
Backend: GATA pe cloud (coin-live cu cache 10s, verificat). Frontend: DE FACUT pe Pi (build + scp -r).

Cerinta (8 iunie): pe pagina coin, fiecare valoare (pret/volum/lichiditate) sa arate DOUA valori + variatia:
  - "at scan (acum Xm)" = valoarea inghetata din scan (din /api/crypto/signals-v2)
  - "now (live)" = valoarea curenta (din /api/crypto/coin-live, refresh la 10s)
  - variatia in % intre ele (verde/rosu)
  - text "acum X timp de la scan"
Motiv: userul care intra mai tarziu vede cat era la finalizarea scanului SI cat e acum, deci cat s-a miscat. Intr-o ora valoarea se schimba mult.

Pasi pe Pi:
1. Backup CoinDetailPage.tsx (cp ... .bak-scanvslive-$(date +%Y%m%d_%H%M))
2. State separat: pastreaza valoarea de la scan (scanSnapshot) cand vine din signals-v2, si valoarea live separat. NU suprascrie scan-ul cu live-ul.
3. useEffect cu setInterval(10000) care cheama coin-live si actualizeaza doar live-ul.
4. UI: pentru pret/volum/lichiditate, afiseaza "at scan -> now (live)" + variatie %.
5. "acum X timp de la scan" derivat din last_updated (now - last_updated).
6. Build pe Pi + scp -r dist/* pe cloud + hard-refresh.

## Checklist inainte de orice interventie
1. Backup cu timestamp.
2. Cod nou cu heredoc single-quote.
3. Verifica bucata izolat (ast.parse pe /tmp).
4. Injecteaza prin slice pe linii cu assert.
5. Verifica tot fisierul: ast.parse + import cu .venv/bin/python.
6. Daca reconstruiesti un obiect: compara camp cu camp cu backup.
7. Restart, apoi confirma din log/DB/curl, nu din UI.
8. Frontend: build pe Pi, scp -r pe cloud, hard-refresh.
9. Algoritmul: NEATINS.
