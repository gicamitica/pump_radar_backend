# PumpRadar Handoff 24 Mai 2026

## Stack
- Backend: FastAPI port 8020, service pumpradar-advanced
- MongoDB: container arbitrajz-mongo, DB pumpradar
- AI Judge: Claude Haiku 4.5 (principal) + Qwen local port 8088 (fallback)
- Caddy Docker: /srv/data/docker/caddy/Caddyfile
- Telegram: 19 canale indexate
- Frontend: /srv/data/pump_radar_repo/frontend/dist
- Pi dev: 192.168.50.14, Cloud prod: 91.99.137.60

## Comenzi utile
- Trigger scan: curl -s -X POST http://localhost:8020/api/admin/trigger-scan
- Verificare AI: docker exec -it arbitrajz-mongo mongo pumpradar --eval 'var d = db.qwen_decision_cache.findOne({}, {}, {sort: {updated_at: -1}}); print("Updated:", d.updated_at); d.items.forEach(function(i){print(i.symbol, "|", i.ai_verdict_code, "|", i.final_verdict)})'
- Restart: sudo systemctl restart pumpradar-advanced
- Reload Caddy: docker exec caddy caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
- Deploy frontend: scp -r /home/gicamitica/pump_radar/frontend/dist/* gicamitica@91.99.137.60:/srv/data/pump_radar_repo/frontend/dist/

## De facut
1. Tab DEX → pagina separata /dex cu disclaimer
2. Scos Apify din cod complet
3. Early detection imbunatatit
4. Upgrade cloud CCX23 → Gemma 4 dupa upgrade
5. Reddit RSS filtre mai bune + mai multe subreddits

## Facut azi
- Claude Haiku 4.5 integrat ca AI judge principal
- 3 verdict-uri finale: PUMP IMMINENT / WATCH THIS / DUMP IMMINENT
- Badge-uri vizuale pe carduri cu pulse animation
- Fix pagina coin 500 error
- Fix Caddy /api/user/* routes
- Cache CoinGecko 45min + whale detection 30min
- Reddit RSS 5 subreddits integrat
- Ora locala pe snapshot
- Auto-refresh 2 minute
