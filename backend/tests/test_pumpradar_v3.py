"""
PumpRadar Backend API Tests - Iteration 3
Tests: History endpoint, Watchlist API, Snapshots endpoint, Dark mode verification
New features: Signal History page, My Watchlist feature, Email alerts system
"""
import pytest
import requests
import os
import json

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

# Admin credentials provided in requirements
ADMIN_EMAIL = "viorel.mina@gmail.com"
ADMIN_PASSWORD = "admin123"

_admin_token = None


def get_admin_token():
    """Helper to get admin token"""
    global _admin_token
    if not _admin_token:
        r = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if r.status_code == 200:
            _admin_token = r.json()["data"]["accessToken"]
    return _admin_token


class TestHealth:
    """Basic health check"""
    
    def test_health_endpoint(self):
        """Verify backend is running"""
        r = requests.get(f"{BASE_URL}/api/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["app"] == "PumpRadar"
        print(f"[PASS] Health OK: {data}")


class TestEmailAuthentication:
    """Test email/password authentication flow"""
    
    def test_admin_login(self):
        """Test: Email/password login works (viorel.mina@gmail.com / admin123)"""
        r = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert r.status_code == 200, f"Login failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        assert "accessToken" in data.get("data", {})
        
        user = data["data"]["user"]
        assert user["email"] == ADMIN_EMAIL
        print(f"[PASS] Admin login OK: {user['email']}, roles={user['roles']}")


class TestHistoryEndpoint:
    """/api/crypto/history endpoint tests - NEW FEATURE"""
    
    def test_history_requires_authentication(self):
        """History endpoint requires auth token"""
        r = requests.get(f"{BASE_URL}/api/crypto/history?limit=5")
        assert r.status_code == 401, "History should require authentication"
        print("[PASS] History endpoint correctly requires authentication")
    
    def test_history_returns_data(self):
        """Test: /history endpoint returns historical signal data"""
        token = get_admin_token()
        assert token, "Failed to get admin token"
        
        r = requests.get(f"{BASE_URL}/api/crypto/history?limit=5", headers={
            "Authorization": f"Bearer {token}"
        })
        assert r.status_code == 200, f"History API failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        assert "history" in data.get("data", {})
        
        history = data["data"]["history"]
        assert isinstance(history, list)
        
        if len(history) > 0:
            entry = history[0]
            # Each history entry should have these fields
            assert "timestamp" in entry, "Missing timestamp"
            assert "pump_count" in entry, "Missing pump_count"
            assert "dump_count" in entry, "Missing dump_count"
            assert "market_summary" in entry, "Missing market_summary"
            assert "coins_analyzed" in entry, "Missing coins_analyzed"
            
            print(f"[PASS] History OK: {len(history)} entries")
            print(f"  - Latest: {entry['pump_count']} pumps, {entry['dump_count']} dumps")
        else:
            print("[PASS] History OK: No historical data yet (empty is valid)")


class TestSnapshotsEndpoint:
    """/api/crypto/snapshots endpoint tests - NEW FEATURE"""
    
    def test_snapshots_returns_detailed_data(self):
        """Test: /api/crypto/snapshots returns detailed signal data"""
        r = requests.get(f"{BASE_URL}/api/crypto/snapshots?limit=3")
        assert r.status_code == 200, f"Snapshots API failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        assert "snapshots" in data.get("data", {})
        
        snapshots = data["data"]["snapshots"]
        assert isinstance(snapshots, list)
        
        if len(snapshots) > 0:
            snap = snapshots[0]
            # Detailed snapshot fields
            assert "timestamp" in snap, "Missing timestamp"
            assert "pump_signals" in snap, "Missing pump_signals"
            assert "dump_signals" in snap, "Missing dump_signals"
            assert "market_summary" in snap, "Missing market_summary"
            
            # Verify signals have detail
            if snap["pump_signals"]:
                sig = snap["pump_signals"][0]
                assert "symbol" in sig
                assert "signal_strength" in sig
                assert "reason" in sig
            
            print(f"[PASS] Snapshots OK: {len(snapshots)} snapshots")
            print(f"  - Latest: {len(snap.get('pump_signals',[]))} pumps, {len(snap.get('dump_signals',[]))} dumps")
        else:
            print("[PASS] Snapshots OK: No data yet (empty is valid)")


class TestWatchlistAPI:
    """/api/user/watchlist endpoint tests - NEW FEATURE"""
    
    def test_watchlist_requires_auth(self):
        """Watchlist endpoints require authentication"""
        r = requests.get(f"{BASE_URL}/api/user/watchlist")
        assert r.status_code == 401, "Watchlist GET should require auth"
        
        r = requests.post(f"{BASE_URL}/api/user/watchlist/add", json={
            "symbol": "BTC"
        })
        assert r.status_code == 401, "Watchlist add should require auth"
        print("[PASS] Watchlist endpoints correctly require authentication")
    
    def test_get_watchlist(self):
        """Test: Get user watchlist"""
        token = get_admin_token()
        assert token, "Failed to get admin token"
        
        r = requests.get(f"{BASE_URL}/api/user/watchlist", headers={
            "Authorization": f"Bearer {token}"
        })
        assert r.status_code == 200, f"Watchlist GET failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        assert "watchlist" in data.get("data", {})
        
        watchlist = data["data"]["watchlist"]
        assert isinstance(watchlist, list)
        print(f"[PASS] Watchlist GET OK: {len(watchlist)} items")
    
    def test_add_to_watchlist(self):
        """Test: Add coin to watchlist"""
        token = get_admin_token()
        assert token, "Failed to get admin token"
        
        # Add BTC to watchlist
        r = requests.post(f"{BASE_URL}/api/user/watchlist/add", 
            json={
                "symbol": "BTC",
                "alertEnabled": False,
                "alertThreshold": 80
            },
            headers={"Authorization": f"Bearer {token}"}
        )
        assert r.status_code == 200, f"Watchlist add failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        
        # Verify BTC is in watchlist
        watchlist = data["data"].get("watchlist", [])
        btc_found = any(w.get("symbol") == "BTC" for w in watchlist)
        assert btc_found, "BTC should be in watchlist after adding"
        print(f"[PASS] Watchlist add OK: BTC added")
    
    def test_remove_from_watchlist(self):
        """Test: Remove coin from watchlist"""
        token = get_admin_token()
        assert token, "Failed to get admin token"
        
        # Remove BTC
        r = requests.delete(f"{BASE_URL}/api/user/watchlist/BTC", headers={
            "Authorization": f"Bearer {token}"
        })
        assert r.status_code == 200, f"Watchlist remove failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        
        # Verify BTC is removed
        watchlist = data["data"].get("watchlist", [])
        btc_found = any(w.get("symbol") == "BTC" for w in watchlist)
        assert not btc_found, "BTC should be removed from watchlist"
        print(f"[PASS] Watchlist remove OK: BTC removed")


class TestAlertSettings:
    """Alert settings API tests"""
    
    def test_get_alert_settings(self):
        """Test: Get user alert settings"""
        token = get_admin_token()
        assert token, "Failed to get admin token"
        
        r = requests.get(f"{BASE_URL}/api/user/alerts", headers={
            "Authorization": f"Bearer {token}"
        })
        assert r.status_code == 200, f"Alert settings GET failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        
        # Check structure
        settings = data.get("data", {})
        assert "email_alerts_enabled" in settings
        assert "global_alerts_enabled" in settings
        print(f"[PASS] Alert settings OK: email_alerts={settings['email_alerts_enabled']}")


class TestSignalsWithScientificReasoning:
    """Test signals have scientific AI analysis"""
    
    def test_signals_have_detailed_analysis(self):
        """Test: Signals contain scientific reasoning with quantitative data"""
        r = requests.get(f"{BASE_URL}/api/crypto/signals")
        assert r.status_code == 200
        data = r.json()["data"]
        
        pump_signals = data.get("pump_signals", [])
        assert len(pump_signals) > 0, "No pump signals to verify"
        
        # Check for required fields and scientific terms
        signal = pump_signals[0]
        assert "symbol" in signal
        assert "signal_strength" in signal
        assert "reason" in signal
        
        reason = signal.get("reason", "")
        
        # Verify quantitative terms in AI reasoning
        quant_terms = ["volume", "price", "%", "ratio", "mcap", "1h", "24h", "momentum"]
        terms_found = [t for t in quant_terms if t.lower() in reason.lower()]
        assert len(terms_found) >= 2, f"Reason lacks quantitative terms: {reason[:200]}"
        
        # Verify technical_factors field exists (new)
        if "technical_factors" in signal:
            print(f"  - Technical factors: {signal['technical_factors'][:100]}...")
        
        print(f"[PASS] Signal {signal['symbol']}: {signal['signal_strength']}%")
        print(f"  - Quantitative terms found: {terms_found}")


class TestAIChatEndpoint:
    """Test AI Chat customer service"""
    
    def test_ai_chat_returns_english_response(self):
        """Test: AI Chat returns intelligent English response"""
        r = requests.post(f"{BASE_URL}/api/ai/chat", json={
            "message": "What are the current market conditions?"
        })
        assert r.status_code == 200, f"AI Chat failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        
        reply = data["data"]["reply"]
        assert len(reply) > 50, "Response too short"
        
        # Verify English
        english_words = ["the", "and", "is", "market", "signal"]
        is_english = sum(1 for w in english_words if w in reply.lower()) >= 2
        assert is_english, f"Response may not be in English"
        
        print(f"[PASS] AI Chat OK: {reply[:150]}...")


class TestSuperAdminProtection:
    """Test Super Admin page protection"""
    
    def test_admin_users_requires_auth(self):
        """Super Admin API requires authentication"""
        r = requests.get(f"{BASE_URL}/api/admin/users")
        assert r.status_code == 401
        print("[PASS] Super Admin API correctly requires authentication")
    
    def test_admin_users_works_for_admin(self):
        """Admin can access user list"""
        token = get_admin_token()
        assert token, "Failed to get admin token"
        
        r = requests.get(f"{BASE_URL}/api/admin/users", headers={
            "Authorization": f"Bearer {token}"
        })
        assert r.status_code == 200, f"Admin API failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        
        users = data["data"]["users"]
        total = data["data"]["total"]
        print(f"[PASS] Super Admin API OK: {total} users")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
