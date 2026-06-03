"""
PumpRadar Backend API Tests - Iteration 4 (PRO Features)
Tests: Signal Accuracy Tracker, Market Open Email Scheduler Jobs
New features:
  1. /api/crypto/accuracy endpoint - returns accuracy stats for 1h/4h/24h timeframes
  2. Scheduler jobs: london_market_email, nyse_market_email, accuracy_tracker
"""
import pytest
import requests
import os
import json

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

# Admin credentials
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


class TestAccuracyEndpoint:
    """/api/crypto/accuracy endpoint tests - NEW PRO FEATURE"""
    
    def test_accuracy_endpoint_exists(self):
        """Test: /api/crypto/accuracy endpoint exists and returns 200"""
        r = requests.get(f"{BASE_URL}/api/crypto/accuracy")
        assert r.status_code == 200, f"Accuracy endpoint failed: {r.status_code} - {r.text}"
        print("[PASS] Accuracy endpoint returns 200")
    
    def test_accuracy_returns_correct_structure(self):
        """Test: Accuracy endpoint returns proper data structure with 1h/4h/24h stats"""
        r = requests.get(f"{BASE_URL}/api/crypto/accuracy")
        assert r.status_code == 200, f"Accuracy API failed: {r.text}"
        
        data = r.json()
        assert data.get("success") == True, f"API not successful: {data}"
        
        accuracy_data = data.get("data", {})
        
        # Verify required fields exist
        assert "accuracy_1h" in accuracy_data, "Missing accuracy_1h"
        assert "accuracy_4h" in accuracy_data, "Missing accuracy_4h"
        assert "accuracy_24h" in accuracy_data, "Missing accuracy_24h"
        assert "last_updated" in accuracy_data, "Missing last_updated"
        
        print(f"[PASS] Accuracy structure OK")
        print(f"  - accuracy_1h: {accuracy_data['accuracy_1h']}")
        print(f"  - accuracy_4h: {accuracy_data['accuracy_4h']}")
        print(f"  - accuracy_24h: {accuracy_data['accuracy_24h']}")
    
    def test_accuracy_timeframe_structure(self):
        """Test: Each timeframe has pump/dump/overall/samples fields"""
        r = requests.get(f"{BASE_URL}/api/crypto/accuracy")
        assert r.status_code == 200
        
        accuracy_data = r.json()["data"]
        
        for timeframe in ["accuracy_1h", "accuracy_4h", "accuracy_24h"]:
            tf_data = accuracy_data[timeframe]
            
            # Each timeframe should have these fields
            assert "pump" in tf_data, f"{timeframe} missing 'pump' field"
            assert "dump" in tf_data, f"{timeframe} missing 'dump' field"
            assert "overall" in tf_data, f"{timeframe} missing 'overall' field"
            assert "samples" in tf_data, f"{timeframe} missing 'samples' field"
            
            # Values should be numbers (0-100 range for percentages)
            assert isinstance(tf_data["pump"], (int, float)), f"{timeframe} pump not numeric"
            assert isinstance(tf_data["dump"], (int, float)), f"{timeframe} dump not numeric"
            assert isinstance(tf_data["overall"], (int, float)), f"{timeframe} overall not numeric"
            assert isinstance(tf_data["samples"], int), f"{timeframe} samples not integer"
            
            # Percentage values should be 0-100
            assert 0 <= tf_data["pump"] <= 100, f"{timeframe} pump out of range"
            assert 0 <= tf_data["dump"] <= 100, f"{timeframe} dump out of range"
            assert 0 <= tf_data["overall"] <= 100, f"{timeframe} overall out of range"
            
            print(f"[PASS] {timeframe} structure valid: pump={tf_data['pump']}%, dump={tf_data['dump']}%, samples={tf_data['samples']}")
    
    def test_accuracy_works_without_auth(self):
        """Test: Accuracy endpoint works for unauthenticated users (public)"""
        r = requests.get(f"{BASE_URL}/api/crypto/accuracy")
        assert r.status_code == 200, "Accuracy should be publicly accessible"
        print("[PASS] Accuracy endpoint works without authentication")
    
    def test_accuracy_works_with_auth(self):
        """Test: Accuracy endpoint works with authenticated user"""
        token = get_admin_token()
        assert token, "Failed to get admin token"
        
        r = requests.get(f"{BASE_URL}/api/crypto/accuracy", headers={
            "Authorization": f"Bearer {token}"
        })
        assert r.status_code == 200, f"Accuracy with auth failed: {r.text}"
        print("[PASS] Accuracy endpoint works with authentication")


class TestSchedulerJobsConfiguration:
    """Test scheduler jobs are properly configured (via startup event)
    NOTE: We cannot directly test scheduler jobs, but we can verify the 
    functions they call exist and work correctly.
    """
    
    def test_signals_endpoint_works(self):
        """Verify signals endpoint works (called by crypto_signals job)"""
        r = requests.get(f"{BASE_URL}/api/crypto/signals")
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        print("[PASS] Signals endpoint working (crypto_signals job target)")
    
    def test_accuracy_endpoint_works(self):
        """Verify accuracy endpoint works (uses data from accuracy_tracker job)"""
        r = requests.get(f"{BASE_URL}/api/crypto/accuracy")
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        print("[PASS] Accuracy endpoint working (accuracy_tracker job target)")
    
    def test_snapshot_data_has_required_fields_for_emails(self):
        """Verify snapshot data has fields needed for market open emails"""
        r = requests.get(f"{BASE_URL}/api/crypto/snapshots?limit=1")
        assert r.status_code == 200
        data = r.json()["data"]
        
        snapshots = data.get("snapshots", [])
        if snapshots:
            snap = snapshots[0]
            # Market open emails need these fields
            assert "pump_signals" in snap, "Missing pump_signals for email"
            assert "dump_signals" in snap, "Missing dump_signals for email"
            assert "fear_greed" in snap, "Missing fear_greed for email"
            assert "market_summary" in snap, "Missing market_summary for email"
            
            print("[PASS] Snapshot has all fields needed for market open emails")
            print(f"  - {len(snap.get('pump_signals', []))} pump signals")
            print(f"  - {len(snap.get('dump_signals', []))} dump signals")
            print(f"  - Fear & Greed: {snap.get('fear_greed', {})}")
        else:
            print("[PASS] No snapshots yet - market emails won't send without data (expected)")


class TestPreviousFeaturesStillWork:
    """Regression tests: Verify all previous features still work"""
    
    def test_auth_login(self):
        """Test: Login still works"""
        r = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        assert "accessToken" in data["data"]
        print("[PASS] Auth login working")
    
    def test_signals_endpoint(self):
        """Test: Signals endpoint still works"""
        r = requests.get(f"{BASE_URL}/api/crypto/signals")
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        assert "pump_signals" in data["data"]
        assert "dump_signals" in data["data"]
        print(f"[PASS] Signals endpoint working: {len(data['data']['pump_signals'])} pumps, {len(data['data']['dump_signals'])} dumps")
    
    def test_history_endpoint(self):
        """Test: History endpoint still works"""
        token = get_admin_token()
        r = requests.get(f"{BASE_URL}/api/crypto/history?limit=5", headers={
            "Authorization": f"Bearer {token}"
        })
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        assert "history" in data["data"]
        print(f"[PASS] History endpoint working: {len(data['data']['history'])} entries")
    
    def test_watchlist_endpoint(self):
        """Test: Watchlist endpoint still works"""
        token = get_admin_token()
        r = requests.get(f"{BASE_URL}/api/user/watchlist", headers={
            "Authorization": f"Bearer {token}"
        })
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        assert "watchlist" in data["data"]
        print(f"[PASS] Watchlist endpoint working: {len(data['data']['watchlist'])} items")
    
    def test_ai_chat_endpoint(self):
        """Test: AI Chat endpoint still works"""
        r = requests.post(f"{BASE_URL}/api/ai/chat", json={
            "message": "hello"
        })
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        assert "reply" in data["data"]
        print(f"[PASS] AI Chat endpoint working")


class TestProSubscriptionRequirements:
    """Test that PRO features have proper access controls"""
    
    def test_alert_settings_require_pro(self):
        """Test: Updating alert settings requires Pro subscription"""
        token = get_admin_token()
        
        # First check current subscription
        r = requests.get(f"{BASE_URL}/api/user/subscription", headers={
            "Authorization": f"Bearer {token}"
        })
        assert r.status_code == 200
        sub_data = r.json()["data"]
        print(f"  - Current subscription: {sub_data.get('subscription')}")
        
        # Try to enable alerts
        r = requests.post(f"{BASE_URL}/api/user/alerts", 
            json={"email_alerts_enabled": True, "global_alerts_enabled": True},
            headers={"Authorization": f"Bearer {token}"}
        )
        
        # Admin has annual subscription so this should work
        if sub_data.get("subscription") in ["monthly", "annual"]:
            assert r.status_code == 200, f"Pro user should be able to update alerts"
            print("[PASS] Pro user can update alert settings")
        else:
            assert r.status_code == 402, "Non-Pro user should get 402"
            print("[PASS] Alert settings correctly require Pro subscription")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
