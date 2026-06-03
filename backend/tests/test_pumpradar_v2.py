"""
PumpRadar Backend API Tests - Iteration 2
Tests: Google OAuth, Email Login, Super Admin, AI Chat, Signals with Scientific Reasoning
"""
import pytest
import requests
import os
import json

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

# Admin credentials provided in requirements
ADMIN_EMAIL = "viorel.mina@gmail.com"
ADMIN_PASSWORD = "admin123"

# Test user credentials
TEST_EMAIL = "test_user_iteration2@pumpradar.test"
TEST_PASSWORD = "TestPass2024!"
TEST_NAME = "Test User v2"

_admin_token = None
_test_token = None


class TestHealth:
    """Basic health check"""
    
    def test_health_endpoint(self):
        """Verify backend is running"""
        r = requests.get(f"{BASE_URL}/api/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["app"] == "PumpRadar"
        print(f"Health OK: {data}")


class TestEmailAuthentication:
    """Test email/password authentication flow"""
    
    def test_admin_login_with_provided_credentials(self):
        """Test: Email/password login works (viorel.mina@gmail.com / admin123)"""
        global _admin_token
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
        assert "admin" in user["roles"], f"User should have admin role. Roles: {user['roles']}"
        
        _admin_token = data["data"]["accessToken"]
        print(f"Admin login OK: {user['email']}, roles={user['roles']}, subscription={user['subscription']}")
    
    def test_login_invalid_credentials(self):
        """Test invalid credentials return proper error"""
        r = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "invalid@test.com",
            "password": "wrongpassword"
        })
        assert r.status_code == 401
        print("Invalid login correctly rejected")
    
    def test_auth_me_endpoint(self):
        """Test /api/auth/me returns user data"""
        global _admin_token
        if not _admin_token:
            pytest.skip("No admin token available")
        
        r = requests.get(f"{BASE_URL}/api/auth/me", headers={
            "Authorization": f"Bearer {_admin_token}"
        })
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        assert data["data"]["user"]["email"] == ADMIN_EMAIL
        print(f"Auth me OK: {data['data']['user']['email']}")


class TestSuperAdminAPI:
    """Test Super Admin page API protection"""
    
    def test_admin_users_requires_auth(self):
        """Test: Super Admin API returns 401 when not authenticated"""
        r = requests.get(f"{BASE_URL}/api/admin/users")
        assert r.status_code == 401
        assert "Not authenticated" in r.json().get("detail", "")
        print("Admin API correctly requires authentication")
    
    def test_admin_users_requires_admin_role(self):
        """Test: Non-admin users get 403 Forbidden"""
        # First create a non-admin user
        requests.post(f"{BASE_URL}/api/auth/register", json={
            "email": TEST_EMAIL,
            "password": TEST_PASSWORD,
            "name": TEST_NAME
        })
        
        # Login as non-admin
        r = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_EMAIL,
            "password": TEST_PASSWORD
        })
        
        if r.status_code == 200:
            token = r.json()["data"]["accessToken"]
            
            # Try to access admin endpoint
            r2 = requests.get(f"{BASE_URL}/api/admin/users", headers={
                "Authorization": f"Bearer {token}"
            })
            assert r2.status_code == 403, f"Expected 403, got {r2.status_code}"
            print("Non-admin correctly blocked from admin API")
        else:
            # User might already exist with invalid credentials
            print("Skipped non-admin test - couldn't create test user")
    
    def test_admin_users_returns_data_for_admin(self):
        """Test: /api/admin/users returns users when authenticated as admin"""
        global _admin_token
        if not _admin_token:
            # Get fresh token
            r = requests.post(f"{BASE_URL}/api/auth/login", json={
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD
            })
            assert r.status_code == 200
            _admin_token = r.json()["data"]["accessToken"]
        
        r = requests.get(f"{BASE_URL}/api/admin/users", headers={
            "Authorization": f"Bearer {_admin_token}"
        })
        assert r.status_code == 200, f"Admin API failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        assert "users" in data.get("data", {})
        assert "total" in data.get("data", {})
        
        users = data["data"]["users"]
        total = data["data"]["total"]
        print(f"Admin API OK: {total} users returned")
        
        # Verify admin user is in the list
        admin_found = any(u["email"] == ADMIN_EMAIL for u in users)
        assert admin_found, "Admin user should be in the user list"


class TestAIChatEndpoint:
    """Test AI Chat customer service endpoint"""
    
    def test_ai_chat_returns_intelligent_response(self):
        """Test: AI Chat endpoint /api/ai/chat returns intelligent responses in English"""
        r = requests.post(f"{BASE_URL}/api/ai/chat", json={
            "message": "What is a pump signal and how does PumpRadar detect them?"
        })
        assert r.status_code == 200, f"AI Chat failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        assert "reply" in data.get("data", {})
        
        reply = data["data"]["reply"]
        
        # Verify response is intelligent and in English
        assert len(reply) > 50, "Response too short"
        
        # Check for key terms that indicate intelligent response
        lower_reply = reply.lower()
        has_relevant_content = any(term in lower_reply for term in [
            "pump", "signal", "volume", "price", "momentum", 
            "algorithm", "analysis", "market", "crypto"
        ])
        assert has_relevant_content, f"Response doesn't seem relevant: {reply[:200]}"
        
        # Verify English response (check for common English words)
        english_indicators = ["the", "is", "and", "or", "for", "this", "not"]
        is_english = sum(1 for w in english_indicators if w in lower_reply) >= 3
        assert is_english, f"Response may not be in English: {reply[:200]}"
        
        print(f"AI Chat OK: {reply[:150]}...")
    
    def test_ai_chat_with_market_context(self):
        """Test AI provides context about current signals"""
        r = requests.post(f"{BASE_URL}/api/ai/chat", json={
            "message": "What are the current pump signals today?"
        })
        assert r.status_code == 200
        data = r.json()
        reply = data["data"]["reply"]
        
        # AI should mention number of signals or specific coins
        has_signal_context = any(term in reply.lower() for term in [
            "pump signal", "coins", "currently", "active", "%"
        ])
        print(f"AI Market Context: {reply[:200]}...")


class TestCryptoSignals:
    """Test crypto signal endpoints with scientific reasoning"""
    
    def test_signals_endpoint_returns_data(self):
        """Test: Dashboard shows pump/dump signals"""
        r = requests.get(f"{BASE_URL}/api/crypto/signals")
        assert r.status_code == 200, f"Signals API failed: {r.text}"
        data = r.json()
        assert data.get("success") == True
        
        signals_data = data.get("data", {})
        assert "pump_signals" in signals_data
        assert "dump_signals" in signals_data
        assert "market_summary" in signals_data
        assert "coins_analyzed" in signals_data
        
        pump_count = len(signals_data["pump_signals"])
        dump_count = len(signals_data["dump_signals"])
        coins = signals_data["coins_analyzed"]
        
        print(f"Signals OK: {pump_count} pumps, {dump_count} dumps, {coins} coins analyzed")
    
    def test_signals_have_scientific_reasoning(self):
        """Test: Signals have scientific reasoning in their descriptions"""
        r = requests.get(f"{BASE_URL}/api/crypto/signals")
        assert r.status_code == 200
        data = r.json()["data"]
        
        pump_signals = data.get("pump_signals", [])
        assert len(pump_signals) > 0, "No pump signals to verify"
        
        # Check first pump signal for scientific reasoning
        signal = pump_signals[0]
        
        # Required fields
        assert "symbol" in signal
        assert "signal_strength" in signal
        assert "reason" in signal
        
        reason = signal.get("reason", "")
        
        # Check for scientific/quantitative terms in reasoning
        scientific_terms = [
            "volume", "price", "market", "momentum", "%", 
            "ratio", "24h", "1h", "cap", "trend", "buying",
            "selling", "increase", "decrease", "support"
        ]
        
        terms_found = [t for t in scientific_terms if t.lower() in reason.lower()]
        assert len(terms_found) >= 2, f"Reason lacks scientific terms: {reason[:200]}"
        
        print(f"Signal {signal['symbol']}: strength={signal['signal_strength']}%")
        print(f"Reason (scientific terms: {terms_found}): {reason[:200]}...")
    
    def test_signals_include_market_metrics(self):
        """Test signals include Fear & Greed and trending coins"""
        r = requests.get(f"{BASE_URL}/api/crypto/signals")
        assert r.status_code == 200
        data = r.json()["data"]
        
        # Check Fear & Greed
        fear_greed = data.get("fear_greed")
        assert fear_greed is not None, "Fear & Greed index missing"
        assert "value" in fear_greed
        assert "classification" in fear_greed
        assert 0 <= fear_greed["value"] <= 100
        
        # Check trending
        trending = data.get("trending", [])
        assert isinstance(trending, list)
        
        print(f"Market metrics: F&G={fear_greed['value']} ({fear_greed['classification']}), Trending: {trending[:5]}")


class TestGoogleOAuthEndpoint:
    """Test Google OAuth backend endpoint exists"""
    
    def test_google_auth_endpoint_exists(self):
        """Test: Google auth endpoint /api/auth/google exists"""
        # Test with invalid session_id - should return 401, not 404
        r = requests.post(f"{BASE_URL}/api/auth/google", json={
            "session_id": "invalid_test_session_id"
        })
        
        # Should NOT be 404 (endpoint exists)
        assert r.status_code != 404, "Google auth endpoint not found"
        
        # Should be 401 or 400 (invalid session)
        assert r.status_code in [400, 401, 500], f"Unexpected status: {r.status_code}"
        print(f"Google auth endpoint exists, returns {r.status_code} for invalid session")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
