"""PumpRadar backend API tests"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

TEST_EMAIL = "test_reg_pumpradar@pumpradar.com"
TEST_PASSWORD = "TestPass123!"
TEST_NAME = "Test User"

# Auth token shared across tests
_token = None

class TestHealth:
    def test_health(self):
        r = requests.get(f"{BASE_URL}/api/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        print("Health OK")

class TestAuth:
    def test_register(self):
        r = requests.post(f"{BASE_URL}/api/auth/register", json={
            "email": TEST_EMAIL, "password": TEST_PASSWORD, "name": TEST_NAME
        })
        # 200 success or 400 already exists
        assert r.status_code in [200, 201, 400]
        print(f"Register status: {r.status_code}, body: {r.text[:200]}")

    def test_login_valid(self):
        global _token
        # First ensure user exists
        requests.post(f"{BASE_URL}/api/auth/register", json={
            "email": TEST_EMAIL, "password": TEST_PASSWORD, "name": TEST_NAME
        })
        r = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": TEST_EMAIL, "password": TEST_PASSWORD
        })
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        assert "accessToken" in data.get("data", {})
        _token = data["data"]["accessToken"]
        print(f"Login OK, token: {_token[:20]}...")

    def test_login_invalid(self):
        r = requests.post(f"{BASE_URL}/api/auth/login", json={
            "email": "nonexistent@x.com", "password": "wrong"
        })
        assert r.status_code in [400, 401, 404]
        print(f"Invalid login status: {r.status_code}")

    def test_get_me(self):
        global _token
        if not _token:
            pytest.skip("No token")
        r = requests.get(f"{BASE_URL}/api/auth/me", headers={"Authorization": f"Bearer {_token}"})
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        print(f"Me: {data['data'].get('email')}")

class TestSignals:
    def test_signals_unauthenticated(self):
        r = requests.get(f"{BASE_URL}/api/crypto/signals")
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        assert "pump_signals" in data.get("data", {})
        assert "dump_signals" in data.get("data", {})
        print(f"Signals OK: pump={len(data['data']['pump_signals'])}, dump={len(data['data']['dump_signals'])}")

    def test_signals_authenticated(self):
        global _token
        if not _token:
            # try login
            requests.post(f"{BASE_URL}/api/auth/register", json={
                "email": TEST_EMAIL, "password": TEST_PASSWORD, "name": TEST_NAME
            })
            r = requests.post(f"{BASE_URL}/api/auth/login", json={
                "email": TEST_EMAIL, "password": TEST_PASSWORD
            })
            if r.status_code == 200:
                _token = r.json()["data"]["accessToken"]
        headers = {"Authorization": f"Bearer {_token}"} if _token else {}
        r = requests.get(f"{BASE_URL}/api/crypto/signals", headers=headers)
        assert r.status_code == 200
        data = r.json()
        assert data.get("success") == True
        assert "has_full_access" in data.get("data", {})
        print(f"Auth signals OK, has_full_access={data['data']['has_full_access']}")
