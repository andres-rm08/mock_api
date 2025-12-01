import pytest
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)

def test_booking_lifecycle():
    r = client.get("/availability")
    assert r.status_code == 200

    booking_data = {"guest_name": "Alice", "room_type": "single", "check_in": "2025-12-01", "check_out": "2025-12-03"}
    r = client.post("/bookings", json=booking_data)
    assert r.status_code == 200
    booking_id = r.json()["id"]

    updated_data = {"guest_name": "Alice", "room_type": "double", "check_in": "2025-12-01", "check_out": "2025-12-03"}
    r = client.put(f"/bookings/{booking_id}", json=updated_data)
    assert r.status_code == 200

    r = client.delete(f"/bookings/{booking_id}")
    assert r.status_code == 200
