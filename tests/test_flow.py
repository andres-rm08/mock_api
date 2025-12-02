import pytest
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)

def test_booking_lifecycle():
    # Check availability
    r = client.get("/availability")
    assert r.status_code == 201

    # Create booking
    booking_data = {
        "guest_name": "Alice",
        "room_type": "single",
        "check_in": "2025-12-01",
        "check_out": "2025-12-03"
    }
    r = client.post("/bookings", json=booking_data)
    assert r.status_code == 200
    booking_id = r.json()["id"]

    # Update booking
    updated_data = {
        "guest_name": "Alice",
        "room_type": "double",
        "check_in": "2025-12-01",
        "check_out": "2025-12-03"
    }
    r = client.put(f"/bookings/{booking_id}", json=updated_data)
    assert r.status_code == 200

    # Check-in booking
    r = client.post(f"/checkin/{booking_id}")
    assert r.status_code == 200
    assert r.json()["booking"]["status"] == "checked_in"

    # Check-out booking
    r = client.post(f"/checkout/{booking_id}")
    assert r.status_code == 200
    assert r.json()["booking"]["status"] == "checked_out"

    # Delete booking
    r = client.delete(f"/bookings/{booking_id}")
    assert r.status_code == 200
