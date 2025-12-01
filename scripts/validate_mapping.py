import httpx
import json

BASE_URL = "http://127.0.0.1:8000"

def run_flow():
    val = []

    r = httpx.get(f"{BASE_URL}/availability")
    val.append({"request": "/availability", "response": r.json()})

    booking = {"guest_name": "Bob", "room_type": "single", "check_in": "2025-12-05", "check_out": "2025-12-07"}
    r = httpx.post(f"{BASE_URL}/bookings", json=booking)
    val.append({"request": {"endpoint": "/bookings", "body": booking}, "response": r.json()})
    booking_id = r.json()["id"]

    updated_booking = {"guest_name": "Bob", "room_type": "suite", "check_in": "2025-12-05", "check_out": "2025-12-07"}
    r = httpx.put(f"{BASE_URL}/bookings/{booking_id}", json=updated_booking)
    val.append({"request": {"endpoint": f"/bookings/{booking_id}", "body": updated_booking}, "response": r.json()})

    r = httpx.delete(f"{BASE_URL}/bookings/{booking_id}")
    val.append({"request": f"/bookings/{booking_id}", "response": r.json()})

    with open("validation-output.json", "w") as f:
        json.dump(val, f, indent=2)

if __name__ == "__main__":
    run_flow()
    print("Validation complete. See validation-output.json")
