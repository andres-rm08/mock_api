from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import json
import uuid

app = FastAPI(title="Mock OPERA API")

DB_FILE = "db.json"

try:
    with open(DB_FILE, "r") as f:
        bookings = json.load(f)
except FileNotFoundError:
    bookings = []
    with open(DB_FILE, "w") as f:
        json.dump(bookings, f)

class Booking(BaseModel):
    guest_name: str
    room_type: str
    check_in: str
    check_out: str

def save_bookings():
    with open(DB_FILE, "w") as f:
        json.dump(bookings, f, indent=2)

def append_validation(request, response):
    """Append request/response to validation-output.json"""
    try:
        with open("validation-output.json", "r") as f:
            val = json.load(f)
    except FileNotFoundError:
        val = []

    val.append({"request": request, "response": response})
    with open("validation-output.json", "w") as f:
        json.dump(val, f, indent=2)

@app.get("/availability")
def get_availability():
    response = {"rooms_available": {"single": 5, "double": 3, "suite": 2}}
    append_validation({"endpoint": "/availability"}, response)
    return response

@app.get("/bookings", response_model=List[dict])
def get_bookings():
    return bookings

@app.post("/bookings")
def create_booking(booking: Booking):
    booking_id = str(uuid.uuid4())
    b = booking.dict()
    b["id"] = booking_id
    bookings.append(b)
    save_bookings()
    append_validation({"endpoint": "/bookings", "body": booking.dict()}, b)
    with open("webhook_received.json", "w") as f:
        json.dump({"event": "booking_created", "booking": b}, f, indent=2)
    return b

@app.put("/bookings/{booking_id}")
def update_booking(booking_id: str, booking: Booking):
    for b in bookings:
        if b["id"] == booking_id:
            b.update(booking.dict())
            save_bookings()
            append_validation({"endpoint": f"/bookings/{booking_id}", "body": booking.dict()}, b)
            return b
    raise HTTPException(status_code=404, detail="Booking not found")

@app.delete("/bookings/{booking_id}")
def delete_booking(booking_id: str):
    for i, b in enumerate(bookings):
        if b["id"] == booking_id:
            removed = bookings.pop(i)
            save_bookings()
            append_validation({"endpoint": f"/bookings/{booking_id}"}, removed)
            return {"status": "deleted"}
    raise HTTPException(status_code=404, detail="Booking not found")
