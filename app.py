from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Dict, Any, Optional
import json
import uuid
from datetime import datetime, date, timezone
import os
import tempfile
import threading

app = FastAPI(title="Mock OPERA API (reservations)")

DB_FILE = "db.json"
PROFILES_FILE = "profiles.json"
VALIDATION_FILE = "validation-output.json"
WEBHOOK_FILE = "webhook_received.json"

_log_lock = threading.Lock()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def append_json_file(path: str, entry: Dict[str, Any]) -> None:
    """
    Append an object to a JSON array file. Thread-safe within process via _log_lock.
    If file doesn't exist or is invalid, create/reset it.
    """
    with _log_lock:
        try:
            if not os.path.exists(path):
                with open(path, "w", encoding="utf-8") as f:
                    json.dump([entry], f, ensure_ascii=False, indent=2)
                return

            with open(path, "r+", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    if not isinstance(data, list):
                        data = [data]
                except json.JSONDecodeError:
                    data = []
                data.append(entry)
                f.seek(0)
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.truncate()
        except Exception:

            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump([entry], f, ensure_ascii=False, indent=2)
            except Exception:
                pass

def append_validation(request: Dict[str, Any], response: Dict[str, Any], success: bool = True, endpoint: Optional[str] = None, status_code: Optional[int] = None, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Standardized audit log entry appended to validation-output.json
    """
    entry = {
        "timestamp": now_iso(),
        "endpoint": endpoint if endpoint else request.get("endpoint"),
        "status_code": status_code if status_code is not None else (200 if success else 400),
        "outcome": "success" if success else "failure",
        "request": request,
        "response": response,
    }
    if extra:
        entry["extra"] = extra
    append_json_file(VALIDATION_FILE, entry)

def write_webhook(event_type: str, payload: Dict[str, Any]) -> None:
    entry = {
        "timestamp": now_iso(),
        "event": event_type,
        "payload": payload,
    }
    append_json_file(WEBHOOK_FILE, entry)

def rotate_validation_log(max_bytes: int = 2_000_000, backup_suffix: str = ".old") -> None:
    """
    Optional utility: rotate validation log if it grows beyond max_bytes.
    Not invoked by the app by default — intended for CI/test wrappers.
    """
    if not os.path.exists(VALIDATION_FILE):
        return
    size = os.path.getsize(VALIDATION_FILE)
    if size <= max_bytes:
        return
    backup = VALIDATION_FILE + backup_suffix
    if os.path.exists(backup):
        os.remove(backup)
    os.replace(VALIDATION_FILE, backup)
    with open(VALIDATION_FILE, "w", encoding="utf-8") as f:
        json.dump([], f)

try:
    with open(DB_FILE, "r", encoding="utf-8") as f:
        bookings = json.load(f)
        if not isinstance(bookings, list):
            bookings = []
except FileNotFoundError:
    bookings = []
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(bookings, f, indent=2)

try:
    with open(PROFILES_FILE, "r", encoding="utf-8") as f:
        profiles = json.load(f)
        if not isinstance(profiles, list):
            profiles = []
except FileNotFoundError:
    profiles = []
    with open(PROFILES_FILE, "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2)

def save_bookings():
    """
    Atomic write to DB_FILE to avoid partial/corrupt writes.
    """
    dirpath = os.path.dirname(os.path.abspath(DB_FILE)) or "."
    fd, tmp_path = tempfile.mkstemp(dir=dirpath)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(bookings, f, indent=2)
        os.replace(tmp_path, DB_FILE)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise

def save_profiles():
    """
    Atomic write to PROFILES_FILE to avoid partial/corrupt writes.
    """
    dirpath = os.path.dirname(os.path.abspath(PROFILES_FILE)) or "."
    fd, tmp_path = tempfile.mkstemp(dir=dirpath)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(profiles, f, indent=2)
        os.replace(tmp_path, PROFILES_FILE)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise

def normalize_bookings_on_load():
    """If older bookings lack timestamps, add created_at/updated_at for realism (only on load)."""
    changed = False
    for b in bookings:
        if "created_at" not in b:
            b["created_at"] = now_iso()
            changed = True
        if "updated_at" not in b:
            b["updated_at"] = b["created_at"]
            changed = True
    if changed:
        save_bookings()

normalize_bookings_on_load()

class Address(BaseModel):
    street_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None

class LoyaltyInfo(BaseModel):
    program_name: Optional[str] = None
    membership_number: Optional[str] = None
    tier_level: Optional[str] = None
    points_balance: Optional[int] = None

class Booking(BaseModel):
    """Legacy model kept for alias endpoints if needed."""
    guest_name: str
    room_type: str
    check_in: str
    check_out: str

ROOM_INVENTORY = {
    "single": 5,
    "double": 3,
    "suite": 2
}

class Reservation(BaseModel):
    reservation_id: Optional[str] = None
    profile_id: Optional[str] = None
    property_id: str = Field(..., description="Property code / hotel id")
    guest_name: Optional[str] = None

    room_type: str
    rate_plan_code: Optional[str] = None
    source_code: Optional[str] = None
    market_code: Optional[str] = None

    arrival_date: str
    departure_date: str

    guaranteed: bool = False
    guarantee_type: Optional[str] = None
    currency: Optional[str] = "USD"
    total_amount: Optional[float] = None
    guest_count: int = 1

    status: Optional[str] = "reserved"  

    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @field_validator("room_type")
    @classmethod
    def validate_room_type(cls, v):
        if v not in ROOM_INVENTORY:
            raise ValueError(f"invalid room_type '{v}'; must be one of {list(ROOM_INVENTORY.keys())}")
        return v

    @model_validator(mode='after')
    def departure_after_arrival(self):
        a = self.arrival_date
        d = self.departure_date
        if a and d:
            try:
                a_dt = datetime.fromisoformat(a).replace(hour=0, minute=0, second=0, microsecond=0)
                d_dt = datetime.fromisoformat(d).replace(hour=0, minute=0, second=0, microsecond=0)
                if d_dt <= a_dt:
                    raise ValueError("departure_date must be after arrival_date")
            except Exception:
                
                raise
        return self

class Profile(BaseModel):
    profile_id: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    name: Optional[str] = None
    emails: List[str] = Field(default_factory=list)
    phones: List[str] = Field(default_factory=list)
    address: Optional[Address] = None
    loyalty_info: Optional[LoyaltyInfo] = None
    date_of_birth: Optional[str] = None
    preferred_language: Optional[str] = None
    preferences: Dict[str, Any] = Field(default_factory=dict)
    vip_status: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @model_validator(mode='after')
    def ensure_display_name(self):
        if not self.name:
            parts = [self.first_name, self.last_name]
            combined = " ".join([p for p in parts if p])
            if combined:
                self.name = combined
        return self

def parse_date(date_str: str) -> datetime:
    """
    Accepts ISO date strings (YYYY-MM-DD or full ISO). Returns datetime at midnight (naive).
    """
    try:
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format, use YYYY-MM-DD or ISO format")

@app.get("/availability")
def get_availability(check_in: Optional[str] = Query(None), check_out: Optional[str] = Query(None), include_tentatives: Optional[bool] = Query(False)):
    today_str = date.today().isoformat()
    try:
        check_in_date = parse_date(check_in) if check_in else parse_date(today_str)
        check_out_date = parse_date(check_out) if check_out else parse_date(today_str)
    except HTTPException:
        raise

    availability = {}
    for room_type, total in ROOM_INVENTORY.items():
        booked_count = 0
        for b in bookings:
            if b["room_type"] != room_type:
                continue
            status = b.get("status", "booked")
            if status not in ["booked", "checked_in", "reserved", "guaranteed"]:
                continue
            if not include_tentatives and status == "reserved" and not b.get("guaranteed", False):
                continue

            b_check_in = parse_date(b["check_in"])
            b_check_out = parse_date(b["check_out"])
            if not (check_out_date <= b_check_in or check_in_date >= b_check_out):
                booked_count += 1

        availability[room_type] = max(0, total - booked_count)

    response = {"rooms_available": availability}
    append_validation({"endpoint": "/availability", "check_in": check_in, "check_out": check_out, "include_tentatives": include_tentatives}, response, success=True, endpoint="/availability", status_code=200)
    return response

def check_room_availability(room_type: str, check_in: str, check_out: str, exclude_id: Optional[str] = None, include_tentatives: bool = False) -> bool:
    """
    Returns True if room available for the requested date range (exclusive of exclude_id).
    include_tentatives: if False, only guaranteed/booked/checked_in/guaranteed reservations consume inventory.
    """
    check_in_date = parse_date(check_in)
    check_out_date = parse_date(check_out)
    booked_count = 0
    for b in bookings:
        if b["room_type"] != room_type:
            continue
        if exclude_id and (b.get("id") == exclude_id or b.get("reservation_id") == exclude_id):
            continue
        status = b.get("status", "booked")

        if status not in ["booked", "checked_in", "reserved", "guaranteed"]:
            continue

        if not include_tentatives and status == "reserved" and not b.get("guaranteed", False):
            continue
        b_check_in = parse_date(b["check_in"])
        b_check_out = parse_date(b["check_out"])
        if not (check_out_date <= b_check_in or check_in_date >= b_check_out):
            booked_count += 1
    return booked_count < ROOM_INVENTORY.get(room_type, 0)

def find_reservation_by_id(res_id: str) -> Optional[Dict[str, Any]]:
    return next((b for b in bookings if b.get("id") == res_id or b.get("reservation_id") == res_id), None)

def find_profile_by_id(profile_id: str) -> Optional[Dict[str, Any]]:
    return next((p for p in profiles if p.get("profile_id") == profile_id), None)

def get_profile_reservations(profile_id: str) -> List[Dict[str, Any]]:
    return [b for b in bookings if b.get("profile_id") == profile_id]

def serialize_profile(profile: Dict[str, Any], include_history: bool = False) -> Dict[str, Any]:
    whitelist = {
        "profile_id",
        "first_name",
        "last_name",
        "name",
        "emails",
        "phones",
        "address",
        "loyalty_info",
        "preferred_language",
        "preferences",
        "vip_status",
        "created_at",
        "updated_at",
    }
    serialized = {k: v for k, v in profile.items() if k in whitelist}
    if include_history:
        history = []
        for r in get_profile_reservations(profile.get("profile_id")):
            history.append({
                "reservation_id": r.get("reservation_id"),
                "property_id": r.get("property_id"),
                "room_type": r.get("room_type"),
                "check_in": r.get("check_in"),
                "check_out": r.get("check_out"),
                "status": r.get("status"),
                "rate_plan_code": r.get("rate_plan_code"),
                "source_code": r.get("source_code"),
                "market_code": r.get("market_code"),
                "guaranteed": r.get("guaranteed"),
                "guarantee_type": r.get("guarantee_type"),
                "total_amount": r.get("total_amount"),
                "currency": r.get("currency"),
                "guest_count": r.get("guest_count"),
                "created_at": r.get("created_at"),
                "updated_at": r.get("updated_at"),
            })
        serialized["reservation_history"] = history
    return serialized

def create_reservation_logic(reservation: Reservation) -> Dict[str, Any]:
    arrival = reservation.arrival_date
    departure = reservation.departure_date
    room_type = reservation.room_type

    if not check_room_availability(room_type, arrival, departure):
        detail = f"No {room_type} rooms available for {arrival} to {departure}"
        append_validation({"endpoint": "/reservations", "body": reservation.model_dump()}, {"error": detail}, success=False, endpoint="/reservations", status_code=409)
        write_webhook("reservation.create_failed", {"reason": detail, "request": reservation.model_dump()})
        raise HTTPException(status_code=409, detail=detail)

    rid = str(uuid.uuid4())
    r = reservation.model_dump()
    r["reservation_id"] = rid
    r["status"] = r.get("status", "reserved")
    r["created_at"] = now_iso()
    r["updated_at"] = r["created_at"]

    mapped = {
        "id": rid,
        "reservation_id": rid,
        "profile_id": r.get("profile_id"),
        "property_id": r["property_id"],
        "guest_name": r.get("guest_name"),
        "room_type": r["room_type"],
        "check_in": r["arrival_date"],
        "check_out": r["departure_date"],
        "status": r["status"],
        "rate_plan_code": r.get("rate_plan_code"),
        "source_code": r.get("source_code"),
        "market_code": r.get("market_code"),
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
        "guaranteed": r.get("guaranteed", False),
        "guarantee_type": r.get("guarantee_type"),
        "total_amount": r.get("total_amount"),
        "currency": r.get("currency"),
        "guest_count": r.get("guest_count", 1),
    }
    bookings.append(mapped)
    save_bookings()

    append_validation({"endpoint": "/reservations", "body": reservation.model_dump()}, mapped, success=True, endpoint="/reservations", status_code=201)
    write_webhook("reservation.created", {"reservation_id": rid, "property_id": mapped.get("property_id"), "payload": mapped})
    return mapped

def update_reservation_logic(res_id: str, reservation: Reservation) -> Dict[str, Any]:
    for b in bookings:
        if b.get("id") == res_id or b.get("reservation_id") == res_id:
            if not check_room_availability(reservation.room_type, reservation.arrival_date, reservation.departure_date, exclude_id=res_id):
                detail = f"No {reservation.room_type} rooms available for {reservation.arrival_date} to {reservation.departure_date}"
                append_validation({"endpoint": f"/reservations/{res_id}", "body": reservation.model_dump()}, {"error": detail}, success=False, endpoint=f"/reservations/{res_id}", status_code=409)
                write_webhook("reservation.update_failed", {"id": res_id, "reason": detail, "request": reservation.model_dump()})
                raise HTTPException(status_code=409, detail=detail)

            old_created = b.get("created_at")

            b.update({
                "guest_name": reservation.guest_name,
                "room_type": reservation.room_type,
                "check_in": reservation.arrival_date,
                "check_out": reservation.departure_date,
                "rate_plan_code": reservation.rate_plan_code,
                "source_code": reservation.source_code,
                "market_code": reservation.market_code,
                "guaranteed": reservation.guaranteed,
                "guarantee_type": reservation.guarantee_type,
                "total_amount": reservation.total_amount,
                "currency": reservation.currency,
                "guest_count": reservation.guest_count,
                "status": reservation.status or b.get("status")
            })
            if old_created:
                b["created_at"] = old_created
            b["updated_at"] = now_iso()
            save_bookings()
            append_validation({"endpoint": f"/reservations/{res_id}", "body": reservation.model_dump()}, b, success=True, endpoint=f"/reservations/{res_id}", status_code=200)
            write_webhook("reservation.updated", {"reservation_id": res_id, "property_id": b.get("property_id"), "payload": b})
            return b
    append_validation({"endpoint": f"/reservations/{res_id}"}, {"error": "Reservation not found"}, success=False, endpoint=f"/reservations/{res_id}", status_code=404)
    write_webhook("reservation.update_failed", {"id": res_id, "reason": "not found"})
    raise HTTPException(status_code=404, detail="Reservation not found")

def delete_reservation_logic(res_id: str) -> Dict[str, Any]:
    for i, b in enumerate(bookings):
        if b.get("id") == res_id or b.get("reservation_id") == res_id:
            removed = bookings.pop(i)
            save_bookings()
            deleted_payload = {"reservation_id": res_id, "deleted_at": now_iso(), "original": removed}
            append_validation({"endpoint": f"/reservations/{res_id}"}, deleted_payload, success=True, endpoint=f"/reservations/{res_id}", status_code=200)
            write_webhook("reservation.deleted", {"reservation_id": res_id, "property_id": removed.get("property_id"), "payload": deleted_payload})
            return {"status": "deleted", "reservation_id": res_id}
    append_validation({"endpoint": f"/reservations/{res_id}"}, {"error": "Reservation not found"}, success=False, endpoint=f"/reservations/{res_id}", status_code=404)
    write_webhook("reservation.delete_failed", {"reservation_id": res_id, "reason": "not found"})
    raise HTTPException(status_code=404, detail="Reservation not found")

def checkin_logic(res_id: str) -> Dict[str, Any]:
    b = find_reservation_by_id(res_id)
    if not b:
        append_validation({"endpoint": f"/checkin/{res_id}"}, {"error": "Reservation not found"}, success=False, endpoint=f"/checkin/{res_id}", status_code=404)
        write_webhook("reservation.checkin_failed", {"reservation_id": res_id, "reason": "not found"})
        raise HTTPException(status_code=404, detail="Reservation not found")
    if b.get("status", "booked") not in ["reserved", "booked", "guaranteed"]:
        append_validation({"endpoint": f"/checkin/{res_id}"}, {"error": "Reservation cannot be checked in"}, success=False, endpoint=f"/checkin/{res_id}", status_code=400)
        write_webhook("reservation.checkin_failed", {"reservation_id": res_id, "reason": "invalid status"})
        raise HTTPException(status_code=400, detail="Reservation cannot be checked in")
    b["status"] = "checked_in"
    b["updated_at"] = now_iso()
    save_bookings()
    append_validation({"endpoint": f"/checkin/{res_id}"}, b, success=True, endpoint=f"/checkin/{res_id}", status_code=200)
    write_webhook("reservation.checkin", {"reservation_id": res_id, "property_id": b.get("property_id"), "payload": {"status": b["status"], "updated_at": b["updated_at"]}})
    return {"message": f"Reservation {res_id} checked in", "reservation": b}

def checkout_logic(res_id: str) -> Dict[str, Any]:
    b = find_reservation_by_id(res_id)
    if not b:
        append_validation({"endpoint": f"/checkout/{res_id}"}, {"error": "Reservation not found"}, success=False, endpoint=f"/checkout/{res_id}", status_code=404)
        write_webhook("reservation.checkout_failed", {"reservation_id": res_id, "reason": "not found"})
        raise HTTPException(status_code=404, detail="Reservation not found")
    if b.get("status") != "checked_in":
        append_validation({"endpoint": f"/checkout/{res_id}"}, {"error": "Reservation cannot be checked out"}, success=False, endpoint=f"/checkout/{res_id}", status_code=400)
        write_webhook("reservation.checkout_failed", {"reservation_id": res_id, "reason": "invalid status"})
        raise HTTPException(status_code=400, detail="Reservation cannot be checked out")
    b["status"] = "checked_out"
    b["updated_at"] = now_iso()
    save_bookings()
    append_validation({"endpoint": f"/checkout/{res_id}"}, b, success=True, endpoint=f"/checkout/{res_id}", status_code=200)
    write_webhook("reservation.checkout", {"reservation_id": res_id, "property_id": b.get("property_id"), "payload": {"status": b["status"], "updated_at": b["updated_at"]}})
    return {"message": f"Reservation {res_id} checked out", "reservation": b}

@app.get("/reservations", response_model=List[dict])
def get_reservations():
    append_validation({"endpoint": "/reservations", "action": "list"}, {"count": len(bookings)}, success=True, endpoint="/reservations", status_code=200)
    return bookings

@app.post("/reservations", status_code=201)
def create_reservation(reservation: Reservation):
    return create_reservation_logic(reservation)

@app.put("/reservations/{reservation_id}")
def update_reservation(reservation_id: str, reservation: Reservation):
    return update_reservation_logic(reservation_id, reservation)

@app.delete("/reservations/{reservation_id}")
def delete_reservation(reservation_id: str):
    return delete_reservation_logic(reservation_id)

@app.post("/checkin/{reservation_id}")
def reservation_checkin(reservation_id: str):
    return checkin_logic(reservation_id)

@app.post("/checkout/{reservation_id}")
def reservation_checkout(reservation_id: str):
    return checkout_logic(reservation_id)

@app.get("/bookings", response_model=List[dict])
def get_bookings_alias():

    return get_reservations()

@app.post("/bookings", status_code=201)
def create_booking_alias(booking: Booking):

    res = Reservation(
        property_id="DEFAULT_PROPERTY",
        guest_name=booking.guest_name,
        room_type=booking.room_type,
        arrival_date=booking.check_in,
        departure_date=booking.check_out
    )
    return create_reservation_logic(res)

@app.put("/bookings/{booking_id}")
def update_booking_alias(booking_id: str, booking: Booking):
    res = Reservation(
        property_id="DEFAULT_PROPERTY",
        guest_name=booking.guest_name,
        room_type=booking.room_type,
        arrival_date=booking.check_in,
        departure_date=booking.check_out
    )
    return update_reservation_logic(booking_id, res)

@app.delete("/bookings/{booking_id}")
def delete_booking_alias(booking_id: str):
    return delete_reservation_logic(booking_id)

@app.post("/bookings/checkin/{booking_id}")
def booking_checkin_alias(booking_id: str):
    return checkin_logic(booking_id)

@app.post("/bookings/checkout/{booking_id}")
def booking_checkout_alias(booking_id: str):
    return checkout_logic(booking_id)

@app.get("/profiles", response_model=List[dict])
def list_profiles():
    serialized = [serialize_profile(p) for p in profiles]
    append_validation({"endpoint": "/profiles", "action": "list"}, {"count": len(serialized)}, success=True, endpoint="/profiles", status_code=200)
    return serialized

@app.post("/profiles", status_code=201)
def create_profile(profile: Profile):
    pid = profile.profile_id or str(uuid.uuid4())
    if profile.profile_id and find_profile_by_id(profile.profile_id):
        append_validation({"endpoint": "/profiles", "body": profile.model_dump()}, {"error": "Profile already exists"}, success=False, endpoint="/profiles", status_code=409)
        raise HTTPException(status_code=409, detail="Profile already exists")

    data = profile.model_dump()
    data["profile_id"] = pid
    now_ts = now_iso()
    data["created_at"] = data.get("created_at") or now_ts
    data["updated_at"] = now_ts

    profiles.append(data)
    save_profiles()

    response_payload = serialize_profile(data)
    append_validation({"endpoint": "/profiles", "body": profile.model_dump()}, response_payload, success=True, endpoint="/profiles", status_code=201)
    write_webhook("profile.created", {"profile_id": pid, "payload": response_payload})
    return response_payload

@app.get("/profiles/{profile_id}")
def get_profile(profile_id: str):
    p = find_profile_by_id(profile_id)
    if not p:
        append_validation({"endpoint": f"/profiles/{profile_id}"}, {"error": "Profile not found"}, success=False, endpoint=f"/profiles/{profile_id}", status_code=404)
        raise HTTPException(status_code=404, detail="Profile not found")

    serialized = serialize_profile(p, include_history=True)
    append_validation({"endpoint": f"/profiles/{profile_id}"}, serialized, success=True, endpoint=f"/profiles/{profile_id}", status_code=200)
    return serialized

@app.put("/profiles/{profile_id}")
def update_profile(profile_id: str, profile: Profile):
    existing = find_profile_by_id(profile_id)
    if not existing:
        append_validation({"endpoint": f"/profiles/{profile_id}", "body": profile.model_dump()}, {"error": "Profile not found"}, success=False, endpoint=f"/profiles/{profile_id}", status_code=404)
        raise HTTPException(status_code=404, detail="Profile not found")

    updates = profile.model_dump(exclude={"profile_id", "created_at"}, exclude_unset=True)
    if profile.name:
        updates["name"] = profile.name
    existing.update({k: v for k, v in updates.items() if v is not None or k in ["preferences"]})
    existing["updated_at"] = now_iso()
    save_profiles()

    serialized = serialize_profile(existing)
    append_validation({"endpoint": f"/profiles/{profile_id}", "body": profile.model_dump()}, serialized, success=True, endpoint=f"/profiles/{profile_id}", status_code=200)
    write_webhook("profile.updated", {"profile_id": profile_id, "payload": serialized})
    return serialized

@app.delete("/profiles/{profile_id}")
def delete_profile(profile_id: str):
    for idx, p in enumerate(profiles):
        if p.get("profile_id") == profile_id:
            removed = profiles.pop(idx)
            save_profiles()
            deleted_payload = {"profile_id": profile_id, "deleted_at": now_iso(), "original": removed}
            append_validation({"endpoint": f"/profiles/{profile_id}"}, deleted_payload, success=True, endpoint=f"/profiles/{profile_id}", status_code=200)
            write_webhook("profile.deleted", {"profile_id": profile_id})
            return {"status": "deleted", "profile_id": profile_id}

    append_validation({"endpoint": f"/profiles/{profile_id}"}, {"error": "Profile not found"}, success=False, endpoint=f"/profiles/{profile_id}", status_code=404)
    raise HTTPException(status_code=404, detail="Profile not found")