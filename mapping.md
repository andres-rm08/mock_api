-> Mock API to OPERA Mapping

| Mock API Endpoint      | OPERA API Endpoint                 | Notes                         |
|------------------------|------------------------------------|-------------------------------|
| GET /availability      | GET /api/v1/availability           | Returns room availability     |
| POST /bookings         | POST /api/v1/reservations          | Create reservation            |
| PUT /bookings/{id}     | PUT /api/v1/reservations/{id}      | Update reservation            |
| DELETE /bookings/{id}  | DELETE /api/v1/reservations/{id}   | Cancel reservation            |
| Webhook events         | Real OPERA webhooks                | Booking created/modified      |
