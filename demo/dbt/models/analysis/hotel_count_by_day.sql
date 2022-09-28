SELECT
  BOOKING_DATE,
  HOTEL,
  COUNT(ID) as count_bookings
FROM {{ ref('unified_data') }}
GROUP BY
  BOOKING_DATE,
  HOTEL