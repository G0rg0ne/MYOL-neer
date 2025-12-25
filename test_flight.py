from fast_flights import create_filter, get_flights_from_filter, FlightData, Passengers

filter = create_filter(
    flight_data=[
        FlightData(
            date="2026-01-15",  # Future date
            from_airport="CDG",  # Paris Charles de Gaulle
            to_airport="AMS",    # Amsterdam Schiphol
        )
    ],
    trip="one-way",
    passengers=Passengers(adults=1, children=0, infants_in_seat=0, infants_on_lap=0),
    seat="economy",
    max_stops=0,
)
print(filter.as_b64().decode("utf-8"))
#print(get_flights_from_filter(filter, mode="common"))
print(get_flights_from_filter(filter, mode="local"))