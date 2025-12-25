# MYOL-neer

Flight ticket price dataset builder using Google Flights via the fast-flights library.

## Features

- Fetches real-time flight prices from Google Flights
- No API keys required - uses web scraping
- Supports multiple routes and date ranges
- Exports data to CSV with standardized schema
- Configurable seat class filters (economy, premium-economy, business, first)
- Rate limiting and error handling with retry logic

## Important: Regional Restrictions

**Google Flights displays a cookie consent dialog for EU users**, which can block automated scraping. If you encounter errors mentioning "Before you continue to Google" or cookie consent, you have these options:

1. **Run from a non-EU server** (e.g., AWS US-East, DigitalOcean NYC)
2. **Use a VPN/proxy** from a non-EU country
3. **Use a residential proxy service** like BrightData (supported in fast-flights v3)

## Dataset Schema

| Field | Description |
|-------|-------------|
| `origin` | IATA airport code of departure |
| `destination` | IATA airport code of arrival |
| `departure_date` | Flight departure date (YYYY-MM-DD) |
| `query_date` | Date when price was queried (YYYY-MM-DD) |
| `days_before_departure` | Days between query and departure |
| `airline` | Airline name |
| `price` | Total ticket price |
| `currency` | Price currency code (e.g., USD, EUR) |
| `stops` | Number of stops (0 = direct) |
| `flight_duration` | Total flight duration in minutes |
| `cabin` | Cabin class (ECONOMY, PREMIUM_ECONOMY, BUSINESS, FIRST) |
| `offer_rank` | Price ranking (1 = cheapest) |
| `departure_time` | Departure time |
| `arrival_time` | Arrival time |
| `source` | Data source identifier ("google_flights") |

## Setup

### Option A: Docker (Recommended)

1. Run with Docker Compose:

```bash
docker compose up --build
```

Data will be saved to the `./data` directory on your host.

### Option B: Local Python

#### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

#### 2. Install Playwright (Required for local mode)

```bash
pip install playwright
playwright install chromium
```

## Usage

### Test the API

First, run the test script to verify the library works from your location:

```bash
python test_flight.py
```

### Basic Usage

Run the default job to fetch prices for sample routes:

```bash
python flight_price_fetcher.py
```

This will:
- Query 15 major European routes (LHR, CDG, FRA, AMS, MAD, FCO, MUC, etc.)
- Fetch prices for dates 7-60 days out (weekly intervals)
- Save results to `data/flight_prices_YYYYMMDD_HHMMSS.csv`

### Custom Usage

```python
from flight_price_fetcher import GoogleFlightsFetcher, generate_date_range

# Initialize fetcher
fetcher = GoogleFlightsFetcher(
    request_delay=2.0,  # seconds between requests
    fetch_mode="local"  # uses local Playwright browser
)

# Define custom routes
routes = [
    ('LHR', 'CDG'),  # London to Paris
    ('FRA', 'AMS'),  # Frankfurt to Amsterdam
]

# Generate dates for next 30 days
dates = generate_date_range(start_days=1, end_days=30, step=3)

# Fetch offers
offers = fetcher.fetch_multiple_routes(
    routes=routes,
    departure_dates=dates,
    seat_class='business',  # Optional: filter by cabin
    max_offers_per_search=10
)

# Save to CSV
fetcher.save_to_csv(offers, 'my_dataset.csv')
```

### Fetch Modes

The `fetch_mode` parameter controls how requests are made:

| Mode | Description |
|------|-------------|
| `common` | Direct HTTP requests (fastest, but may hit consent walls) |
| `local` | Uses local Playwright browser (handles JavaScript, slower) |
| `fallback` | Tries common first, falls back to external API on failure |

### Single Route Query

```python
offers = fetcher.fetch_flight_offers(
    origin='JFK',
    destination='LAX',
    departure_date='2025-01-15',
    adults=1,
    seat_class='economy',
    max_offers=50
)
```

### Using fast-flights Directly

```python
from fast_flights import FlightData, Passengers, Result, get_flights

result: Result = get_flights(
    flight_data=[
        FlightData(date="2025-01-15", from_airport="JFK", to_airport="LAX")
    ],
    trip="one-way",
    seat="economy",
    passengers=Passengers(adults=1, children=0, infants_in_seat=0, infants_on_lap=0),
    fetch_mode="local",
)

print(result)
```

## Rate Limiting

To avoid being blocked by Google:
- Default delay of 2 seconds between requests
- Automatic retry with exponential backoff on errors
- Consider increasing `request_delay` for large batch jobs

## Troubleshooting

### Cookie Consent Wall (EU Users)

If you see errors like "Before you continue to Google", you're hitting the EU cookie consent wall. Solutions:

1. **Use a US-based cloud server** (AWS, DigitalOcean, etc.)
2. **Use a VPN** connected to a US server
3. **Upgrade to fast-flights v3** and use a proxy service

### Timeout Errors

If requests are timing out:
- Increase `request_delay` to 5+ seconds
- Try a different `fetch_mode`
- Check your internet connection

## License

MIT
