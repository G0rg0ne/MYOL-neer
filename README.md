# MYOL-neer

Flight ticket price dataset builder using the Amadeus API.

## Features

- Fetches real-time flight prices from Amadeus API
- Supports multiple routes and date ranges
- Exports data to CSV with standardized schema
- Configurable cabin class filters
- Rate limiting and error handling

## Dataset Schema

| Field | Description |
|-------|-------------|
| `origin` | IATA airport code of departure |
| `destination` | IATA airport code of arrival |
| `departure_date` | Flight departure date (YYYY-MM-DD) |
| `query_date` | Date when price was queried (YYYY-MM-DD) |
| `days_before_departure` | Days between query and departure |
| `airline` | Operating carrier IATA code |
| `price` | Total ticket price |
| `currency` | Price currency code (e.g., USD) |
| `stops` | Number of stops (0 = direct) |
| `flight_duration` | Total flight duration in minutes |
| `cabin` | Cabin class (ECONOMY, PREMIUM_ECONOMY, BUSINESS, FIRST) |
| `offer_rank` | Price ranking (1 = cheapest) |
| `source` | Data source identifier ("amadeus") |

## Setup

### Option A: Docker (Recommended)

1. Copy `env.example` to `.env` and add your Amadeus credentials
2. Run with Docker Compose:

```bash
docker compose up --build
```

Data will be saved to the `./data` directory on your host.

### Option B: Local Python

#### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

#### 2. Get Amadeus API Credentials

1. Create a free account at [Amadeus for Developers](https://developers.amadeus.com/)
2. Create a new app to get your API Key and Secret
3. Copy `env.example` to `.env` and fill in your credentials:

```bash
cp env.example .env
```

Edit `.env`:
```
AMADEUS_API_KEY=your_api_key_here
AMADEUS_API_SECRET=your_api_secret_here
AMADEUS_ENV=test
```

> **Note:** Use `test` environment for development (limited data, free). Switch to `production` for real data (requires paid plan).

## Usage

### Basic Usage

Run the default job to fetch prices for sample routes:

```bash
python flight_price_fetcher.py
```

This will:
- Query 15 major European routes (LHR, CDG, FRA, AMS, MAD, FCO, MUC, ZRH, etc.)
- Fetch prices for dates 7-60 days out (weekly intervals)
- Save results to `data/flight_prices_YYYYMMDD_HHMMSS.csv`

### Custom Usage

```python
from flight_price_fetcher import AmadeusFlightFetcher, generate_date_range

# Initialize fetcher
fetcher = AmadeusFlightFetcher()

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
    cabin_class='BUSINESS',  # Optional: filter by cabin
    max_offers_per_search=10
)

# Save to CSV
fetcher.save_to_csv(offers, 'my_dataset.csv')
```

### Single Route Query

```python
offers = fetcher.fetch_flight_offers(
    origin='JFK',
    destination='LAX',
    departure_date='2025-01-15',
    adults=1,
    cabin_class='ECONOMY',
    max_offers=50
)
```

## API Rate Limits

Amadeus API has rate limits depending on your plan:
- **Test environment:** ~10 requests/second
- **Production:** Varies by plan

The fetcher includes built-in error handling for rate limit responses.

## License

MIT
