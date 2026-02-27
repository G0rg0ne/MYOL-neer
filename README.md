# MYOL-neer

**Make Your Own Label — neer** (never ending endless research)

A scheduled Google Flights price dataset builder that scrapes real-time flight offers across configured routes and date ranges, then exports the results to structured CSV files — locally or directly to S3-compatible storage.

Built on top of the excellent [**fast-flights**](https://github.com/AWeirdDev/flights) library, which reverse-engineered Google Flights' protobuf-based URL parameters to enable fast, API-key-free flight data retrieval.

---

## How It Works

1. Reads routes, date ranges, and search parameters from `config.yaml`
2. Iterates over all (route × date) combinations and fetches live offers from Google Flights
3. Parses and normalises results into a flat, analysis-ready schema
4. Saves data to a timestamped CSV file in the `data/` directory
5. Optionally uploads the CSV to an S3 / MinIO bucket and removes the local copy

The tool is designed for scheduled, unattended execution: run it daily as a Docker container or a Kubernetes CronJob to build a longitudinal price dataset.

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.12 |
| Flight data | [fast-flights](https://github.com/AWeirdDev/flights) (embedded) |
| HTTP client | `primp` (Chrome-impersonating client) |
| Browser automation | `playwright` (local mode) |
| HTML parsing | `selectolax` |
| Protobuf | `protobuf >= 5.27` |
| Configuration | `pyyaml` |
| Logging | `loguru` |
| S3 storage | `boto3` |
| Dependency management | `uv` + `hatchling` |
| Containerisation | Docker / Docker Compose |
| Kubernetes | Helm chart + CronJob + SealedSecrets |

---

## Project Structure

```
MYOL-neer/
├── flight_price_fetcher.py      # Main entry point & GoogleFlightsFetcher class
├── test_flight.py               # Quick connectivity/library test
├── config.yaml                  # Local configuration (not committed)
├── pyproject.toml               # Project metadata & dependencies
├── uv.lock                      # Locked dependency versions
├── Dockerfile                   # Container image definition
├── docker-compose.yml           # Compose for local Docker runs
│
├── fast_flights/                # Embedded fast-flights library (AWeirdDev/flights)
│   ├── __init__.py
│   ├── core.py                  # Main fetch logic
│   ├── filter.py                # TFS filter builder
│   ├── schema.py                # Result / Flight data models
│   ├── flights_impl.py          # FlightData, Passengers, TFSData
│   ├── local_playwright.py      # Playwright-based fetching
│   ├── fallback_playwright.py   # Fallback Playwright implementation
│   ├── bright_data_fetch.py     # BrightData proxy integration
│   ├── decoder.py               # JSON/protobuf response decoder
│   ├── cookies_impl.py          # Cookie handling
│   ├── search.py                # Airport search utilities
│   ├── primp.py                 # HTTP client wrapper
│   ├── cookies_pb2.py           # Generated protobuf definitions
│   ├── flights_pb2.py           # Generated protobuf definitions
│   └── _generated_enum.py       # IATA airport code enum
│
├── k8s/                         # Kubernetes / Helm deployment
│   ├── Chart.yaml               # Helm chart metadata
│   ├── values.yaml              # Default Helm values
│   └── templates/
│       ├── _helpers.tpl
│       ├── namespace.yaml
│       ├── cronjob.yaml         # Daily CronJob
│       ├── configmap.yaml       # config.yaml generated from Helm values
│       └── sealed-secret-s3.yaml # Encrypted S3 credentials (Bitnami SealedSecrets)
│
└── assets/
    └── deployment_argoCD.png    # ArgoCD deployment screenshot
```

---

## Dataset Schema

Each row in the output CSV represents a single flight offer fetched at a specific point in time.

| Field | Type | Description |
|-------|------|-------------|
| `origin` | string | IATA code of departure airport |
| `destination` | string | IATA code of arrival airport |
| `departure_date` | date | Flight departure date (YYYY-MM-DD) |
| `query_date` | date | Date the price was queried (YYYY-MM-DD) |
| `days_before_departure` | int | Days between query date and departure |
| `airline` | string | Operating airline name |
| `price` | float | Total ticket price |
| `currency` | string | Currency code (e.g. EUR, USD) |
| `stops` | int | Number of stops (0 = direct) |
| `flight_duration` | int | Total flight duration in minutes |
| `cabin` | string | Cabin class (ECONOMY / PREMIUM_ECONOMY / BUSINESS / FIRST) |
| `offer_rank` | int | Price ranking within the search result (1 = cheapest) |
| `departure_time` | string | Departure time |
| `arrival_time` | string | Arrival time |
| `source` | string | Data source identifier (`"google_flights"`) |

---

## Configuration

All settings live in `config.yaml` in the project root (or are injected via Kubernetes ConfigMap). This file is **not committed to Git** — create it from the template below.

```yaml
# Fetcher behaviour
fetcher:
  request_delay: 2.0     # Seconds between requests
  max_retries: 3         # Retry attempts on failure
  retry_delay: 10        # Base delay before retry (exponential backoff)
  mode: "local"          # "common" | "local" | "bright-data"
  max_concurrent: 3      # Async concurrency limit

# Search parameters
search:
  max_offers_per_search: 20
  seat_class: "economy"  # economy | premium-economy | business | first
  adults: 1
  children: 0
  currency: "EUR"

# Date range (relative to today)
date_range:
  end_days: 30           # How many days ahead to search
  step: 1                # Interval between dates

# Routes (IATA airport codes)
routes:
  - ["CDG", "AMS"]       # Paris → Amsterdam
  - ["CDG", "MAD"]       # Paris → Madrid
  - ["LHR", "CDG"]       # London → Paris

# Output
output:
  directory: "data"
  file_prefix: "flight_prices"

# S3 / MinIO (optional)
s3:
  enabled: false
  endpoint_url: "http://your-minio:9000"
  bucket: "flight-data"
  prefix: "prices/"
  # Credentials via env vars: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
```

### Fetch Modes

| Mode | Description | When to Use |
|------|-------------|-------------|
| `common` | Direct HTTP requests via `primp` | Fastest; works outside EU |
| `local` | Playwright-controlled local Chromium | Handles JS / EU cookie consent walls |
| `bright-data` | Routes requests through BrightData SERP proxy | Reliable worldwide; requires account |

---

## Setup

### Option A: Local Python

**1. Install dependencies (using uv):**

```bash
pip install uv
uv sync
```

Or with pip:

```bash
pip install -r requirements.txt   # if you export one, or install from pyproject.toml
```

**2. Install Playwright (required for `local` mode):**

```bash
uv run playwright install chromium
# or
playwright install chromium
```

**3. Create your config:**

Copy the template above into `config.yaml` and edit routes/dates.

**4. Test connectivity:**

```bash
uv run python test_flight.py
```

**5. Run the fetcher:**

```bash
uv run python flight_price_fetcher.py
```

Results are saved to `data/flight_prices_YYYYMMDD_HHMMSS.csv`.

---

### Option B: Docker (Recommended for Local Scheduling)

```bash
docker compose up --build
```

Output is written to `./data/` on the host via a bind mount.

---

### Option C: Kubernetes with Helm

**1. Customise values:**

Edit `k8s/values.yaml` — set your routes, schedule, S3 endpoint, and image tag.

**2. Install the Helm chart:**

```bash
helm install flight-fetcher ./k8s --namespace flight-fetcher --create-namespace
```

The chart deploys:
- A **Namespace** (`flight-fetcher`)
- A **ConfigMap** containing the generated `config.yaml`
- A **CronJob** that runs daily at the configured time (default: noon UTC)
- A **SealedSecret** for S3 credentials (requires Bitnami Sealed Secrets controller)

**3. Monitor jobs:**

```bash
kubectl get cronjobs -n flight-fetcher
kubectl get pods -n flight-fetcher
kubectl logs -l app=flight-fetcher -n flight-fetcher --tail=100
```

---

### Option D: GitOps with ArgoCD

Apply the ArgoCD Application manifest to let ArgoCD manage the deployment from Git:

```bash
kubectl apply -f k8s/argocd-application.yaml
```

ArgoCD will automatically sync changes from the repository with self-healing enabled.

![ArgoCD Deployment](assets/deployment_argoCD.png)

---

## Usage Examples

### Programmatic Usage

```python
from flight_price_fetcher import GoogleFlightsFetcher, generate_date_range

fetcher = GoogleFlightsFetcher(
    request_delay=2.0,
    mode="local",
    max_concurrent=3,
)

routes = [
    ("LHR", "CDG"),   # London → Paris
    ("FRA", "AMS"),   # Frankfurt → Amsterdam
]

dates = generate_date_range(end_days=30, step=3)

offers = fetcher.fetch_multiple_routes(
    routes=routes,
    departure_dates=dates,
    seat_class="business",
    max_offers_per_search=10,
)

fetcher.save_to_csv(offers, "my_dataset.csv")
```

### Single Route Query

```python
offers = fetcher.fetch_flight_offers(
    origin="JFK",
    destination="LAX",
    departure_date="2025-06-15",
    adults=1,
    seat_class="economy",
    max_offers=50,
)
```

### Using fast-flights Directly

```python
from fast_flights import FlightData, Passengers, create_filter, get_flights_from_filter

flight_data = FlightData(
    date="2025-06-15",
    from_airport="JFK",
    to_airport="LAX",
)
passengers = Passengers(adults=1, children=0, infants_in_seat=0, infants_on_lap=0)

flight_filter = create_filter(
    flight_data=[flight_data],
    trip="one-way",
    passengers=passengers,
    seat="economy",
)

result = get_flights_from_filter(flight_filter, mode="local")
print(result)
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | If S3 enabled | S3 / MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | If S3 enabled | S3 / MinIO secret key |
| `BRIGHT_DATA_API_KEY` | If `mode: bright-data` | BrightData API key |
| `BRIGHT_DATA_API_URL` | No | BrightData endpoint (default: `https://api.brightdata.com/request`) |
| `BRIGHT_DATA_SERP_ZONE` | No | BrightData zone name (default: `serp_api1`) |
| `KUBECONFIG` | No | Path to kubeconfig for kubectl access |

---

## Rate Limiting & Best Practices

- Default 2-second delay between requests (`fetcher.request_delay`)
- Automatic exponential backoff retry on failures
- For large batch jobs (many routes × many dates), increase `request_delay` to 5+ seconds
- Keep `max_concurrent` at 3 or below to avoid triggering Google rate limits

---

## Important: Regional Restrictions

Google Flights shows a cookie consent dialog for EU-based IP addresses, which can block automated requests.

**Solutions:**

| Scenario | Recommendation |
|----------|---------------|
| Running from EU | Switch to `mode: local` (Playwright handles the consent wall) |
| Cloud deployment | Use a US-East region (AWS, DigitalOcean, etc.) |
| Reliable worldwide | Use `mode: bright-data` with a BrightData SERP zone |

---

## Troubleshooting

### "Before you continue to Google" error
You are hitting the EU cookie consent wall. Use `mode: local` or a non-EU server.

### Timeout errors
- Increase `request_delay` to 5+ seconds
- Switch to `mode: local`
- Reduce `max_concurrent`

### `config.yaml` not found
Create `config.yaml` in the project root. In Kubernetes, it is automatically injected via ConfigMap.

### Playwright not installed
```bash
uv run playwright install chromium
```

---

## Credits

- **[fast-flights](https://github.com/AWeirdDev/flights)** by [AWeirdDev](https://github.com/AWeirdDev) — the core Google Flights scraping library used in this project. It works by reverse-engineering Google's protobuf-encoded `tfs` URL parameter to construct valid search requests without needing an API key. The `fast_flights/` directory in this repository contains an embedded copy of that library.

---

## License

MIT
