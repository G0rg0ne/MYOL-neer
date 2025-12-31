#!/usr/bin/env python3
"""
Google Flights Price Dataset Builder

Fetches flight offer data from Google Flights using fast-flights
and builds a dataset with standardized schema for price analysis.
"""

import csv
import time
import logging
import re
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

import yaml

from fast_flights import FlightData, Passengers, create_filter, get_flights_from_filter


def load_config(config_path: Optional[str] = None) -> dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config file. If None, looks for config.yaml in script directory.
        
    Returns:
        Configuration dictionary with all settings
    """
    if config_path is None:
        config_path = Path(__file__).parent / 'config.yaml'
    else:
        config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GoogleFlightsFetcher:
    """Fetches flight prices from Google Flights and structures them into a dataset."""
    
    SCHEMA = [
        'origin',
        'destination',
        'departure_date',
        'query_date',
        'days_before_departure',
        'airline',
        'price',
        'currency',
        'stops',
        'flight_duration',
        'cabin',
        'offer_rank',
        'departure_time',
        'arrival_time',
        'source'
    ]
    
    # Rate limiting defaults
    DEFAULT_REQUEST_DELAY = 2.0  # seconds between requests (be respectful to Google)
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY = 10  # seconds before retry on error
    
    # Seat class mapping
    SEAT_CLASSES = {
        'economy': 'ECONOMY',
        'premium-economy': 'PREMIUM_ECONOMY',
        'business': 'BUSINESS',
        'first': 'FIRST'
    }
    
    def __init__(
        self,
        request_delay: float = DEFAULT_REQUEST_DELAY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        mode: str = "local",
        max_direct_durations: Optional[dict] = None
    ):
        """
        Initialize the Google Flights fetcher.
        
        Args:
            request_delay: Seconds to wait between requests (default 2s)
            max_retries: Maximum retries on errors (default 3)
            retry_delay: Base delay in seconds before retry (default 10s)
            mode: fast-flights mode for get_flights_from_filter:
                - "common": Direct HTTP requests (fastest, may hit consent walls)
                - "local": Uses local Playwright browser (handles JS, slower)
            max_direct_durations: Dict mapping route keys to max direct flight duration in minutes.
                Format: {"AMS-CDG": 100, "CDG-MAD": 160, ...}
        """
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.mode = mode
        self._last_request_time = 0.0
        self._max_direct_durations = self._parse_duration_config(max_direct_durations or {})
        
        logger.info(f"Google Flights fetcher initialized (delay: {request_delay}s, mode: {mode})")
    
    def _parse_duration_config(self, config_durations: dict) -> dict:
        """
        Parse duration config from YAML format to internal tuple-key format.
        
        Args:
            config_durations: Dict like {"AMS-CDG": 100, "CDG-MAD": 160}
            
        Returns:
            Dict like {("AMS", "CDG"): 100, ("CDG", "MAD"): 160}
        """
        parsed = {}
        for route_str, duration in config_durations.items():
            parts = route_str.upper().split('-')
            if len(parts) == 2:
                # Sort to make lookup order-independent
                key = tuple(sorted(parts))
                parsed[key] = duration
        return parsed
    
    def _wait_for_rate_limit(self) -> None:
        """Wait to respect rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.request_delay:
            sleep_time = self.request_delay - elapsed
            logger.debug(f"Rate limit: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self._last_request_time = time.time()
    
    def _parse_duration(self, duration_str: str) -> int:
        """
        Parse duration string to minutes.
        
        Args:
            duration_str: Duration like "2 hr 30 min" or "1h 45m"
            
        Returns:
            Total duration in minutes
        """
        if not duration_str:
            return 0
        
        duration_str = duration_str.lower().strip()
        hours = 0
        minutes = 0
        
        # Try pattern: "2 hr 30 min" or "2 hr" or "30 min"
        hr_match = re.search(r'(\d+)\s*(?:hr|hour|h)', duration_str)
        min_match = re.search(r'(\d+)\s*(?:min|minute|m)', duration_str)
        
        if hr_match:
            hours = int(hr_match.group(1))
        if min_match:
            minutes = int(min_match.group(1))
        
        return hours * 60 + minutes
    
    def _parse_price(self, price_str: str) -> tuple[float, str]:
        """
        Parse price string to amount and currency.
        
        Args:
            price_str: Price like "$299" or "€199" or "299 USD"
            
        Returns:
            Tuple of (price_amount, currency_code)
        """
        if not price_str:
            return 0.0, 'USD'
        
        price_str = price_str.strip()
        
        # Currency symbol mapping
        currency_symbols = {
            '$': 'USD',
            '€': 'EUR',
            '£': 'GBP',
            '¥': 'JPY',
            '₹': 'INR',
            'A$': 'AUD',
            'C$': 'CAD',
        }
        
        currency = 'USD'
        for symbol, code in currency_symbols.items():
            if symbol in price_str:
                currency = code
                price_str = price_str.replace(symbol, '')
                break
        
        # Extract numeric value
        price_match = re.search(r'[\d,]+(?:\.\d+)?', price_str.replace(',', ''))
        if price_match:
            price_value = float(price_match.group().replace(',', ''))
        else:
            price_value = 0.0
        
        return price_value, currency
    
    def _format_time(self, hour: int, minute: int, date_tuple: tuple) -> str:
        """
        Format time and date into a readable string.
        
        Args:
            hour: Hour (0-23)
            minute: Minute (0-59)
            date_tuple: Date as (year, month, day) tuple
            
        Returns:
            Formatted string like "8:10 AM on Sun, Mar 1"
        """
        try:
            year, month, day = date_tuple
            dt = datetime(year, month, day, hour, minute)
            # Format time: 12-hour with AM/PM
            hour_12 = hour % 12 or 12
            am_pm = "AM" if hour < 12 else "PM"
            time_str = f"{hour_12}:{minute:02d} {am_pm}"
            # Format date: "Sun, Mar 1"
            date_str = dt.strftime("%a, %b") + f" {day}"
            return f"{time_str} on {date_str}"
        except Exception:
            return f"{hour}:{minute:02d}"
    
    def _parse_stops(self, stops_info: str) -> int:
        """
        Parse stops information.
        
        Args:
            stops_info: String like "Nonstop", "1 stop", "2 stops"
            
        Returns:
            Number of stops (0 for nonstop), or -1 if unknown/unparseable
        """
        if not stops_info:
            return -1  # Unknown - could not parse
        
        stops_info = stops_info.lower().strip()
        
        if 'nonstop' in stops_info or 'direct' in stops_info:
            return 0
        
        stops_match = re.search(r'(\d+)\s*stop', stops_info)
        if stops_match:
            return int(stops_match.group(1))
        
        return -1  # Unknown - could not parse
    
    def _infer_stops_from_duration(self, origin: str, destination: str, duration_minutes: int) -> int:
        """
        Infer number of stops based on flight duration and route.
        
        This is a fallback when we can't parse stops from the HTML.
        Uses typical direct flight durations from config.
        
        Args:
            origin: Origin airport code
            destination: Destination airport code
            duration_minutes: Flight duration in minutes
            
        Returns:
            Inferred number of stops (0 or 1+), or -1 if can't infer
        """
        if duration_minutes <= 0:
            return -1
        
        route_key = tuple(sorted([origin.upper(), destination.upper()]))
        
        # Look up max direct duration from config
        max_direct = self._max_direct_durations.get(route_key)
        
        if max_direct:
            # If duration is within expected range, likely nonstop
            if duration_minutes <= max_direct:
                return 0
            else:
                # Duration too long for direct, likely has stops
                return 1  # At least 1 stop
        
        # For unknown routes, use a heuristic:
        # European short-haul direct flights rarely exceed 4 hours
        if duration_minutes <= 240:
            return 0  # Probably direct
        else:
            return 1  # Probably has stops
    
    def fetch_flight_offers(
        self,
        origin: str,
        destination: str,
        departure_date: str,
        adults: int = 1,
        children: int = 0,
        seat_class: str = 'economy',
        max_offers: int = 50
    ) -> list[dict]:
        """
        Fetch flight offers from Google Flights.
        
        Args:
            origin: IATA airport code (e.g., 'JFK')
            destination: IATA airport code (e.g., 'LAX')
            departure_date: Date in YYYY-MM-DD format
            adults: Number of adult passengers
            children: Number of child passengers
            seat_class: Seat class (economy, premium-economy, business, first)
            max_offers: Maximum number of offers to retrieve
            
        Returns:
            List of structured flight offer dictionaries
        """
        query_date = datetime.now().strftime('%Y-%m-%d')
        departure_dt = datetime.strptime(departure_date, '%Y-%m-%d')
        query_dt = datetime.strptime(query_date, '%Y-%m-%d')
        days_before = (departure_dt - query_dt).days
        
        # Skip past dates
        if days_before < 0:
            logger.warning(f"Skipping past date: {departure_date}")
            return []
        
        try:
            logger.info(f"Fetching flights: {origin} → {destination} on {departure_date}")
            
            # Rate limiting: wait between requests
            self._wait_for_rate_limit()
            
            # Prepare flight data
            flight_data = FlightData(
                date=departure_date,
                from_airport=origin.upper(),
                to_airport=destination.upper()
            )
            
            # Prepare passengers
            passengers = Passengers(
                adults=adults,
                children=children,
                infants_in_seat=0,
                infants_on_lap=0
            )
            
            # Normalize seat class
            seat = seat_class.lower() if seat_class else 'economy'
            if seat not in self.SEAT_CLASSES:
                seat = 'economy'
            
            cabin_label = self.SEAT_CLASSES[seat]
            
            # Create filter using the same method as test_flight.py
            flight_filter = create_filter(
                flight_data=[flight_data],
                trip="one-way",
                passengers=passengers,
                seat=seat,
            )
            
            # Make request with retry logic
            result = None
            for attempt in range(self.max_retries + 1):
                try:
                    result = get_flights_from_filter(
                        flight_filter,
                        mode=self.mode,
                    )
                    break  # Success, exit retry loop
                except Exception as e:
                    if attempt < self.max_retries:
                        wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(
                            f"Request failed, waiting {wait_time}s before retry "
                            f"({attempt + 1}/{self.max_retries}): {e}"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Max retries exceeded: {e}")
                        raise
            
            if result is None or not hasattr(result, 'flights') or not result.flights:
                logger.warning(f"No flights found for {origin}→{destination} on {departure_date}")
                return []
            
            offers = []
            for rank, flight in enumerate(result.flights[:max_offers], start=1):
                # Parse flight details
                price, currency = self._parse_price(getattr(flight, 'price', ''))
                
                # Get duration first (needed for stops inference)
                duration_str = getattr(flight, 'duration', '')
                flight_duration = self._parse_duration(str(duration_str) if duration_str else '')
                
                # Get stops info - now the core.py has improved selectors
                stops_info = getattr(flight, 'stops', '')
                stops = self._parse_stops(str(stops_info) if stops_info else '')
                
                # If stops couldn't be parsed, try to infer from duration
                if stops == -1 and flight_duration > 0:
                    stops = self._infer_stops_from_duration(origin, destination, flight_duration)
                    if stops >= 0:
                        logger.debug(f"Inferred stops={stops} for {origin}→{destination} ({flight_duration} min)")
                
                # Sanity check: if duration is very long (>6 hours) but marked as nonstop,
                # it's likely wrong - infer from duration instead
                if stops == 0 and flight_duration > 360:
                    inferred = self._infer_stops_from_duration(origin, destination, flight_duration)
                    if inferred > 0:
                        logger.warning(
                            f"Suspicious flight: {origin}→{destination} marked as nonstop "
                            f"but duration is {flight_duration} min. Inferring stops={inferred}."
                        )
                        stops = inferred
                
                # Get airline
                airline = getattr(flight, 'name', '') or 'Unknown'
                # Clean up airline name - just get the carrier code or short name
                if airline and ',' in airline:
                    airline = airline.split(',')[0].strip()
                
                # Get times
                departure_time = getattr(flight, 'departure', '') or ''
                arrival_time = getattr(flight, 'arrival', '') or ''
                
                offer_data = {
                    'origin': origin.upper(),
                    'destination': destination.upper(),
                    'departure_date': departure_date,
                    'query_date': query_date,
                    'days_before_departure': days_before,
                    'airline': airline,
                    'price': price,
                    'currency': currency,
                    'stops': stops,
                    'flight_duration': flight_duration,
                    'cabin': cabin_label,
                    'offer_rank': rank,
                    'departure_time': departure_time,
                    'arrival_time': arrival_time,
                    'source': 'google_flights'
                }
                offers.append(offer_data)
            
            logger.info(f"Retrieved {len(offers)} flight offers")
            return offers
            
        except Exception as e:
            logger.error(f"Error fetching flight offers: {e}")
            return []
    
    def fetch_multiple_routes(
        self,
        routes: list[tuple[str, str]],
        departure_dates: list[str],
        seat_class: str = 'economy',
        adults: int = 1,
        max_offers_per_search: int = 50
    ) -> list[dict]:
        """
        Fetch flight offers for multiple routes and dates.
        
        Args:
            routes: List of (origin, destination) tuples
            departure_dates: List of departure dates in YYYY-MM-DD format
            seat_class: Seat class filter
            adults: Number of adult passengers
            max_offers_per_search: Max offers per individual search
            
        Returns:
            Combined list of all flight offers
        """
        all_offers = []
        total_queries = len(routes) * len(departure_dates)
        current_query = 0
        
        logger.info(f"Starting {total_queries} queries ({len(routes)} routes × {len(departure_dates)} dates)")
        
        for origin, destination in routes:
            for date in departure_dates:
                current_query += 1
                logger.info(f"Progress: {current_query}/{total_queries} queries")
                
                offers = self.fetch_flight_offers(
                    origin=origin,
                    destination=destination,
                    departure_date=date,
                    adults=adults,
                    seat_class=seat_class,
                    max_offers=max_offers_per_search
                )
                all_offers.extend(offers)
        
        logger.info(f"Completed all queries. Total offers collected: {len(all_offers)}")
        return all_offers
    
    def save_to_csv(
        self,
        offers: list[dict],
        output_path: str,
        append: bool = False
    ) -> str:
        """
        Save flight offers to a CSV file.
        
        Args:
            offers: List of flight offer dictionaries
            output_path: Path to output CSV file
            append: If True, append to existing file; otherwise overwrite
            
        Returns:
            Path to the saved file
        """
        if not offers:
            logger.warning("No offers to save")
            return output_path
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        file_exists = output_file.exists()
        mode = 'a' if append and file_exists else 'w'
        
        with open(output_file, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.SCHEMA)
            
            if mode == 'w' or not file_exists:
                writer.writeheader()
            
            writer.writerows(offers)
        
        logger.info(f"Saved {len(offers)} offers to {output_path}")
        return output_path


def generate_date_range(start_days: int = 1, end_days: int = 90, step: int = 7) -> list[str]:
    """
    Generate a list of future dates for querying.
    
    Args:
        start_days: Days from today to start
        end_days: Days from today to end
        step: Day interval between dates
        
    Returns:
        List of dates in YYYY-MM-DD format
    """
    today = datetime.now()
    dates = []
    
    for days in range(start_days, end_days + 1, step):
        future_date = today + timedelta(days=days)
        dates.append(future_date.strftime('%Y-%m-%d'))
    
    return dates


def main(config_path: Optional[str] = None):
    """
    Main entry point for the flight price fetcher.
    
    Args:
        config_path: Optional path to config file. If None, uses config.yaml in script directory.
    """
    
    # Load configuration
    try:
        config = load_config(config_path)
        logger.info(f"Loaded configuration from {config_path or 'config.yaml'}")
    except FileNotFoundError as e:
        print(f"\n✗ {e}")
        print("  Please create a config.yaml file or specify a valid config path.")
        return 1
    
    # Extract settings from config
    fetcher_config = config.get('fetcher', {})
    search_config = config.get('search', {})
    date_config = config.get('date_range', {})
    output_config = config.get('output', {})
    
    # Routes from config (convert lists to tuples)
    routes_list = config.get('routes', [])
    routes = [tuple(route) for route in routes_list]
    
    if not routes:
        print("\n✗ No routes configured in config.yaml")
        return 1
    
    # Generate dates from config
    departure_dates = generate_date_range(
        start_days=date_config.get('start_days', 7),
        end_days=date_config.get('end_days', 60),
        step=date_config.get('step', 7)
    )
    
    # Output file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path(__file__).parent / output_config.get('directory', 'data')
    file_prefix = output_config.get('file_prefix', 'flight_prices')
    output_file = output_dir / f'{file_prefix}_{timestamp}.csv'
    
    # Get max direct durations from config
    max_direct_durations = config.get('max_direct_durations', {})
    
    try:
        # Initialize fetcher with config settings
        fetcher = GoogleFlightsFetcher(
            request_delay=fetcher_config.get('request_delay', 2.0),
            max_retries=fetcher_config.get('max_retries', 3),
            retry_delay=fetcher_config.get('retry_delay', 10),
            mode=fetcher_config.get('mode', 'local'),
            max_direct_durations=max_direct_durations
        )
        
        # Log configuration summary
        logger.info(f"Routes: {len(routes)}")
        logger.info(f"Date range: {date_config.get('start_days', 7)}-{date_config.get('end_days', 60)} days, step={date_config.get('step', 7)}")
        logger.info(f"Seat class: {search_config.get('seat_class', 'economy')}")
        logger.info(f"Max offers per search: {search_config.get('max_offers_per_search', 20)}")
        
        # Fetch all offers
        all_offers = fetcher.fetch_multiple_routes(
            routes=routes,
            departure_dates=departure_dates,
            seat_class=search_config.get('seat_class', 'economy'),
            adults=search_config.get('adults', 1),
            max_offers_per_search=search_config.get('max_offers_per_search', 20)
        )
        
        # Save to CSV
        if all_offers:
            fetcher.save_to_csv(all_offers, str(output_file))
            print(f"\n✓ Dataset saved to: {output_file}")
            print(f"  Total records: {len(all_offers)}")
        else:
            print("\n✗ No flight offers retrieved")
            
    except Exception as e:
        print(f"\n✗ Error: {e}")
        logger.exception("Error during flight fetching")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
