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

from fast_flights import FlightData, Passengers, create_filter, get_flights_from_filter

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
        mode: str = "local"
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
        """
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.mode = mode
        self._last_request_time = 0.0
        
        logger.info(f"Google Flights fetcher initialized (delay: {request_delay}s, mode: {mode})")
    
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
    
    def _parse_stops(self, stops_info: str) -> int:
        """
        Parse stops information.
        
        Args:
            stops_info: String like "Nonstop", "1 stop", "2 stops"
            
        Returns:
            Number of stops (0 for nonstop)
        """
        if not stops_info:
            return 0
        
        stops_info = stops_info.lower().strip()
        
        if 'nonstop' in stops_info or 'direct' in stops_info:
            return 0
        
        stops_match = re.search(r'(\d+)\s*stop', stops_info)
        if stops_match:
            return int(stops_match.group(1))
        
        return 0
    
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
                
                # Get stops info
                stops_info = getattr(flight, 'stops', '')
                stops = self._parse_stops(str(stops_info) if stops_info else 'Nonstop')
                
                # Get duration
                duration_str = getattr(flight, 'duration', '')
                flight_duration = self._parse_duration(str(duration_str) if duration_str else '')
                
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


def main():
    """Main entry point for the flight price fetcher."""
    
    # European routes using IATA airport codes
    routes = [
        ('LHR', 'CDG'),  # London Heathrow to Paris CDG
        ('CDG', 'LHR'),  # Paris CDG to London Heathrow
        ('MAD', 'BCN'),  # Madrid to Barcelona
        ('BCN', 'MAD'),  # Barcelona to Madrid
        ('FRA', 'LHR'),  # Frankfurt to London
        ('LHR', 'AMS'),  # London to Amsterdam
        ('CDG', 'MAD'),  # Paris to Madrid
        ('MAD', 'FCO'),  # Madrid to Rome
        ('LHR', 'FCO'),  # London to Rome
        ('CDG', 'BCN'),  # Paris to Barcelona
        ('AMS', 'CDG'),  # Amsterdam to Paris
        ('MUC', 'LHR'),  # Munich to London
        ('FRA', 'CDG'),  # Frankfurt to Paris
        ('LHR', 'BER'),  # London to Berlin
        ('CDG', 'MXP'),  # Paris to Milan
    ]
    
    # Generate dates: every 7 days for the next 60 days
    departure_dates = generate_date_range(start_days=7, end_days=60, step=7)
    
    # Output file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path(__file__).parent / 'data'
    output_file = output_dir / f'flight_prices_{timestamp}.csv'
    
    try:
        # Initialize fetcher
        fetcher = GoogleFlightsFetcher(
            request_delay=2.0,  # Be respectful to Google
            mode="local"  # Uses local Playwright browser
        )
        
        # Fetch all offers
        all_offers = fetcher.fetch_multiple_routes(
            routes=routes,
            departure_dates=departure_dates,
            seat_class='economy',
            max_offers_per_search=20
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
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
