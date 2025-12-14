#!/usr/bin/env python3
"""
Amadeus Flight Price Dataset Builder

Fetches flight offer data from the Amadeus API and builds a dataset
with standardized schema for price analysis.
"""

import os
import csv
import time
import logging
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

from amadeus import Client, ResponseError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class AmadeusFlightFetcher:
    """Fetches flight prices from Amadeus API and structures them into a dataset."""
    
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
        'source'
    ]
    
    # Rate limiting defaults
    DEFAULT_REQUEST_DELAY = 0.5  # seconds between requests
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY = 5  # seconds before retry on rate limit
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        environment: str = 'test',
        request_delay: float = DEFAULT_REQUEST_DELAY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY
    ):
        """
        Initialize the Amadeus client.
        
        Args:
            api_key: Amadeus API key (defaults to env var AMADEUS_API_KEY)
            api_secret: Amadeus API secret (defaults to env var AMADEUS_API_SECRET)
            environment: 'test' for sandbox or 'production' for live API
            request_delay: Seconds to wait between API requests (default 0.5s)
            max_retries: Maximum retries on rate limit errors (default 3)
            retry_delay: Base delay in seconds before retry, doubles each attempt (default 5s)
        """
        self.api_key = api_key or os.getenv('AMADEUS_API_KEY')
        self.api_secret = api_secret or os.getenv('AMADEUS_API_SECRET')
        self.environment = environment or os.getenv('AMADEUS_ENV', 'test')
        
        # Rate limiting configuration
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._last_request_time = 0.0
        
        if not self.api_key or not self.api_secret:
            raise ValueError(
                "Amadeus API credentials required. Set AMADEUS_API_KEY and "
                "AMADEUS_API_SECRET environment variables or pass them directly."
            )
        
        # Initialize Amadeus client
        hostname = 'production' if self.environment == 'production' else 'test'
        self.client = Client(
            client_id=self.api_key,
            client_secret=self.api_secret,
            hostname=hostname
        )
        logger.info(f"Amadeus client initialized (environment: {hostname}, delay: {request_delay}s)")
    
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
        Parse ISO 8601 duration string to minutes.
        
        Args:
            duration_str: Duration in ISO 8601 format (e.g., 'PT2H30M')
            
        Returns:
            Total duration in minutes
        """
        if not duration_str:
            return 0
            
        duration_str = duration_str.replace('PT', '')
        hours = 0
        minutes = 0
        
        if 'H' in duration_str:
            hours_part, duration_str = duration_str.split('H')
            hours = int(hours_part)
        
        if 'M' in duration_str:
            minutes_part = duration_str.replace('M', '')
            if minutes_part:
                minutes = int(minutes_part)
        
        return hours * 60 + minutes
    
    def fetch_flight_offers(
        self,
        origin: str,
        destination: str,
        departure_date: str,
        adults: int = 1,
        cabin_class: Optional[str] = None,
        max_offers: int = 50
    ) -> list[dict]:
        """
        Fetch flight offers from Amadeus API.
        
        Args:
            origin: IATA airport/city code (e.g., 'JFK')
            destination: IATA airport/city code (e.g., 'LAX')
            departure_date: Date in YYYY-MM-DD format
            adults: Number of adult passengers
            cabin_class: Optional cabin class (ECONOMY, PREMIUM_ECONOMY, BUSINESS, FIRST)
            max_offers: Maximum number of offers to retrieve
            
        Returns:
            List of structured flight offer dictionaries
        """
        query_date = datetime.now().strftime('%Y-%m-%d')
        departure_dt = datetime.strptime(departure_date, '%Y-%m-%d')
        query_dt = datetime.strptime(query_date, '%Y-%m-%d')
        days_before = (departure_dt - query_dt).days
        
        try:
            # Build search parameters
            search_params = {
                'originLocationCode': origin.upper(),
                'destinationLocationCode': destination.upper(),
                'departureDate': departure_date,
                'adults': adults,
                'max': max_offers
            }
            
            if cabin_class:
                search_params['travelClass'] = cabin_class.upper()
            
            logger.info(f"Fetching flights: {origin} → {destination} on {departure_date}")
            
            # Rate limiting: wait between requests
            self._wait_for_rate_limit()
            
            # Make API request with retry logic for rate limits
            response = None
            for attempt in range(self.max_retries + 1):
                try:
                    response = self.client.shopping.flight_offers_search.get(**search_params)
                    break  # Success, exit retry loop
                except ResponseError as e:
                    if e.response.status_code == 429:  # Rate limit exceeded
                        if attempt < self.max_retries:
                            wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                            logger.warning(
                                f"Rate limit hit, waiting {wait_time}s before retry "
                                f"({attempt + 1}/{self.max_retries})"
                            )
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error("Max retries exceeded for rate limit")
                            raise
                    else:
                        raise  # Re-raise non-rate-limit errors
            
            if response is None:
                return []
            
            offers = []
            for rank, offer in enumerate(response.data, start=1):
                # Extract price information
                price = float(offer.get('price', {}).get('total', 0))
                currency = offer.get('price', {}).get('currency', 'USD')
                
                # Process each itinerary (we focus on outbound)
                for itinerary in offer.get('itineraries', []):
                    segments = itinerary.get('segments', [])
                    
                    if not segments:
                        continue
                    
                    # Calculate stops (number of segments - 1)
                    stops = len(segments) - 1
                    
                    # Get primary airline (operating carrier of first segment)
                    first_segment = segments[0]
                    airline = first_segment.get('operating', {}).get('carrierCode') or \
                              first_segment.get('carrierCode', 'UNKNOWN')
                    
                    # Parse total flight duration
                    duration_str = itinerary.get('duration', 'PT0M')
                    flight_duration = self._parse_duration(duration_str)
                    
                    # Get cabin class from traveler pricings
                    cabin = 'ECONOMY'  # default
                    traveler_pricings = offer.get('travelerPricings', [])
                    if traveler_pricings:
                        fare_details = traveler_pricings[0].get('fareDetailsBySegment', [])
                        if fare_details:
                            cabin = fare_details[0].get('cabin', 'ECONOMY')
                    
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
                        'cabin': cabin,
                        'offer_rank': rank,
                        'source': 'amadeus'
                    }
                    offers.append(offer_data)
                    break  # Only take first itinerary per offer
            
            logger.info(f"Retrieved {len(offers)} flight offers")
            return offers
            
        except ResponseError as e:
            # Handle common API errors gracefully
            status = e.response.status_code
            body = e.response.body
            
            if status == 400 or 'SYSTEM ERROR' in str(body):
                # Test environment often lacks data for certain routes
                logger.warning(
                    f"No data available for {origin}→{destination} on {departure_date} "
                    f"(API returned {status}). Skipping..."
                )
            else:
                logger.error(f"Amadeus API error: {status} - {body}")
            return []
        except Exception as e:
            logger.error(f"Error fetching flight offers: {e}")
            return []
    
    def fetch_multiple_routes(
        self,
        routes: list[tuple[str, str]],
        departure_dates: list[str],
        cabin_class: Optional[str] = None,
        max_offers_per_search: int = 50
    ) -> list[dict]:
        """
        Fetch flight offers for multiple routes and dates.
        
        Args:
            routes: List of (origin, destination) tuples
            departure_dates: List of departure dates in YYYY-MM-DD format
            cabin_class: Optional cabin class filter
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
                    cabin_class=cabin_class,
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
    
    # European routes - using city codes for better test environment coverage
    # Note: Amadeus test environment has limited data; some routes may return no results
    routes = [
        ('LON', 'PAR'),  # London to Paris (city codes have broader coverage)
        ('PAR', 'LON'),  # Paris to London
        ('MAD', 'BCN'),  # Madrid to Barcelona
        ('BCN', 'MAD'),  # Barcelona to Madrid
        ('FRA', 'LON'),  # Frankfurt to London
        ('LON', 'AMS'),  # London to Amsterdam
        ('PAR', 'MAD'),  # Paris to Madrid
        ('MAD', 'ROM'),  # Madrid to Rome
        ('LON', 'ROM'),  # London to Rome
        ('PAR', 'BCN'),  # Paris to Barcelona
        ('AMS', 'PAR'),  # Amsterdam to Paris
        ('MUC', 'LON'),  # Munich to London
        ('FRA', 'PAR'),  # Frankfurt to Paris
        ('LON', 'BER'),  # London to Berlin
        ('PAR', 'MIL'),  # Paris to Milan
    ]
    
    # Generate dates: every 7 days for the next 60 days
    departure_dates = generate_date_range(start_days=7, end_days=60, step=7)
    
    # Output file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path(__file__).parent / 'data'
    output_file = output_dir / f'flight_prices_{timestamp}.csv'
    
    try:
        # Initialize fetcher
        fetcher = AmadeusFlightFetcher()
        
        # Fetch all offers
        all_offers = fetcher.fetch_multiple_routes(
            routes=routes,
            departure_dates=departure_dates,
            max_offers_per_search=20
        )
        
        # Save to CSV
        if all_offers:
            fetcher.save_to_csv(all_offers, str(output_file))
            print(f"\n✓ Dataset saved to: {output_file}")
            print(f"  Total records: {len(all_offers)}")
        else:
            print("\n✗ No flight offers retrieved")
            
    except ValueError as e:
        print(f"\n✗ Configuration error: {e}")
        print("  Please set up your .env file with Amadeus credentials.")
        return 1
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())

