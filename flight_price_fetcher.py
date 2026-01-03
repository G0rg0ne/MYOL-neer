#!/usr/bin/env python3
"""
Google Flights Price Dataset Builder

Fetches flight offer data from Google Flights using fast-flights
and builds a dataset with standardized schema for price analysis.
"""

import csv
import os
import time
import logging
import re
import asyncio
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

import yaml
import boto3
from botocore.exceptions import ClientError, BotoCoreError

from fast_flights import FlightData, Passengers, create_filter, get_flights_from_filter
from fast_flights.core import get_flights_from_filter_async
from fast_flights.local_playwright import PlaywrightSession


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
    DEFAULT_MAX_CONCURRENT = 3  # maximum parallel requests
    
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
        max_direct_durations: Optional[dict] = None,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT
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
            max_concurrent: Maximum number of parallel requests (default 3)
        """
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.mode = mode
        self.max_concurrent = max_concurrent
        self._last_request_time = 0.0
        
        logger.info(f"Google Flights fetcher initialized (delay: {request_delay}s, mode: {mode}, max_concurrent: {max_concurrent})")
    
    
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
    
    def _parse_stops(self, stops_info) -> int:
        """
        Parse stops information.
        
        Args:
            stops_info: Integer (already parsed), or string like "Nonstop", "1 stop", "2 stops"
            
        Returns:
            Number of stops (0 for nonstop), or -1 if unknown/unparseable
        """
        # If already an integer, return directly (core.py already parses this)
        if isinstance(stops_info, int):
            return stops_info
        
        if not stops_info:
            return -1  # Unknown - could not parse
        
        # Convert to string and check if it's just a number
        stops_str = str(stops_info).strip()
        if stops_str.isdigit():
            return int(stops_str)
        
        stops_str = stops_str.lower()
        
        if 'nonstop' in stops_str or 'direct' in stops_str:
            return 0
        
        if stops_str == 'unknown':
            return -1
        
        stops_match = re.search(r'(\d+)\s*stop', stops_str)
        if stops_match:
            return int(stops_match.group(1))
        
        return -1  # Unknown - could not parse
    
    
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
                
                # Get stops info - core.py returns int (0, 1, 2...) or "Unknown"
                stops_info = getattr(flight, 'stops', None)
                stops = self._parse_stops(stops_info)
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
    
    async def _async_rate_limiter(self, last_request_time: dict) -> None:
        """Async rate limiter that respects request_delay between requests."""
        current_time = time.time()
        elapsed = current_time - last_request_time.get('time', 0)
        if elapsed < self.request_delay:
            sleep_time = self.request_delay - elapsed
            await asyncio.sleep(sleep_time)
        last_request_time['time'] = time.time()
    
    async def fetch_flight_offers_async(
        self,
        origin: str,
        destination: str,
        departure_date: str,
        session: Optional[PlaywrightSession] = None,
        rate_limiter_state: Optional[dict] = None,
        adults: int = 1,
        children: int = 0,
        seat_class: str = 'economy',
        max_offers: int = 50
    ) -> list[dict]:
        """
        Async version of fetch_flight_offers that supports concurrent execution.
        
        Args:
            origin: IATA airport code (e.g., 'JFK')
            destination: IATA airport code (e.g., 'LAX')
            departure_date: Date in YYYY-MM-DD format
            session: Optional PlaywrightSession for browser reuse
            rate_limiter_state: Optional dict to track rate limiting per context
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
            
            # Rate limiting: wait between requests (per context if provided)
            if rate_limiter_state is not None:
                await self._async_rate_limiter(rate_limiter_state)
            else:
                # Fallback to synchronous rate limiting
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
            
            # Create filter
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
                    # Use async version for local mode to stay in the same event loop
                    if self.mode == "local":
                        result = await get_flights_from_filter_async(
                            flight_filter,
                            session=session,
                        )
                    else:
                        # For other modes, use executor to run synchronous version
                        loop = asyncio.get_event_loop()
                        result = await loop.run_in_executor(
                            None,
                            lambda: get_flights_from_filter(
                                flight_filter,
                                mode=self.mode,
                            )
                        )
                    break  # Success, exit retry loop
                except Exception as e:
                    if attempt < self.max_retries:
                        wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(
                            f"Request failed, waiting {wait_time}s before retry "
                            f"({attempt + 1}/{self.max_retries}): {e}"
                        )
                        await asyncio.sleep(wait_time)
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
                
                # Get stops info
                stops_info = getattr(flight, 'stops', None)
                stops = self._parse_stops(stops_info)
                
                # Get airline
                airline = getattr(flight, 'name', '') or 'Unknown'
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
    
    async def fetch_multiple_routes_async(
        self,
        routes: list[tuple[str, str]],
        departure_dates: list[str],
        seat_class: str = 'economy',
        adults: int = 1,
        max_offers_per_search: int = 50
    ) -> list[dict]:
        """
        Async version: Fetch flight offers for multiple routes and dates with concurrency control.
        
        Uses semaphore to limit parallel requests and maintains rate limiting per context.
        
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
        
        logger.info(f"Starting {total_queries} queries ({len(routes)} routes × {len(departure_dates)} dates) with max_concurrent={self.max_concurrent}")
        
        # Initialize global browser session for local mode (will be reused across threads)
        session = None
        if self.mode == "local":
            from fast_flights.local_playwright import get_global_session
            session = get_global_session()
            # Initialize browser upfront to avoid delay on first request
            await session.initialize()
        
        # Semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Rate limiter state per concurrent context
        rate_limiter_states = [{'time': 0.0} for _ in range(self.max_concurrent)]
        
        async def fetch_single_query(origin: str, destination: str, date: str, query_num: int) -> list[dict]:
            """Fetch a single route/date combination with concurrency control."""
            async with semaphore:
                # Get rate limiter state for this context
                context_idx = query_num % self.max_concurrent
                rate_state = rate_limiter_states[context_idx]
                
                logger.info(f"Progress: {query_num}/{total_queries} queries")
                
                offers = await self.fetch_flight_offers_async(
                    origin=origin,
                    destination=destination,
                    departure_date=date,
                    session=session,
                    rate_limiter_state=rate_state,
                    adults=adults,
                    seat_class=seat_class,
                    max_offers=max_offers_per_search
                )
                return offers
        
        # Create all tasks
        tasks = []
        query_num = 0
        for origin, destination in routes:
            for date in departure_dates:
                query_num += 1
                task = fetch_single_query(origin, destination, date, query_num)
                tasks.append(task)
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect results and handle exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i+1} failed: {result}")
            else:
                all_offers.extend(result)
        
        # Note: We don't close the global session here as it may be reused
        # The session will be cleaned up when the process exits
        
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
    
    def upload_to_s3(
        self,
        file_path: str,
        bucket: str,
        endpoint_url: Optional[str] = None,
        region: str = "us-east-1",
        prefix: str = "",
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None
    ) -> bool:
        """
        Upload a file to S3 or S3-compatible storage.
        
        Args:
            file_path: Local path to the file to upload
            bucket: S3 bucket name
            endpoint_url: Custom S3 endpoint URL (e.g., http://minio:9000). 
                         If None, uses AWS S3.
            region: AWS region (required even for custom endpoints)
            prefix: Optional key prefix in bucket (e.g., "prices/")
            access_key_id: AWS access key ID (from env var if not provided)
            secret_access_key: AWS secret access key (from env var if not provided)
            
        Returns:
            True if upload successful, False otherwise
        """
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            logger.error(f"File not found: {file_path}")
            return False
        
        # Get credentials from parameters or environment variables
        aws_access_key_id = access_key_id or os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY')
        
        if not aws_access_key_id or not aws_secret_access_key:
            logger.error("S3 credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.")
            return False
        
        try:
            # Create S3 client with custom endpoint if provided
            s3_config = {
                'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key,
                'region_name': region
            }
            
            if endpoint_url:
                s3_config['endpoint_url'] = endpoint_url
            
            s3_client = boto3.client('s3', **s3_config)
            
            # Construct S3 key (object name)
            filename = file_path_obj.name
            s3_key = f"{prefix}{filename}" if prefix else filename
            # Remove trailing slash from prefix if present
            if s3_key.startswith('/'):
                s3_key = s3_key[1:]
            
            # Upload file
            logger.info(f"Uploading {file_path} to s3://{bucket}/{s3_key}")
            s3_client.upload_file(
                file_path,
                bucket,
                s3_key,
                ExtraArgs={'ContentType': 'text/csv'}
            )
            
            logger.info(f"Successfully uploaded to s3://{bucket}/{s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"S3 client error during upload: {e}")
            return False
        except BotoCoreError as e:
            logger.error(f"Boto3 core error during upload: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during S3 upload: {e}")
            return False


def generate_date_range(start_date: str, end_date: str) -> list[str]:
    """
    Generate a list of future dates for querying.
    
    Args:
        start_days: Days from today to start
        end_days: Days from today to end
        step: Day interval between dates
        
    Returns:
        List of dates in YYYY-MM-DD format
    """
    start_date = datetime.strptime(start_date,  "%d-%m-%Y") 
    end_date = datetime.strptime(end_date,  "%d-%m-%Y") 
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
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
    s3_config = config.get('s3', {})
    
    # Routes from config (convert lists to tuples)
    routes_list = config.get('routes', [])
    routes = [tuple(route) for route in routes_list]
    
    if not routes:
        print("\n✗ No routes configured in config.yaml")
        return 1
    
    # Generate dates from config
    departure_dates = generate_date_range(
        start_date=date_config.get('start_date', '01-01-2026'),
        end_date=date_config.get('end_date', '07-01-2026')
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
            max_direct_durations=max_direct_durations,
            max_concurrent=fetcher_config.get('max_concurrent', 3)
        )
        
        # Log configuration summary
        logger.info(f"Routes: {len(routes)}")
        logger.info(f"Date range: {date_config.get('start_days', 7)}-{date_config.get('end_days', 60)} days, step={date_config.get('step', 7)}")
        logger.info(f"Seat class: {search_config.get('seat_class', 'economy')}")
        logger.info(f"Max offers per search: {search_config.get('max_offers_per_search', 20)}")
        logger.info(f"Max concurrent requests: {fetcher.max_concurrent}")
        
        # Fetch all offers using async version for better performance
        all_offers = asyncio.run(fetcher.fetch_multiple_routes_async(
            routes=routes,
            departure_dates=departure_dates,
            seat_class=search_config.get('seat_class', 'economy'),
            adults=search_config.get('adults', 1),
            max_offers_per_search=search_config.get('max_offers_per_search', 20)
        ))
        
        # Save to CSV
        if all_offers:
            fetcher.save_to_csv(all_offers, str(output_file))
            print(f"\n✓ Dataset saved to: {output_file}")
            print(f"  Total records: {len(all_offers)}")
            
            # Upload to S3 if enabled
            if s3_config.get('enabled', False):
                endpoint_url = s3_config.get('endpoint_url', '') or None
                bucket = s3_config.get('bucket', 'flight-data')
                region = s3_config.get('region', 'us-east-1')
                prefix = s3_config.get('prefix', '')
                
                if upload_success := fetcher.upload_to_s3(
                    file_path=str(output_file),
                    bucket=bucket,
                    endpoint_url=endpoint_url,
                    region=region,
                    prefix=prefix
                ):
                    print(f"✓ Dataset uploaded to S3: s3://{bucket}/{prefix}{output_file.name}")
                else:
                    print(f"✗ Failed to upload dataset to S3")
        else:
            print("\n✗ No flight offers retrieved")
            
    except Exception as e:
        print(f"\n✗ Error: {e}")
        logger.exception("Error during flight fetching")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
