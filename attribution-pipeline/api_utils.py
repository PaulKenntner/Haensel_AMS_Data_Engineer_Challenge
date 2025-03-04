"""
API utilities for the attribution pipeline.

This module provides functions for interacting with the IHC Attribution API,
including authentication, sending data, and processing responses.
"""

import requests
import json
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
import os
from datetime import datetime
from config import IHC_API_KEY, IHC_CONV_TYPE_ID, API_MAX_RETRIES, API_RETRY_DELAY

from config import logger

class IHCApiClient:
    """Client for interacting with the IHC Attribution API."""
    
    def __init__(
        self, 
        api_key: str, 
        conv_type_id: str,
        base_url: str = "https://api.ihc-attribution.com/v1",
        max_retries: int = API_MAX_RETRIES,
        retry_delay: int = API_RETRY_DELAY
    ):
        """
        Initialize the IHC API client.
        
        Args:
            api_key: API key for authentication
            conv_type_id: Conversion type ID for the API
            base_url: Base URL for the API
            max_retries: Maximum number of retries for failed requests
            retry_delay: Delay between retries in seconds
        """
        self.api_key = api_key
        self.conv_type_id = conv_type_id
        self.base_url = base_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Validate API key
        if not api_key:
            raise ValueError("API key is required")
        
        # Validate conversion type ID
        if not conv_type_id:
            raise ValueError("Conversion type ID is required")
        
        logger.info(f"Initialized IHC API client with conversion type ID: {conv_type_id}")
    
    def get_headers(self) -> Dict[str, str]:
        """
        Get headers for API requests.
        
        Returns:
            Dictionary of headers
        """
        return {
            'Content-Type': 'application/json',
            'x-api-key': self.api_key
        }
    
    def compute_ihc(
        self, 
        customer_journeys: List[Dict[str, Any]],
        redistribution_parameter: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Compute IHC attribution for customer journeys.
        
        Args:
            customer_journeys: List of dictionaries representing customer journey sessions
            redistribution_parameter: Optional redistribution parameters
            
        Returns:
            Dictionary with API response
            
        Raises:
            requests.RequestException: If the API request fails
            ValueError: If the API returns an error
        """
        url = f"{self.base_url}/compute_ihc?conv_type_id={self.conv_type_id}"
        
        # Prepare request body
        body = {
            'customer_journeys': customer_journeys
        }
        
        # Add redistribution parameter if provided
        if redistribution_parameter:
            body['redistribution_parameter'] = redistribution_parameter
        
        # Log request details
        logger.info(f"Sending request to {url} with {len(customer_journeys)} sessions")
        
        # Send request with retry logic
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    url,
                    headers=self.get_headers(),
                    data=json.dumps(body)
                )
                
                # Check if request was successful
                response.raise_for_status()
                
                # Parse response
                result = response.json()
                
                # Check for API errors
                if result.get('statusCode') not in [200, 206]:
                    error_msg = f"API error: {result.get('statusCode')} - {result.get('message', 'Unknown error')}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                
                # Log partial failures if any
                partial_failures = result.get('partialFailureErrors', [])
                if partial_failures:
                    logger.warning(f"API returned {len(partial_failures)} partial failures")
                    for failure in partial_failures:
                        logger.warning(f"Partial failure: {failure}")
                
                logger.info(f"Successfully computed IHC for {len(customer_journeys)} sessions")
                return result
            
            except (requests.RequestException, ValueError) as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts: {e}")
                    raise
    
    def process_ihc_results(
        self, 
        api_response: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Process IHC attribution results from API response.
        
        Args:
            api_response: Dictionary with API response
            
        Returns:
            List of dictionaries with processed attribution results
        """
        # Extract attribution results
        attribution_results = api_response.get('value', [])
        
        # Process results
        processed_results = []
        for result in attribution_results:
            processed_result = {
                'conv_id': result['conversion_id'],
                'session_id': result['session_id'],
                'ihc': result['ihc']
            }
            processed_results.append(processed_result)
        
        logger.info(f"Processed {len(processed_results)} attribution results")
        return processed_results

def send_journeys_to_api(
    api_client: IHCApiClient,
    journey_chunks: List[List[Dict[str, Any]]],
    rate_limit_delay: float = 1.0
) -> List[Dict[str, Any]]:
    """
    Send journey chunks to the API and collect results.
    
    Args:
        api_client: IHC API client
        journey_chunks: List of journey chunks
        rate_limit_delay: Delay between API requests in seconds
        
    Returns:
        List of dictionaries with attribution results
    """
    all_results = []
    
    for i, chunk in enumerate(journey_chunks):
        try:
            logger.info(f"Processing chunk {i+1}/{len(journey_chunks)} with {len(chunk)} sessions")
            
            # Validate journey data
            from journey_builder import validate_journey_data
            if not validate_journey_data(chunk):
                logger.error(f"Invalid journey data in chunk {i+1}")
                continue
            
            # Send to API
            response = api_client.compute_ihc(chunk)
            
            # Process results
            results = api_client.process_ihc_results(response)
            all_results.extend(results)
            
            # Respect rate limits
            if i < len(journey_chunks) - 1:
                time.sleep(rate_limit_delay)
                
        except Exception as e:
            logger.error(f"Error processing chunk {i+1}: {e}")
            # Continue with next chunk
    
    logger.info(f"Processed {len(all_results)} attribution results from {len(journey_chunks)} chunks")
    return all_results

def save_api_response(
    response: Dict[str, Any],
    output_dir: str,
    prefix: str = "ihc_response"
) -> str:
    """
    Save API response to a file for debugging or auditing.
    
    Args:
        response: API response
        output_dir: Directory to save the file
        prefix: Prefix for the filename
        
    Returns:
        Path to the saved file
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)
    
    # Save response
    with open(filepath, 'w') as f:
        json.dump(response, f, indent=2)
    
    logger.info(f"Saved API response to {filepath}")
    return filepath

def validate_api_results(attribution_results: List[Dict[str, Any]]) -> bool:
    """
    Validate attribution results.
    
    Checks:
    - Required fields are present
    - IHC values are in range [0, 1]
    
    Args:
        attribution_results: List of dictionaries with attribution results
        
    Returns:
        True if results are valid, False otherwise
    """
    required_fields = ['conv_id', 'session_id', 'ihc']
    
    for i, result in enumerate(attribution_results):
        # Check required fields
        for field in required_fields:
            if field not in result:
                logger.error(f"Result {i} missing required field: {field}")
                return False
        
        # Check IHC value
        if not 0 <= result['ihc'] <= 1:
            logger.error(f"Result {i} has invalid IHC value: {result['ihc']}")
            return False
    
    # Check if there are any results
    if not attribution_results:
        logger.warning("No attribution results to validate")
        return False
    
    logger.info(f"Validated {len(attribution_results)} attribution results")
    return True

def get_api_credentials_from_env() -> Tuple[str, str]:
    """
    Get API credentials from config.py.
    
    Returns:
        Tuple of (api_key, conv_type_id)
        
    Raises:
        ValueError: If credentials are not valid
    """
    # Use values from config.py instead of directly from environment
    api_key = IHC_API_KEY
    conv_type_id = IHC_CONV_TYPE_ID
    
    if not api_key:
        raise ValueError("IHC_API_KEY not set in config.py")
    
    if not conv_type_id:
        raise ValueError("IHC_CONV_TYPE_ID not set in config.py")
    
    return api_key, conv_type_id

def create_redistribution_parameter(
    direct_channels: List[str] = None
) -> Dict[str, Any]:
    """
    Create redistribution parameter for the IHC API.
    
    This is used to redistribute attribution from direct channels to other channels.
    
    Args:
        direct_channels: List of channel names to be redistributed
        
    Returns:
        Dictionary with redistribution parameters
    """
    if direct_channels is None:
        direct_channels = ['Direct']
    
    redistribution_parameter = {
        'initializer': {
            'direction': 'earlier_sessions_only',
            'receive_threshold': 0,
            'redistribution_channel_labels': direct_channels
        },
        'holder': {
            'direction': 'any_session',
            'receive_threshold': 0,
            'redistribution_channel_labels': direct_channels
        },
        'closer': {
            'direction': 'later_sessions_only',
            'receive_threshold': 0.1,
            'redistribution_channel_labels': direct_channels
        }
    }
    
    return redistribution_parameter
