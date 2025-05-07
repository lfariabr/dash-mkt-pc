import os
import json
import aiohttp
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables with more explicit path
env_path = Path('/Users/luisfaria/Desktop/sEngineer/dash/.env')
load_dotenv(dotenv_path=env_path, override=True)

logger = logging.getLogger(__name__)

# Fallback token that's known to work
FALLBACK_TOKEN = 'XXXXXXXXX'

async def fetch_graphql(session, url, query, variables):
    """
    Execute a GraphQL query using aiohttp with authentication from environment variables.
    
    Args:
        session: aiohttp ClientSession
        url: GraphQL endpoint URL
        query: GraphQL query string
        variables: Query variables
    """
    # Try to get token from environment, fall back to hardcoded if needed
    token = os.getenv('API_CRM_TOKEN')

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
    }
        
    payload = {
        "query": query,
        "variables": variables
    }
        
    attempt = 0
    max_attempts = 3
    
    while attempt < max_attempts:
        
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'errors' in data:
                        error_msg = data['errors'][0].get('message', 'Unknown GraphQL error')
                        logger.error(f"GraphQL error: {error_msg}")

                        if 'unauthorized' in error_msg.lower() or 'unauthenticated' in error_msg.lower():
                            # If using env token and it fails, try fallback
                            if token != FALLBACK_TOKEN:
                                logger.warning("Auth failed with env token, trying fallback token")
                                token = FALLBACK_TOKEN
                                headers['Authorization'] = f'Bearer {token}'
                                # Retry immediately with fallback token
                                continue
                            else:
                                logger.error("Authentication failed with fallback token")
                                return None
                        return None
                    
                    return data
                else:
                    response_text = await response.text()
                    logger.error(f"HTTP error {response.status}: {response_text}")
                    if response.status == 401:
                        if token != FALLBACK_TOKEN:
                            logger.warning("Auth failed with env token, trying fallback token")
                            token = FALLBACK_TOKEN
                            headers['Authorization'] = f'Bearer {token}'
                            continue
                        else:
                            logger.error("Authentication failed with fallback token")
                            return None
                    elif response.status >= 500:
                        logger.error("Server error - will retry")
                    else:
                        logger.error("Client error - check your request")
                        return None
        except aiohttp.ClientError as e:
            logger.error(f"Network error: {e}")
        except asyncio.TimeoutError:
            logger.error("Request timed out")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        attempt += 1
        if attempt < max_attempts:
            wait_time = min(5 * 2 ** attempt, 30)
            logger.info(f"Retrying in {wait_time} seconds (attempt {attempt}/{max_attempts})...")
            await asyncio.sleep(wait_time)
        else:
            logger.error("Max retries reached")
    
    return None