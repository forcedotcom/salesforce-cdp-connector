from loguru import logger

def clean_login_url(login_url: str) -> str:
    """Cleans the login url by removing the protocol."""
    if not login_url:
        return ""
    cleaned = login_url.lower()
    cleaned = cleaned.replace('https://', '').replace('http://', '')
    # Remove trailing slash if present
    if cleaned.endswith('/'):
        cleaned = cleaned[:-1]
    logger.trace(f"Cleaned login url: {cleaned}")
    return cleaned