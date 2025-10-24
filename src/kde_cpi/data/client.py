"""HTTP client for retrieving CPI datasets from BLS."""

from urllib.parse import urljoin

import requests
from attrs import define, field

from .files import BASE_URL


@define(slots=True)
class CpiHttpClient:
    """Thin HTTP wrapper around the BLS CPI flat-file endpoints."""

    base_url: str = BASE_URL
    timeout: float = 30.0
    session: requests.Session = field(factory=requests.Session)

    def get_text(self, filename: str, *, encoding: str = "utf-8") -> str:
        """Fetch a remote CPI resource and return its decoded text payload."""
        url = urljoin(self.base_url, filename)
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        response.encoding = encoding
        return response.text

    def close(self) -> None:
        """Release the underlying HTTP session."""
        self.session.close()


__all__ = ["CpiHttpClient"]
