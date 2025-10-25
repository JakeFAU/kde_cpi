"""HTTP client for retrieving CPI datasets from BLS."""

from urllib.parse import urljoin

import requests
from attrs import define, field
import structlog

from .files import BASE_URL

logger = structlog.get_logger(__name__)


@define(slots=True)
class CpiHttpClient:
    """Thin HTTP wrapper around the BLS CPI flat-file endpoints."""

    base_url: str = BASE_URL
    timeout: float = 30.0
    session: requests.Session = field(factory=requests.Session)
    headers: dict[str, str] = field(
        factory=lambda: {
            "User-Agent": "jacob.bourne@gmail.com",
            "Accept": "application/json,text/plain,*/*;q=0.1",
        },
    )

    def get_text(self, filename: str, *, encoding: str = "utf-8") -> str:
        """Fetch a remote CPI resource and return its decoded text payload."""
        url = urljoin(self.base_url, filename)
        log = logger.bind(filename=filename, url=url)
        log.debug("http.fetch_start", timeout=self.timeout)
        try:
            response = self.session.get(url, timeout=self.timeout, headers=self.headers)
            response.raise_for_status()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            log.error("http.fetch_failed", status=status, exc_info=True)
            raise
        response.encoding = encoding
        log.debug("http.fetch_success", bytes=len(response.content))
        return response.text

    def close(self) -> None:
        """Release the underlying HTTP session."""
        self.session.close()
        logger.debug("http.session_closed")


__all__ = ["CpiHttpClient"]
