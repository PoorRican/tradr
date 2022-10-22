from abc import ABC, abstractmethod
import requests


class APIPrototype(ABC):
    """ Provide a common interface to interact with platform API\'s.

    Members:
        name:
            name of API/Platform. Class attribute since this is universal across the platform.
        auth:
            ambiguous key-store for authentication. Class attribute since this is universal across the platform.
        BASE_URL:
            URL to which to send endpoint requests. Used for generating API URLs.
    """

    name: str
    auth: dict
    BASE_URL: str

    def __init__(self, *args, **kwargs):
        """ Populates `auth` class level attribute which might be needed for HTTP POST requests. """
        super(APIPrototype, self).__init__()
        self._process_secrets(*args)

    @classmethod
    @abstractmethod
    def _process_secrets(cls, *args):
        """ Populate `auth` class attribute. """
        pass

    @classmethod
    def get(cls, endpoint: str, *args, **kwargs) -> requests.Response:
        """ Encapsulate and send HTTP GET request to `endpoint`.

        Args:
            endpoint:
                Endpoint URL for request. Appended to `BASE_URL`.

            args:
                Gets passed to `requests.get()`

            kwargs:
                Gets passed to `requests.get()`
        """
        return requests.get(cls.BASE_URL + endpoint, *args, **kwargs)

    @classmethod
    @abstractmethod
    def post(cls, endpoint: str, data: dict = None) -> dict:
        """ Encapsulate and send HTTP POST request to `endpoint` with `data`.

        Args:
            endpoint:
                Endpoint URL for request. Appended to `BASE_URL`.

            data:
                Data to use as payload in POST request.
        """
        pass
