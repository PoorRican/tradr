import base64
import hashlib
import hmac
import json
import requests
import time

from core.api_proto import APIPrototype


class GeminiProto(APIPrototype):
    """ Prototype common interface for the Gemini API.

    Notes:
        Stores API secret information and defines HTTP methods to serve as the parent class for both `Exchange` and
        `Market` classes and therefore contains common data necessary for each class.

    References:
        https://docs.gemini.com/rest-api/
    """

    name = 'Gemini'
    valid_freqs = ('1m', '5m', '15m', '30m', '1hr', '6hr', '1day')
    BASE_URL = "https://api.gemini.com"
    auth = {'key': '', 'secret': b''}

    @classmethod
    def _process_secrets(cls, *args):
        assert len(args) == 2
        cls.auth['key'] = args[0]
        cls.auth['secret'] = args[1].encode()

    @classmethod
    def post(cls, endpoint: str, data: dict = None) -> dict:
        """ Encapsulates and delivers payload to API endpoint.

        Args:
            endpoint:
                API URL destination. Appended to `BASE_URL`.
            data:
                Payload to send to `endpoint`.

        Returns:
            Assumes that data returned is in a JSON format. Key-value pairs are returned.

        References:
            Refer to Gemini\'s API docs for a better explanation of how payloads and nonce is formed:
            https://docs.gemini.com/rest-api/#private-api-invocation
        """
        if data is None:
            data = dict()
        payload_nonce = time.time()

        payload = {"request": endpoint, "nonce": payload_nonce}
        payload.update(data)

        encoded_payload = json.dumps(payload).encode()
        b64 = base64.b64encode(encoded_payload)
        sig = hmac.new(cls.auth['secret'], b64, hashlib.sha384).hexdigest()

        headers = {
            "Content-Type": "text/plain",
            "Content-Length": "0",
            "X-GEMINI-APIKEY": cls.auth['key'],
            "X-GEMINI-PAYLOAD": b64,
            "X-GEMINI-SIGNATURE": sig,
            "Cache-Control": "no-cache"
        }

        response = requests.post(cls.BASE_URL + endpoint, headers=headers, data=None)

        return response.json()
