import certifi
import urllib3
from urllib.parse import urlencode

retries = urllib3.util.Retry(connect=5, read=3, redirect=2, backoff_factor=.05)
upool = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where(),
                            num_pools=20, block=False, retries=retries)