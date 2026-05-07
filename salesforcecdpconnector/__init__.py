import warnings

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

warnings.warn(
    "salesforce-cdp-connector is deprecated and will be removed once "
    "salesforce-datacloud-connector reaches GA. Please migrate to "
    "salesforce-datacloud-connector. See "
    "https://github.com/forcedotcom/salesforce-cdp-connector#migration "
    "for migration guidance.",
    DeprecationWarning,
    stacklevel=2,
)
