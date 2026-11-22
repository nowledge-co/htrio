# Security policy

## Supported versions

Security fixes are provided for the latest released minor version. Users should
upgrade to the latest `htrio` release before reporting a suspected regression.

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting for this repository. Do not
open a public issue or include proof-of-concept traffic, credentials, keys, or
private endpoint details in public discussions.

Include the affected package and version, impact, reproduction conditions, and
any proposed mitigation. We will acknowledge a complete report within seven
days and coordinate disclosure after a fix is available.

## Scope

Reports involving HTTP parsing, TLS identity, QUIC transport state, retry and
replay safety, proxy isolation, or credential/header leakage are in scope.
General support requests and vulnerabilities solely in unsupported dependency
versions should use the public issue tracker.
