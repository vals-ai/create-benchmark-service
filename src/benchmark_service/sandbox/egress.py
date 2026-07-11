from __future__ import annotations

import ipaddress
from urllib.parse import urlparse


def _parse_ipv4_network(value: str) -> ipaddress.IPv4Network | None:
    try:
        network = ipaddress.ip_network(value, strict=False)
    except ValueError:
        return None

    if isinstance(network, ipaddress.IPv4Network):
        return network

    raise ValueError(f"allowed address is an IPv6 CIDR which is not supported: {value}")


def resolve_allowed_addresses(allowed_addresses: list[str]) -> tuple[list[str], list[str]]:
    values = [address.strip() for address in allowed_addresses]
    if not values or any(not value for value in values):
        raise ValueError("allowed addresses cannot be empty; use sandbox.clear_egress_rules to clear egress rules")

    cidrs: list[str] = []
    domains: list[str] = []

    for value in values:
        network = _parse_ipv4_network(value)
        if network is not None:
            cidrs.append(str(network))
            continue

        parsed = urlparse(value if "://" in value else f"//{value}")
        if not parsed.hostname:
            raise ValueError(f"allowed address is not a valid URL, host, or CIDR: {value}")

        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            domains.append(parsed.hostname)
            continue

        if isinstance(address, ipaddress.IPv4Address):
            cidrs.append(f"{address}/32")
        else:
            raise ValueError(f"allowed address is an IPv6 address which is not supported: {value}")

    cidrs = list(dict.fromkeys(cidrs))
    domains = list(dict.fromkeys(domains))
    if not cidrs and not domains:
        raise ValueError("allowed addresses did not resolve to provider-compatible rules")

    return cidrs, domains
