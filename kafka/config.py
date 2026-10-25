"""Kafka settings, from config.settings."""

from config.settings import (  # noqa: F401 - re-exported
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_USE_LOCAL_FALLBACK,
    LOCAL_QUEUE_DIR,
)


def bootstrap_address() -> tuple:
    """(host, port) of the first broker."""
    first = KAFKA_BOOTSTRAP_SERVERS.split(",")[0].strip()
    first = first.split("://", 1)[-1]                       # PLAINTEXT://host:port
    host, _, port = first.rpartition(":")
    if not host:                                            # no port given
        return first or "localhost", 9092
    return host.strip("[]"), int(port)


def broker_reachable(timeout: float = 1.0) -> bool:
    import socket
    host, port = bootstrap_address()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False
