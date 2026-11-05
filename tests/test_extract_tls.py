"""Tests for the TLS options used by ``src.extract.connect_to_postgres``."""
from unittest import mock

import pytest

from src import extract


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Isolate tests from DB_SSLMODE / DB_SSLROOTCERT / PGHOST in the env."""
    for name in ("DB_SSLMODE", "DB_SSLROOTCERT", "PGHOST"):
        monkeypatch.delenv(name, raising=False)


def _connect(host="db.example.com", **kwargs):
    with mock.patch.object(extract.psycopg2, "connect") as connect:
        extract.connect_to_postgres("etl", host, 5432, "u", "p", **kwargs)
    return connect.call_args.kwargs


def test_defaults_to_verify_full():
    """Without configuration the connection verifies the server cert."""
    kwargs = _connect()
    assert kwargs["sslmode"] == "verify-full"
    assert "sslrootcert" not in kwargs


def test_reads_sslmode_and_rootcert_from_env(monkeypatch):
    """DB_SSLMODE and DB_SSLROOTCERT are passed through to libpq."""
    monkeypatch.setenv("DB_SSLMODE", "verify-ca")
    monkeypatch.setenv("DB_SSLROOTCERT", "/etc/ssl/rds-ca.pem")
    kwargs = _connect()
    assert kwargs["sslmode"] == "verify-ca"
    assert kwargs["sslrootcert"] == "/etc/ssl/rds-ca.pem"


def test_explicit_arguments_override_env(monkeypatch):
    """Arguments take precedence over the environment."""
    monkeypatch.setenv("DB_SSLMODE", "require")
    kwargs = _connect(sslmode="verify-full", sslrootcert="system")
    assert kwargs["sslmode"] == "verify-full"
    assert kwargs["sslrootcert"] == "system"


@pytest.mark.parametrize("mode", ["disable", "allow", "prefer", "PREFER"])
def test_weak_modes_rejected_for_remote_hosts(monkeypatch, mode):
    """Cleartext-capable modes never reach psycopg2 for remote hosts."""
    monkeypatch.setenv("DB_SSLMODE", mode)
    with mock.patch.object(extract.psycopg2, "connect") as connect:
        with pytest.raises(ValueError):
            extract.connect_to_postgres(
                "etl", "db.example.com", 5432, "u", "p")
    connect.assert_not_called()


@pytest.mark.parametrize(
    "host", ["localhost", "127.0.0.1", "::1", "/var/run/postgresql", ""])
def test_weak_modes_allowed_for_local_hosts(monkeypatch, host):
    """docker-compose / socket setups may opt out of TLS explicitly."""
    monkeypatch.setenv("DB_SSLMODE", "disable")
    assert _connect(host=host)["sslmode"] == "disable"


def test_weak_mode_rejected_when_pghost_is_remote(monkeypatch):
    """An empty host falls back to PGHOST, which must also be local."""
    monkeypatch.setenv("DB_SSLMODE", "disable")
    monkeypatch.setenv("PGHOST", "db.example.com")
    with pytest.raises(ValueError):
        _connect(host=None)


def test_mixed_host_list_is_not_local(monkeypatch):
    """Any remote entry in a multi-host list makes the target remote."""
    monkeypatch.setenv("DB_SSLMODE", "prefer")
    with pytest.raises(ValueError):
        _connect(host="localhost,db.example.com")


def test_unknown_mode_rejected():
    """Typos fail closed instead of falling back to libpq defaults."""
    with pytest.raises(ValueError):
        _connect(sslmode="verify_full")
