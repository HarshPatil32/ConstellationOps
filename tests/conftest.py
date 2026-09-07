import pytest


@pytest.fixture
def app_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UDP_HOST", "127.0.0.1")
    monkeypatch.setenv("UDP_PORT", "0")
