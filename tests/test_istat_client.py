from pathlib import Path

from src.istat_client import RateLimiter


def test_rate_limiter_allows_up_to_max_without_sleeping(tmp_path: Path, monkeypatch):
    fake_now = [1000.0]
    monkeypatch.setattr("src.istat_client.time.time", lambda: fake_now[0])
    sleep_calls = []
    monkeypatch.setattr("src.istat_client.time.sleep", lambda s: sleep_calls.append(s))

    limiter = RateLimiter(max_per_minute=3, state_file=tmp_path / "state.json")

    limiter.wait_for_slot()
    limiter.wait_for_slot()
    limiter.wait_for_slot()

    assert sleep_calls == []


def test_rate_limiter_sleeps_once_limit_reached(tmp_path: Path, monkeypatch):
    fake_now = [1000.0]
    monkeypatch.setattr("src.istat_client.time.time", lambda: fake_now[0])

    sleep_calls = []

    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        fake_now[0] += seconds  # simula il passare del tempo durante l'attesa

    monkeypatch.setattr("src.istat_client.time.sleep", fake_sleep)

    limiter = RateLimiter(max_per_minute=2, state_file=tmp_path / "state.json")

    limiter.wait_for_slot()
    limiter.wait_for_slot()
    limiter.wait_for_slot()  # terza richiesta: deve attendere prima di procedere

    assert len(sleep_calls) >= 1
    assert sleep_calls[0] > 0
