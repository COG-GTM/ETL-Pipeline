import demo.run_demo as run_demo_module


def test_zero_touch_demo_runs_end_to_end(monkeypatch, tmp_path, capsys):
    """The Zero-Touch ETL demo should run all phases on the bundled sample data."""
    monkeypatch.setenv("DEMO_AUTO_RUN", "1")
    monkeypatch.setattr(run_demo_module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(run_demo_module, "DEMO_DIR", str(tmp_path))

    run_demo_module.run_demo()

    out = capsys.readouterr().out
    assert "Running in auto mode" in out
    assert (tmp_path / "domain_knowledge" / "domain_knowledge.json").exists()
