from esorders.cli import main


def test_demo_runs_and_all_checks_pass(capsys, tmp_path):
    assert main(["demo", "--orders", "120", "--seed", "11", "--dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "All checks passed." in out
    assert "FAIL" not in out
