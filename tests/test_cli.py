import sys
import subprocess
import shutil
import pytest

pytestmark = pytest.mark.skipif(
    shutil.which(sys.executable) is None,
    reason="Python executable not found for subprocess tests",
)

def _run_cmd(args, input_text=None, timeout=90):
    proc = subprocess.Popen(
        [sys.executable, "-m", "freshspark"] + args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    out, err = proc.communicate(input=input_text, timeout=timeout)
    return proc.returncode, out, err

def test_cli_reset_exits_cleanly():
    code, out, err = _run_cmd(["reset"])
    assert code == 0
    assert "freshspark:" in out
    assert err == ""

def test_cli_repl_starts_and_exits_quickly():
    # Start REPL with tiny preset and no UI (faster, less flaky), then exit
    code, out, err = _run_cmd(["repl", "--preset", "tiny", "--no-ui"], input_text="exit()\n", timeout=120)
    assert code == 0
    assert "freshspark REPL" in out
    # Don't assert on err content; Spark may log to stderr
