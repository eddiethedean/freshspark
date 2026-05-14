import pytest
from freshspark import reset_active_session


@pytest.fixture(autouse=True)
def _ensure_no_active_session_before_after():
    # Make sure nothing is hanging around before/after each test
    reset_active_session()
    yield
    reset_active_session()
