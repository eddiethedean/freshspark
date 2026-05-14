def test_public_api_imports():
    from freshspark import (
        ensure_fresh,
        fresh_local_spark,
        get_fresh_local_spark,
        reset_active_session,
    )

    assert callable(fresh_local_spark)
    assert callable(get_fresh_local_spark)
    assert callable(reset_active_session)
    assert callable(ensure_fresh)


def test_version_present():
    import freshspark

    assert hasattr(freshspark, "__version__")
    assert isinstance(freshspark.__version__, str)
