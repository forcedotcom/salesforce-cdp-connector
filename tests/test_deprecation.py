import importlib
import warnings


def test_deprecation_warning_emitted_on_import():
    import salesforcecdpconnector

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        importlib.reload(salesforcecdpconnector)
        assert any(
            issubclass(w.category, DeprecationWarning)
            and "salesforce-datacloud-connector" in str(w.message)
            for w in caught
        )
