import pytest
def test_string_is_digit():
    items = ["1", "10", "33"]
    for item in items:
        assert item.isdigit()
