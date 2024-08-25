from immaterialdb.value_serializers import decrement_ord_of_last_char, increment_ord_of_last_char


def test_increment_last_char():
    assert "abc" == increment_ord_of_last_char("abb")


def test_decrement_last_char():
    assert "abb" == decrement_ord_of_last_char("abc")
