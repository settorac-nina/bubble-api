from datetime import datetime, date

from bubble_api.constraint import Constraint


def test__constraints_to_dict__with_value():
    constraint = Constraint("key", "operator", "value")
    res = constraint.to_dict()

    assert res == {
        "key": "key",
        "constraint_type": "operator",
        "value": "value",
    }


def test__constraints_to_dict__without_value():
    constraint = Constraint("key", "operator")
    res = constraint.to_dict()

    assert res == {"key": "key", "constraint_type": "operator"}


def test__constraints_format_none():
    constraint = Constraint("key", "operator", None)

    assert constraint.value is None


def test__constraints_format_string():
    constraint = Constraint("key", "operator", "value")

    assert constraint.value == "value"


def test__constraints_format_int():
    constraint = Constraint("key", "operator", 8)

    assert constraint.value == "8"


def test__constraints_format_float():
    constraint = Constraint("key", "operator", 8.6)

    assert constraint.value == "8.6"


def test__constraints_format_date():
    constraint = Constraint("key", "operator", date(2023, 5, 19))

    assert constraint.value == "2023-05-19"


def test__constraints_format_datetime():
    constraint = Constraint(
        "key", "operator", datetime(2023, 5, 19, 22, 47, 46, 477590)
    )

    assert constraint.value == "2023-05-19T22:47:46.477590Z"
