from bubble_api.constraint import Constraint
from bubble_api.field import Field


def test__field_name_format():
    field = Field("Rental Unit")

    assert field.field_name == "rentalunit"


def test__field_to_constraint__equals():
    res = Field("key") == "value"

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "equals",
        "value": "value",
    }


def test__field_to_constraint__not_equals():
    res = Field("key") != "value"

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "not equals",
        "value": "value",
    }


def test__field_to_constraint__is_empty():
    res = Field("key").is_empty()

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "is_empty",
    }


def test__field_to_constraint__is_not_empty():
    res = Field("key").is_not_empty()

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "is_not_empty",
    }


def test__field_to_constraint__text_contains():
    res = Field("key").text_contains("sub_text")

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "text contains",
        "value": "sub_text",
    }


def test__field_to_constraint__not_text_contains():
    res = Field("key").not_text_contains("sub_text")

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "not text contains",
        "value": "sub_text",
    }


def test__field_to_constraint__greater_than():
    res = Field("key") > "value"

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "greater than",
        "value": "value",
    }


def test__field_to_constraint__greater_than_reversed():
    res = "value" < Field("key")

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "greater than",
        "value": "value",
    }


def test__field_to_constraint__less_than():
    res = Field("key") < "value"

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "less than",
        "value": "value",
    }


def test__field_to_constraint__less_than_reversed():
    res = "value" > Field("key")

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "less than",
        "value": "value",
    }


def test__field_to_constraint__is_in():
    res = Field("key").is_in("list")

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "in",
        "value": "list",
    }


def test__field_to_constraint__is_not_in():
    res = Field("key").is_not_in("list")

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "not in",
        "value": "list",
    }


def test__field_to_constraint__contains():
    res = Field("key").contains("value")

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "contains",
        "value": "value",
    }


def test__field_to_constraint__not_contains():
    res = Field("key").not_contains("value")

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "not contains",
        "value": "value",
    }


def test__field_to_constraint__geographic_search():
    res = Field("key").geographic_search("value")

    assert isinstance(res, Constraint)
    assert res.to_dict() == {
        "key": "key",
        "constraint_type": "geographic search",
        "value": "value",
    }
