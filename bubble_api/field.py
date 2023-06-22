from .constraint import Constraint


class Field:
    def __init__(self, field_name):
        self.field_name = self.field_name = self.format_field_name(field_name)

    @staticmethod
    def format_field_name(field_name: str):
        return field_name.lower().replace(" ", "")

    def equals(self, other):
        return Constraint(
            self.field_name,
            "equals",
            other,
        )

    def not_equals(self, other):
        return Constraint(
            self.field_name,
            "not equal",
            other,
        )

    __eq__ = equals
    __ne__ = not_equals

    def is_empty(self):
        return Constraint(
            self.field_name,
            "is_empty",
        )

    def is_not_empty(self):
        return Constraint(
            self.field_name,
            "is_not_empty",
        )

    def text_contains(self, text):
        return Constraint(
            self.field_name,
            "text contains",
            text,
        )

    def not_text_contains(self, text):
        return Constraint(
            self.field_name,
            "not text contains",
            text,
        )

    def greater_than(self, value):
        return Constraint(
            self.field_name,
            "greater than",
            value,
        )

    def less_than(self, value):
        return Constraint(
            self.field_name,
            "less than",
            value,
        )

    __gt__ = greater_than
    __lt__ = less_than

    def __ge__(self, value):
        raise NotImplementedError

    def __le__(self, value):
        raise NotImplementedError

    def is_in(self, value):
        return Constraint(
            self.field_name,
            "in",
            value,
        )

    def is_not_in(self, value):
        return Constraint(
            self.field_name,
            "not in",
            value,
        )

    def contains(self, value):
        return Constraint(
            self.field_name,
            "contains",
            value,
        )

    def not_contains(self, value):
        return Constraint(
            self.field_name,
            "not contains",
            value,
        )

    def geographic_search(self, value):
        return Constraint(
            self.field_name,
            "geographic_search",
            value,
        )
