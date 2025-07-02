from Jobs.getDataVelo import to_bool
import pytest

# tous les formats de "oui" retournent bien True
def test_to_bool_returns_true_for_oui_variants():
    assert to_bool("oui") is True
    assert to_bool("OUI") is True
    assert to_bool(" Oui ") is True
    assert to_bool("   oUi") is True

#tous les formats de "non" retournent False
def test_to_bool_returns_false_for_non_variants():
    assert to_bool("non") is False
    assert to_bool("NON") is False
    assert to_bool("  Non ") is False

# Que "", None, " " donnent bien False
def test_to_bool_with_empty_or_none():
    assert to_bool("") is False
    assert to_bool(None) is False
    assert to_bool("   ") is False

# Que des valeurs inattendues comme "yes", "1" donnent False
def test_to_bool_with_other_unexpected_values():
    assert to_bool("yes") is False
    assert to_bool("0") is False
    assert to_bool("1") is False
    assert to_bool("vrai") is False
    assert to_bool(True) is False  # même un booléen `True` donne False, car != "oui"
