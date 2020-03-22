from Launcher import get_cleaned_data


def test_cleaned_record_count():
    a = get_cleaned_data().count()
    assert (a == 17872)
