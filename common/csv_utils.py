from unicodedata import numeric
from orthoimagery_pipeline.common.exceptions import InvalidCSVException

def is_numeric(string):
    """
    Test if Python string is a number
    :param string:
    :return: Boolean value if string is a number
    """
    try:
        float(string)
        return True
    except ValueError:
        pass

    try:
        numeric(string)
        return True
    except TypeError:
        pass

    return False


def validate_csv(filename):
    """
    Validate if csv file is properly formatted
    All lines must be of form 'url, cc', where url is a url and cc is numeric string
    Raises assertion error if invalid line is encountered
    :param filename: filename of CSV file to validate
    :return: None
    """

    with open(filename, 'r') as f:
        csv_lines = f.readlines()

    entries = [entry.split(',') for entry in csv_lines]

    try:
        assert all([len(e) == 2 for e in entries])
        assert all([is_numeric(e[1]) for e in entries])
    except AssertionError:
        raise InvalidCSVException

    return
