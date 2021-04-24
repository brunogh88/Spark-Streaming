from argparse import ArgumentParser, ArgumentTypeError, RawTextHelpFormatter
from datetime import datetime

APP_DESCRIPTION = """>>> IMPORTANT <<< """

HEL_PARAMETER_DATE= """You need to put a date in the format YYYYMMDD"""

HEL_PARAMETER_PIPE= """Select a pipe name that you want to run between the: SALES or NAME_NEW_PIPE """

def get_args():
    parser = ArgumentParser(prog="Pipe-Teste", description=APP_DESCRIPTION, formatter_class=RawTextHelpFormatter)
    parser.add_argument('--date', type=__is_date, required=True, help=HEL_PARAMETER_DATE)
    parser.add_argument('--pipe', type=str, required=True, help=HEL_PARAMETER_PIPE)
    return parser.parse_args()


def __is_date(date_int):
    """
    Checks if the parameter is a valid date.

    Args:
        date_int: Date Integer.

    Returns: Integer value.
    """
    try:
        datetime.strptime(date_int, '%Y%m%d')
        return date_int
    except ValueError:
        raise ArgumentTypeError('"{}" is not a valid date!'.format(date_int))