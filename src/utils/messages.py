"""This module contains log messages"""


def __title(value, msg):
    """Format log message

    Args:
        value: Title
        msg: Log Message

    Returns: Formatted log message
    """
    title = '---------------< {} >---------------'.format(value)
    return '\n{}\n{}\n{}'.format(title, msg, '-' * len(title))


#
# App Messages
#
APP_STARTED = 'App Started!'
APP_FINISHED = 'App Finished! Time elapsed: {}'
APP_ARGS = 'App Args: {}'