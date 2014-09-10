"""
Deuce Client API
"""

__DEUCE_VERSION__ = {
    'major': 0,
    'minor': 1
}


def version():
    """Return the Deuce Client Version"""
    return '{0:}.{1:}'.format(__DEUCE_VERSION__['major'],
                              __DEUCE_VERSION__['minor'])
