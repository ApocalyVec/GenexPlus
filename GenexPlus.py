from __future__ import unicode_literals, print_function
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.styles import Style

"""processes commands"""


def do_load(filename, format='original'):
    msg = FormattedText([
        ('class:normal', 'loading'),
        ('', ' '),
        ('class:prompt', filename),
        # ('class:bbb', 'fileName'),
    ])

    return msg

def invalid_load_prompt():
    msg = FormattedText([
        ('class:error', 'Invalid load'),
        ('', '\n'),
        ('class:example', 'Example command: load yourFileName.txt'),
    ])

    return msg
