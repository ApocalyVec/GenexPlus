"""
prompt_toolkit: https://python-prompt-toolkit.readthedocs.io/en/master/pages/printing_text.html
"""

from prompt_toolkit import prompt, print_formatted_text
from prompt_toolkit.formatted_text import FormattedText, PygmentsTokens
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter, Completion, Completer
import click
import fuzzyfinder
from prompt_toolkit.styles import Style

# commands
import GenexPlus as gp

GPKeywords = ['load', 'group', 'cluster', 'query', 'plot', 'help', 'exit']

style = Style.from_dict({
    # User input (default text).
    '': '#ff0066',

    # Prompt.
    'error': '#ff0000',
    'normal': '#00cbff',
    'prompt': '#00ffff bg:#444400',
    'file': '#000651 bg:#cccccc italic',
    'example': 'ansicyan underline'
    # 'pound':    '#00aa00',
    # 'host':     '#00ffff bg:#444400',
    # 'path':     'ansicyan underline',
})


class GPCompleter(Completer):
    def get_completions(self, document, complete_evennt):
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        matches = fuzzyfinder(word_before_cursor, GPKeywords)
        for m in matches:
            yield Completion(m, start_position=-len(word_before_cursor))


GPCompleter = WordCompleter(['load', 'group', 'cluster', 'query', 'plot'],
                            ignore_case=True)


while 1:
    prompt_message = [('class:prompt', 'GenexPlus >'),
                      ]
    user_input = prompt(prompt_message,
                        history=FileHistory('history.txt'),
                        auto_suggest=AutoSuggestFromHistory(),
                        completer=GPCompleter,
                        style=style
                        )
    # click.echo_via_pager(user_input)  # replaces the print statement via pager
    message = ''

    if user_input is not None:
        args = user_input.split()

        if args[0] == 'load':
            if len(args) != 2:
                message = gp.invalid_load_prompt()
            else:
                message = gp.do_load(args[1])

        elif args[0] == 'exit':
            message = FormattedText([
                ('class:normal', 'Exited'),
            ])

            print_formatted_text(message, style=style)
            break

        # if args[0] == 'example':


    print_formatted_text(message, style=style)

    #TODO progress bar