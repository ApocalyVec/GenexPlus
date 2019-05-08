"""
prompt_toolkit: https://python-prompt-toolkit.readthedocs.io/en/master/pages/printing_text.html

open: will open existing workspace. If designated workspace doesn't exist, it will notify user a new
    ex: $ open my_project
show: show existing workspaces on disk
load: load target data set in to the workspace - put data set onto slaves and do the grouping
"""

from prompt_toolkit import prompt, print_formatted_text
from prompt_toolkit.formatted_text import FormattedText, PygmentsTokens
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter, Completion, Completer
import click
import fuzzyfinder
from prompt_toolkit.styles import Style
import pickle

# commands
from GenexPlusProject import GenexPlusProject

GPKeywords = ['load', 'group', 'cluster', 'query', 'plot', 'help', 'exit', 'open']

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

gp_project = None  # GenexPlus workspace  # TODO add workspace

while 1:


    if gp_project is not None:
        prompt_message = [('class:prompt', 'GenexPlus: ' + gp_project.get_project_name() + ' >'),
                          ]
    else:
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


        if args[0] == 'open':  # for user input 'open', open a existing or creat a new gp project
            if len(args) != 2:  # if wronge number of arguments is given
                err_msg = FormattedText([
                    ('class:error', 'Wrong number of arguments, please specify one file to open'),
                ])
                print_formatted_text(err_msg, style=style)
            else:
                try:
                    print("Opening " + args[1])
                    gp_project_file = open(args[1] + '.pickle', "rb")
                    gp_project = pickle.load(gp_project_file)
                except FileNotFoundError:
                    isCreateNewProject = prompt("Project " + args[1] + " does not exit, would you like to create a new GenexPlus Project? [y/n]")
                    if isCreateNewProject == 'y':
                        # creating new GenexPlus project
                        print("Creating " + args[1])
                        gp_project = GenexPlusProject(args[1])

                        gp_project_fn = args[1] + '.pickle'

                        gp_project_file = open(gp_project_fn, "wb")

                        pickle.dump(gp_project, gp_project_file)  # save gp_project object to gp_project_file
                    # else isCreateNewProject == 'n' or 'N' or 'No' or 'no':


        elif args[0] == 'load':  # for user input 'load'
            print("loading")
            # if gp_workspace == None:
            #     msg = FormattedText([
            #         ('class:error', 'No GenexPlus Workspace Opened, do you want to create to a new workspace'),
            #         # TODO
            #     ])

            # if len(args) != 2:
            #     message = gp.invalid_load_prompt()
            # else:
            #     message = gp.do_load(args[1])

        elif args[0] == 'exit':  # for user input 'exit'
            message = FormattedText([
                ('class:normal', 'Exited'),
            ])

            print_formatted_text(message, style=style)
            break

        # if args[0] == 'example':


    print_formatted_text(message, style=style)

    #TODO progress bar