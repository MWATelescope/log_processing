from collections import defaultdict


class Handler():
    def __init__(self):
        self.rules = defaultdict(dict)
        self.startup_functions = []
        self.shutdown_functions = []

    def on_startup(self, func):
        self.startup_functions.append(func)

    def startup(self, repository):
        for func in self.startup_functions:
            func(repository)

    def on_shutdown(self, func):
        self.shutdown_functions.append(func)

    def shutdown(self, repository):
        for func in self.shutdown_functions:
            func(repository)

    def add_rule(self, file_pattern, line_pattern):
        def decorator(func):
            self.rules[file_pattern][line_pattern] = func
            return func
        return decorator