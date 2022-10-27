# log_processing
Python utility for log processing. Allows the user to define regex rules which match each line, and function handlers to process those lines.

## Rules
Define a dictionary in rules.py of the format:

```python
rules = {
    "file_regex": {
        "line_regex": "handler",
    }
}
```

Where:
- file_regex is some regex to match the name of a file,
- line_regex is some regex to match a line within the file,
- handler is the name of a function in handlers.py which will be used to process the line.

## Handlers
Handlers should be defined in handlers.py

Handlers by default will have the signature

```python
def handler(repository, line, match):
```

Where:
- repository is the repository object, which exposes the interface to the database.
- line is the line in the log file to be handled
- match is the regex match group

To make database calls in your handler, use repository.queue_op(sql, params). See the code for examples.

There are some special handlers

### on_start
If you wish to run some code at the start of the run, define a handler called on_start. This can be used to clear out data from previous runs.

### on_finish
Run some code after logs have been processed. Can be used to perform further database cleanup or analysis.

## Usage
```txt
python main.py [OPTIONS]

OPTIONS:
    --log_path      Path to a directory contain the logs to process. Defaults to ../logs
    --cfg           Path to a config file. The config file should contain information about the local database. (defaults to ../cfg/config.cfg)
    --dry_run       Do not run the function handler for each line, just log it instead (default False)
    --verbose       Enable verbose logging (default True)
    --setup_script  Path to a SQL file which will be used to configure the database prior to processing. (default ../db/setup.sql)
```

## Configuration
The config file (supplied by the --cfg option described above) should be in the standard Python config file format. Use the template below.
```txt
[database]
host=
port=
db=
user=
pass=
```

## Installation
This package requires python >= 3.10

Using pyenv:
```bash
pyenv install 3.10.8
```

Setting up a virtual environment:
```bash
pyenv virtualenv 3.10.8 logs
```

Using the above virtual environment (should be ran in the project root directory)
```bash
pyenv local logs
```

Then, install pipenv, and install all dependencies. Will look for the Pipfile in the project root directory.
```bash
pip install pipenv
pipenv install
```

## Running the program
The entrypoint for the program is src/main.py
```bash
cd src
python main.py [OPTIONS]
```