# log_processing
Python utility for log processing. Allows the user to define regex rules which match each line, and function handlers to process those lines.

## Usage
Create a file and import the LogProcessor class. Create an instance of this object then call the run method, passing in a directory containing some logs that you would like to process.

Two things are required to setup the processor. A rules dictionary and a handler object.

```python
from Processor import LogProcessor

log_processor = LogProcessor(
    rules=rules,
    handler=handler
)

log_processor.run('/path/to/my/logs')
```

### Rules
Rules is a standard python dictionary of the format:

```python
rules = {
    "file_regex": {
        "line_regex": "handler_function",
    }
}
```

Where:
- file_regex is some regex to match the name of a file,
- line_regex is some regex to match a line within the file,
- handler_function is the name of a function in your handler object which will be used to process the line.

### Handlers
The handler object should be subclassed from the HandlerBase class in handlers.py. Or, if you wish to parse your logs and upload into a Postgresql database, you can subclass from the PostgresHandler class.

The handler object should implement startup and shutdown methods. Which will be ran at the start and end of the processing run respectively. These can be used to perform some database setup or cleanup.

handler_functions will have the signature:

```python
def handler(self, file_path, line, match):
```

Where:
    - file_path is the path to the file of the current line
    - line is the line in the log file to be handled
    - match is the regex match group

When using the PostgresHandler, you can call self.queue_op(sql, params) in your handler functions to queue a database operation. By default this will run SQL operations in batches of 1000, you can customise this by passing the BATCH_SIZE parameter in the constructor to PostgresHandler. If you want to run a database operation immediately, call self.queue_op(sql, params, run_now=True) immediately.

### Full example
```python
from Processor import LogProcessor
from handler import PostgresHandler

class MyHandler(PostgresHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs):

    def startup(self):
        """Optionally run some setup"""
        pass

    def shutdown(self):
        """Optionally run some cleanup"""
        pass

    def my_handler(self, file_path, line, match):
        field_1 = match.group(1)
        field_2 = match.group(2)

        sql = """
            INSERT INTO my_table (field_1, field_2)
            VALUES
                (%s, %s);
        """
        params = (field_1, field_2)

        self.queue_op(sql, params, run_now=False)

rules = {
    'example\.log': {
        '(.*),(.*)': 'my_handler',
        '.*': 'skip'
    }
}

handler = MyHandler(
    dsn='user:pass@localhost:5432/test',
    setup_script='path/to/db_setup'
)

log_processor = LogProcessor(
    rules=rules,
    handler=handler
)

log_processor.run('/path/to/my/logs')
```


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