# Regex Log Parser
Python library for log parsing/processing. Allows the user to define regex rules which will be matched against filenames and lines within them, and simple functions to handle those lines. See the examples below for more information.

This was originally developed at [MWA Telescope](https://mwatelescope.org) in order to process a large amount of historical log data and gain useful insights form it. Following the success of the project we have open sourced and published it in the hope that it may be useful for somebody else.

## Installation
### Prerequisites
- Python >= 3.10

Install the package
```bash
pip install regex_log_parser
```

If you wish to use the included functionality for uploading data into a postgres database, install the extra dependencies like so:
```bash
pip install regex_log_parser[postgres]
```

## Usage
Create a file and import the LogProcessor class. Create an instance of this object then call the run method, passing in a directory containing some logs that you would like to process.

Two things are required to setup the processor. A rules dictionary and a handler object.

```python
from regex_log_parser import LogProcessor, HandlerBase

log_processor = LogProcessor(
    rules=rules,
    handler=handler,
    dry_run=False
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
    handler=handler,
    dry_run=False
)

log_processor.run('/path/to/my/logs')
```

## The Handler class
The library only stipulates that the handler object passed to the `LogProcessor` object is an instance of `HandlerBase`.

You should subclass from `HandlerBase` and add your own methods to handle the lines found by your rules.

Override the `startup` and `shutdown` methods in your handler class to run a function at the start and end of parsing, respectively.

### The PostgresHandler class
Or, if you wish to make use of the included `PostgresHandler` for uploading data into a PostgreSQL database, subclass from that instead.

The PostgresHandler object has the following constructor:
```python
class PostgresHandler(HandlerBase):
    def __init__(self, dsn: Optional[str] = None, connection: Optional[Connection] = None, setup_script: Optional[str] = None, BATCH_SIZE: int = 1000):
```
- dsn: optionally provide a dsn string which will be used to connect to an existing PostgreSQL database or;
- connection: optionally provide an existing psycopg3 connection. Useful for unit tests.
- setup_script: optionally provide the path to a SQL file in order to perform some database setup/cleanup in between runs.
- BATCH_SIZE: Execute database operations in batches of BATCH_SIZE, defaults to 1000.

In your handler functions, define a SQL string and args tuple, and pass them to the `queue_op` function. These should be setup according to the psycopg3 format, see the example above. If you wish to execute a database operation immediately, pass `run_now=True` to `queue_op`, otherwise, it will be added to a queue, and executed in sequence when the size of the queue reaches `BATCH_SIZE`.


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