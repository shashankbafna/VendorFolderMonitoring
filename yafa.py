"""
Wrapper Script for Operations and Script Execution

Description:
This script serves as a versatile wrapper to perform multiple operations (`add`, `subtract`, `exec`, `s3cp`) 
and execute shell or Python scripts with dynamic argument validation and input handling.

Usage Examples:
1. Perform addition:
   python wrapper_script.py 123 user@example.com add a=10 b=20 --loglevel DEBUG

2. Perform subtraction:
   python wrapper_script.py 123 user@example.com subtract a=30 b=15 --loglevel INFO

3. Execute a shell command:
   python wrapper_script.py 123 user@example.com exec command="ls -lart"

4. Copy a file to S3:
   python wrapper_script.py 123 user@example.com s3cp local_path=/path/to/file s3_uri=s3://bucket/key --loglevel DEBUG

5. Execute a configured script (e.g., shell):
   python wrapper_script.py 123 user@example.com script --script_name somescript.sh --script_args "-a abc-xyz -r abc-xyz,cbs-eqe"

6. Execute a configured script (e.g., Python):
   python wrapper_script.py 123 user@example.com script --script_name script.py --script_args "--si 123 --text sample-text"

Features:
- Supports operations with key-value arguments.
- Executes scripts with dynamic argument validation based on `SCRIPT_ARGUMENT_MAP`.
- Supports logging with adjustable log levels (default: INFO).
"""

import sys
import argparse
import boto3
import subprocess
import logging
from functools import wraps

SCRIPT_ARGUMENT_MAP = {
    "somescript.sh": {
        "type": "shell",
        "arguments": {
            "-a": ["abc-xyz"],
            "-r": ["abc-xyz", "cbs-eqe"]
        }
    },
    "script.py": {
        "type": "python",
        "arguments": {
            "--si": ["123"],
            "--text": ["sample-text"]
        }
    },
    "script123.py": {
        "type": "python",
        "arguments": {
            "--sda": ["13"],
            "--cas": ["a13"]
        }
    }
}

def log_wrapper(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"Entering {func.__name__}")
        logging.debug(f"Arguments: args={args}, kwargs={kwargs}")
        result = func(*args, **kwargs)
        logging.debug(f"Result from {func.__name__}: {result}")
        logging.info(f"Exiting {func.__name__}")
        return result
    return wrapper

@log_wrapper
def add(a, b):
    logging.debug(f"Adding {a} and {b}")
    return a + b

@log_wrapper
def subtract(a, b):
    logging.debug(f"Subtracting {b} from {a}")
    return a - b

@log_wrapper
def exec_command(**kwargs):
    command = kwargs.get("command")
    logging.debug(f"Executing command: {command}")
    if command:
        try:
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            logging.debug(f"Command output: {result.stdout}, Error: {result.stderr}")
            return result.stdout if result.returncode == 0 else result.stderr
        except Exception as e:
            logging.error(f"Error executing command: {e}")
            return f"Error executing command: {e}"
    return "No command provided"

@log_wrapper
def s3cp(**kwargs):
    local_path = kwargs.get("local_path")
    s3_uri = kwargs.get("s3_uri")
    logging.debug(f"Copying from {local_path} to {s3_uri}")
    if not local_path or not s3_uri:
        return "Both local_path and s3_uri are required for s3cp operation"
    
    s3_client = boto3.client('s3')
    try:
        bucket, key_prefix = s3_uri.replace("s3://", "").split("/", 1)
        logging.debug(f"S3 bucket: {bucket}, Key prefix: {key_prefix}")
        s3_client.upload_file(local_path, bucket, key_prefix)
        return f"File {local_path} successfully copied to {s3_uri}"
    except Exception as e:
        logging.error(f"Error copying file: {e}")
        return f"Error copying file: {e}"

@log_wrapper
def validate_arguments(operation, kwargs):
    logging.debug(f"Validating arguments for operation: {operation}, kwargs: {kwargs}")
    if operation in ["add", "subtract"]:
        if not ("a" in kwargs and "b" in kwargs):
            raise ValueError(f"Operation '{operation}' requires 'a' and 'b' arguments.")
    elif operation == "exec":
        if "command" not in kwargs:
            raise ValueError("'exec' operation requires 'command' argument.")
    elif operation == "s3cp":
        if not ("local_path" in kwargs and "s3_uri" in kwargs):
            raise ValueError("'s3cp' operation requires 'local_path' and 's3_uri' arguments.")

@log_wrapper
def execute_operation(operation, kwargs):
    logging.debug(f"Executing operation: {operation}, with kwargs: {kwargs}")
    operations = {
        "add": lambda kwargs: add(int(kwargs["a"]), int(kwargs["b"])),
        "subtract": lambda kwargs: subtract(int(kwargs["a"]), int(kwargs["b"])),
        "exec": exec_command,
        "s3cp": s3cp
    }
    if operation not in operations:
        raise ValueError(f"Invalid operation '{operation}'. Valid operations are: {', '.join(operations.keys())}.")
    return operations[operation](kwargs)

@log_wrapper
def fetch_script_inputs(script_name):
    script_details = SCRIPT_ARGUMENT_MAP.get(script_name)
    if not script_details:
        raise ValueError(f"Script {script_name} is not configured in the argument map.")

    logging.debug(f"Fetching inputs for script {script_name}: {script_details}")
    inputs = {}
    for option, allowed_values in script_details.get("arguments", {}).items():
        value = input(f"Enter value for {option} (allowed: {', '.join(allowed_values)}): ")
        if value not in allowed_values:
            logging.error(f"Invalid value '{value}' for option '{option}'.")
            raise ValueError(f"Invalid value for option '{option}'. Allowed values: {', '.join(allowed_values)}.")
        inputs[option] = value
    return inputs

@log_wrapper
def execute_script(script_name, script_args):
    script_details = SCRIPT_ARGUMENT_MAP.get(script_name)
    if not script_details:
        logging.error(f"Script {script_name} is not configured in the argument map.")
        return f"Script {script_name} cannot be executed as it is not configured."

    inputs = script_args or fetch_script_inputs(script_name)
    command_parts = []

    if script_details["type"] == "shell":
        command_parts.append(f"bash {script_name}")
    elif script_details["type"] == "python":
        command_parts.append(f"python3 {script_name}")
    else:
        logging.error("Unsupported script type.")
        return "Unsupported script type."

    for option, value in inputs.items():
        command_parts.append(f"{option} {value}")

    command = " ".join(command_parts)
    logging.debug(f"Constructed command: {command}")

    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        logging.debug(f"Script output: {result.stdout}, Error: {result.stderr}")
        return result.stdout if result.returncode == 0 else result.stderr
    except Exception as e:
        logging.error(f"Error executing script: {e}")
        return f"Error executing script: {e}"

@log_wrapper
def main():
    parser = argparse.ArgumentParser(description="Wrapper script for various operations and script execution.")
    parser.add_argument("sid", nargs="?", help="SID of the operation")
    parser.add_argument("emailid", nargs="?", help="Email ID of the user")
    parser.add_argument("operation", nargs="?", choices=["add", "subtract", "exec", "s3cp", "script"], help="Operation to perform")
    parser.add_argument("operation_args", nargs="*", help="Key value pairs for the operation")
    parser.add_argument("--loglevel", default="INFO", help="Set the logging level (default: INFO)")
    parser.add_argument("--script_name", help="Name of the script to execute")
    parser.add_argument("--script_args", help="Arguments to pass to the script")

    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.loglevel.upper(), logging.INFO), format="%(asctime)s - %(levelname)s - %(message)s")
    logging.debug(f"Parsed arguments: {args}")

    if args.operation == "script":
        if not args.script_name:
            logging.error("Script name must be provided for the 'script' operation.")
            sys.exit("Script name is required.")

        result = execute_script(args.script_name, args.script_args or {})
        print(result)
        return

    if not (args.sid and args.emailid and args.operation):
        args.sid = args.sid or input("Enter SID: ")
        args.emailid = args.emailid or input("Enter Email ID: ")
        if not args.operation:
            print("Valid operations: add, subtract, exec, s3cp, script")
            args.operation = input("Enter operation: ")

    if args.operation_args:
        kwargs = dict(pair.split("=") for pair in args.operation_args)
    else:
        kwargs = {}
        while True:
            pair = input("Enter key=value pair for operation (or press Enter to stop): ")
            if not pair:
                break
            key, value = pair.split("=")
            kwargs[key] = value

    logging.debug(f"Final arguments: SID={args.sid}, EmailID={args.emailid}, Operation={args.operation}, Kwargs={kwargs}")

    try:
        validate_arguments(args.operation, kwargs)
        result = execute_operation(args.operation, kwargs)
        print(result)
    except ValueError as e:
        logging.error(f"Error: {e}")

if __name__ == "__main__":
    main()
