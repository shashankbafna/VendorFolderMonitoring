import sys
import argparse
import boto3
import subprocess
import logging
from functools import wraps

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
def main():
    parser = argparse.ArgumentParser(description="Wrapper script for various operations.")
    parser.add_argument("sid", nargs="?", help="SID of the operation")
    parser.add_argument("emailid", nargs="?", help="Email ID of the user")
    parser.add_argument("operation", nargs="?", choices=["add", "subtract", "exec", "s3cp"], help="Operation to perform")
    parser.add_argument("operation_args", nargs="*", help="Key value pairs for the operation")
    parser.add_argument("--loglevel", default="INFO", help="Set the logging level (default: INFO)")

    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.loglevel.upper(), logging.INFO), format="%(asctime)s - %(levelname)s - %(message)s")
    logging.debug(f"Parsed arguments: {args}")

    if not (args.sid and args.emailid and args.operation):
        args.sid = args.sid or input("Enter SID: ")
        args.emailid = args.emailid or input("Enter Email ID: ")
        if not args.operation:
            print("Valid operations: add, subtract, exec, s3cp")
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
