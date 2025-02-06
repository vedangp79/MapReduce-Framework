"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import socket
import threading
import hashlib
import tempfile
import shutil
import heapq
from contextlib import ExitStack
import subprocess
import click

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting Worker - Host: %s, Port: %s, Manager: %s:%s",
            host, port, manager_host, manager_port
        )

        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.shutdown_flag = False
        self.registered = False

        heartbeat_thread = threading.Thread(target=self.send_heartbeats)
        heartbeat_thread.start()
        self.register_with_manager()
        self.listen_for_messages()
        heartbeat_thread.join()

    def listen_for_messages(self):
        """Listen for incoming messages from the manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)

            while not self.shutdown_flag:
                try:
                    manager_socket, _ = sock.accept()
                except socket.timeout:
                    continue

                message_str = self.receive_message(manager_socket)
                if not message_str:
                    continue

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue

                self.handle_message(message_dict)

    def receive_message(self, sock):
        """Receive a message from a socket."""
        chunks = []
        while True:
            try:
                data = sock.recv(4096)
                if not data:
                    break
                chunks.append(data)
            except socket.timeout:
                break
        if not chunks:
            return None
        message_bytes = b''.join(chunks)
        return message_bytes.decode('utf-8')

    def handle_message(self, message_dict):
        """Handle incoming messages based on their type."""
        message_type = message_dict["message_type"]
        if message_type == "register_ack":
            self.registered = True
            LOGGER.info("Registration acknowledged by the manager")
        elif message_type == "new_map_task":
            self.handle_map_task(message_dict)
        elif message_type == "new_reduce_task":
            self.handle_reduce_task(message_dict)
        elif message_type == "shutdown":
            self.handle_shutdown()

    def register_with_manager(self):
        """Register the worker with the manager."""
        LOGGER.info("Registering with the manager")
        message = {
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port
        }
        self.send_message_to_manager(message)

    def send_message_to_manager(self, message_dict):
        """Send a message to the manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            sock.sendall(json.dumps(message_dict).encode('utf-8'))

    def send_heartbeats(self):
        """Send heartbeat messages to the manager periodically."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # The socket is now created once, outside the while loop
            while not self.shutdown_flag:
                    heartbeat_message = {
                        "message_type": "heartbeat",
                        "worker_host": self.host,
                        "worker_port": self.port
                    }
                    # Serialize the heartbeat message
                    message = json.dumps(heartbeat_message)
                    # Send the heartbeat message to the manager
                    sock.sendall(message.encode('utf-8'), (
                        self.manager_host, self.manager_port))
                    LOGGER.debug("Sent heartbeat to the manager")
                    time.sleep(2)

    def handle_map_task(self, task):
        """Handle a map task."""
        LOGGER.info("Received map task: %s", task)

        with tempfile.TemporaryDirectory(
                prefix=f"mapper_{task['task_id']}_") as temp_dir:
            input_files = task["input_paths"]
            executable = task["executable"]
            num_partitions = task["num_partitions"]

            with ExitStack() as stack:
                input_handles = [
                    stack.enter_context(open(file, 'r', encoding='utf-8'))
                    for file in input_files
                ]
                tid = task['task_id']
                output_handles = [
                    stack.enter_context(open(
                        os.path.join(temp_dir,
                                     f"maptask{tid:05d}-part{i:05d}"),
                        'w', encoding='utf-8'))
                    for i in range(num_partitions)
                ]

                for input_handle in input_handles:
                    with subprocess.Popen(
                            [executable], stdin=input_handle,
                            stdout=subprocess.PIPE, text=True) as proc:
                        for line in proc.stdout:
                            key = line.split('\t')[0]
                            partition = int(hashlib.md5(
                                key.encode('utf-8')).hexdigest(),
                                            16) % num_partitions
                            output_handles[partition].write(line)

            for output_handle in output_handles:
                output_handle.close()
                self.sort_file(output_handle.name)

            for filename in os.listdir(temp_dir):
                shutil.move(os.path.join(temp_dir, filename),
                            task["output_directory"])

        self.send_task_finished(task["task_id"])

    def sort_file(self, filename):
        """Sort the lines in a file."""
        with open(filename, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        lines.sort()
        with open(filename, 'w', encoding='utf-8') as file:
            file.writelines(lines)

    def handle_reduce_task(self, task):
        """Handle a reduce task."""
        LOGGER.info("Received reduce task: %s", task)

        with tempfile.TemporaryDirectory(
                prefix=f"reducer_{task['task_id']}_") as temp_dir:
            input_files = task["input_paths"]
            executable = task["executable"]
            output_dir = task["output_directory"]

            with ExitStack() as stack:
                input_handles = [
                    stack.enter_context(open(file, 'r', encoding='utf-8'))
                    for file in input_files
                ]
                merged_input = heapq.merge(*input_handles)

                output_filename = os.path.join(temp_dir,
                                               f"part-{task['task_id']:05d}")
                with open(
                    output_filename, 'w', encoding='utf-8'
                ) as output_handle:
                    with subprocess.Popen(
                        [executable], stdin=subprocess.PIPE,
                        stdout=output_handle, text=True
                    ) as proc:
                        for line in merged_input:
                            proc.stdin.write(line)
                        proc.stdin.close()

            output_path = os.path.join(output_dir,
                                       f"part-{task['task_id']:05d}")
            shutil.move(output_filename, output_path)

        self.send_task_finished(task["task_id"])

    def send_task_finished(self, task_id):
        """Send a task finished message to the manager."""
        message = {
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port
        }
        self.send_message_to_manager(message)

    def handle_shutdown(self):
        """Handle a shutdown message from the manager."""
        LOGGER.info("Received shutdown message from the manager")
        self.shutdown_flag = True


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Worker:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
