"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import threading
import socket
import shutil
from pathlib import Path
from collections import deque
import click

# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info("Starting Manager - Host: %s, Port: %s, PWD: %s",
                    host, port, os.getcwd())

        self.manager_info = {
            "host": host,
            "port": port,
            "shutdown": False
        }
        self.workers_info = {
            "workers": {},
            "worker_count": 0,
            "last_heartbeat": {}
        }
        self.task_info = {
            "task_state": "begin",
            "map_tasks": deque(),
            "reduce_tasks": deque(),
            "num_map_tasks": 0,
            "num_reduce_tasks": 0
        }
        self.job_queue = deque()
        self.current_job = {}
        self.job_count = 0

        self.worker_lock = threading.Lock()
        self.job_lock = threading.Lock()

        heartbeat_thread = threading.Thread(target=self.listen_for_heartbeats)
        job_thread = threading.Thread(target=self.process_jobs)
        heartbeat_thread.start()
        job_thread.start()
        self.listen_for_messages()
        heartbeat_thread.join()
        job_thread.join()

    def listen_for_messages(self):
        """Listen for incoming messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.manager_info["host"], self.manager_info["port"]))
            sock.listen()
            sock.settimeout(1)

            while not self.manager_info["shutdown"]:
                try:
                    worker_socket, _ = sock.accept()
                except socket.timeout:
                    continue

                message_str = self.receive_message(worker_socket)
                if not message_str:
                    continue

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue

                self.handle_message(message_dict)

        LOGGER.info("Manager shutting down")

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
        if message_type == "register":
            self.handle_register(message_dict)
        elif message_type == "new_manager_job":
            self.handle_new_job(message_dict)
        elif message_type == "finished":
            self.handle_finished_task(message_dict)
        elif message_type == "shutdown":
            self.handle_shutdown()

    def handle_register(self, worker_info):
        """Handle worker registration."""
        worker_host = worker_info["worker_host"]
        worker_port = worker_info["worker_port"]
        worker_pid = len(self.workers_info["workers"])

        if (worker_host, worker_port) in self.workers_info["last_heartbeat"]:
            self.workers_info["last_heartbeat"][(
                worker_host, worker_port)] = time.time()
        else:
            self.workers_info["last_heartbeat"][(
                worker_host, worker_port)] = time.time()
            worker = {
                "host": worker_host,
                "port": worker_port,
                "state": "ready",
                "task": None
            }
            self.workers_info["workers"][worker_pid] = worker
            self.workers_info["worker_count"] += 1

        self.send_register_ack(worker_host, worker_port)
        LOGGER.info("Registered Worker %s:%s", worker_host, worker_port)

    def send_register_ack(self, worker_host, worker_port):
        """Send registration acknowledgement to the worker."""
        ack_message = {
            "message_type": "register_ack",
            "worker_host": worker_host,
            "worker_port": worker_port
        }
        self.send_message_to_worker(ack_message, worker_host, worker_port)

    def send_message_to_worker(self, message_dict, worker_host, worker_port):
        """Send a message to a worker."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((worker_host, worker_port))
                sock.sendall(json.dumps(message_dict).encode('utf-8'))
        except ConnectionRefusedError:
            LOGGER.error("Failed to send message to worker %s:%s",
                         worker_host, worker_port)

    def handle_new_job(self, job_info):
        """Handle new job submission."""
        with self.job_lock:
            job_id = self.job_count
            job_info["job_id"] = job_id
            self.job_count += 1
            self.job_queue.append(job_info)
            LOGGER.info("Added new job to the queue")

    def process_jobs(self):
        """Process jobs from the job queue."""
        while not self.manager_info["shutdown"]:
            if not self.job_queue:
                time.sleep(1)
                continue

            with self.job_lock:
                job = self.job_queue.popleft()
                self.current_job = job

            LOGGER.info("Processing job %s", job["job_id"])
            output_dir = Path(job["output_directory"])
            if output_dir.exists():
                shutil.rmtree(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

            prefix = f"mapreduce-shared-job{job['job_id']:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as temp_dir:
                LOGGER.info("Created temporary directory %s", temp_dir)
                self.handle_map_stage(job, temp_dir)
                self.handle_reduce_stage(job, temp_dir)

            LOGGER.info("Job %s completed", job["job_id"])

    def handle_map_stage(self, job, temp_dir):
        """Handle the map stage of a job."""
        LOGGER.info("Starting map stage for job %s", job["job_id"])
        self.task_info["task_state"] = "mapping"
        self.partition_map_tasks(job, temp_dir)
        self.wait_for_stage_completion("map")

    def handle_reduce_stage(self, job, temp_dir):
        """Handle the reduce stage of a job."""
        LOGGER.info("Starting reduce stage for job %s", job["job_id"])
        self.task_info["task_state"] = "reducing"
        self.partition_reduce_tasks(job, temp_dir)
        self.wait_for_stage_completion("reduce")

    def partition_map_tasks(self, job, temp_dir):
        """Partition the input files and create map tasks."""
        input_dir = job["input_directory"]
        num_mappers = job["num_mappers"]
        input_files = sorted(os.listdir(input_dir))
        files_per_task = len(input_files) // num_mappers

        for i in range(num_mappers):
            task_files = input_files[i * files_per_task:(
                i + 1) * files_per_task]
            if i == num_mappers - 1:
                task_files.extend(input_files[(i + 1) * files_per_task:])

            task = {
                "task_id": i,
                "input_paths": [
                    os.path.join(input_dir, file) for file in task_files],
                "executable": job["mapper_executable"],
                "output_directory": temp_dir,
                "num_partitions": job["num_reducers"]
            }
            self.task_info["map_tasks"].append(task)
            self.task_info["num_map_tasks"] += 1

    def partition_reduce_tasks(self, job, temp_dir):
        """Partition the intermediate files and create reduce tasks."""
        num_reducers = job["num_reducers"]
        intermediate_files = list(Path(temp_dir).glob("*"))
        files_per_task = len(intermediate_files) // num_reducers

        for i in range(num_reducers):
            task_files = intermediate_files[
                i * files_per_task: (i + 1) * files_per_task]
            if i == num_reducers - 1:
                task_files.extend(
                    intermediate_files[(i + 1) * files_per_task:])

            task = {
                "task_id": i,
                "input_paths": [str(file) for file in task_files],
                "executable": job["reducer_executable"],
                "output_directory": job["output_directory"]
            }
            self.task_info["reduce_tasks"].append(task)
            self.task_info["num_reduce_tasks"] += 1

    def wait_for_stage_completion(self, stage):
        """Wait for all tasks in a stage to complete."""
        while self.task_info[f"num_{stage}_tasks"] > 0:
            self.assign_tasks(stage)
            time.sleep(1)

    def assign_tasks(self, stage):
        """Assign tasks to available workers."""
        tasks = self.task_info[f"{stage}_tasks"]
        while tasks:
            worker_id = self.get_available_worker()
            if worker_id is None:
                break
            task = tasks.popleft()
            self.send_task_to_worker(task, worker_id)
            self.task_info[f"num_{stage}_tasks"] -= 1

    def get_available_worker(self):
        """Find an available worker."""
        for worker_id, worker in self.workers_info["workers"].items():
            if worker["state"] == "ready":
                return worker_id
        return None

    def send_task_to_worker(self, task, worker_id):
        """Send a task to a worker."""
        worker = self.workers_info["workers"][worker_id]
        worker["state"] = "busy"
        worker["task"] = task
        message = {
            "message_type": f"new_{task['task_id']}",
            **task
        }
        self.send_message_to_worker(message, worker["host"], worker["port"])

    def handle_finished_task(self, message):
        """Handle finished task notification from a worker."""
        worker_host = message["worker_host"]
        worker_port = message["worker_port"]

        with self.worker_lock:
            for worker in self.workers_info["workers"].values():
                if (worker["host"] == worker_host and
                        worker["port"] == worker_port):

                    worker["state"] = "ready"
                    worker["task"] = None
                    break

        LOGGER.info("Task finished by worker %s:%s", worker_host, worker_port)

    def listen_for_heartbeats(self):
        """Listen for worker heartbeat messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.manager_info["host"], self.manager_info["port"]))
            sock.settimeout(1)

            while not self.manager_info["shutdown"]:
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    continue

                try:
                    message_dict = json.loads(data.decode("utf-8"))
                    if message_dict["message_type"] == "heartbeat":
                        worker_host = message_dict["worker_host"]
                        worker_port = message_dict["worker_port"]
                        self.workers_info["last_heartbeat"][(
                            worker_host, worker_port)] = time.time()
                        LOGGER.debug(
                            "Received heartbeat from worker %s:%s",
                            worker_host,
                            worker_port
                        )
                except (json.JSONDecodeError, KeyError):
                    continue

    def handle_shutdown(self):
        """Handle shutdown command."""
        LOGGER.info("Received shutdown command")
        self.manager_info["shutdown"] = True
        self.shutdown_workers()

    def shutdown_workers(self):
        """Shutdown all registered workers."""
        for worker in self.workers_info["workers"].values():
            if worker["state"] != "dead":
                shutdown_message = {
                    "message_type": "shutdown"
                }
                self.send_message_to_worker(
                    shutdown_message, worker["host"], worker["port"])


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.temp_dir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
