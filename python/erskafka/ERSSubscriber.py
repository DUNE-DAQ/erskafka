#!/usr/bin/env python3

import getpass
import os
import re
import socket
import threading
from collections.abc import Callable

import ers.issue_pb2 as ersissue
import google.protobuf.message as msg
from ers.utils import construct_logger
from kafka import KafkaConsumer


class ERSSubscriber:
    """Subscribes to the ERS topics in kafka."""

    def __init__(self, config: dict) -> None:
        """Set up the subscriber."""
        self.bootstrap = config["bootstrap"]
        if "group_id" in config:
            self.group = config["group_id"]
        else:
            self.group = ""
        self.timeout = config["timeout"]
        self.running = False
        self.functions = {}
        self.thread = threading.Thread(target=self.message_loop)
        self.log = construct_logger("ERSSubscriber")

    def default_id(self) -> str:
        """Get the subscriber default ID."""
        node = socket.gethostname()
        user = getpass.getuser()
        process = os.getpid()
        thread = threading.get_ident()
        return f"{node}-{user}-{process}-{thread}"

    def add_callback(
        self, function: Callable, name: str, selection: str = ".*"
    ) -> bool:
        """Add callback to the subscriber."""
        if name in self.functions:
            return False

        was_running = self.running
        if was_running:
            self.stop()

        prog = re.compile(selection)
        self.functions[name] = [prog, function]

        if was_running:
            self.start()
        return True

    def clear_callbacks(self) -> None:
        """Remove all callbacks from the subscriber."""
        if self.running:
            self.stop()
        self.functions.clear()

    def remove_callback(self, name: str) -> bool:
        """Remove callback from the subscriber."""
        if name not in self.functions.keys():
            return False

        was_running = self.running
        if was_running:
            self.stop()

        self.functions.pop(name)

        if was_running and len(self.functions) > 0:
            self.start()
        return True

    def start(self) -> None:
        """Start running the subscriber."""
        self.log.info("Starting run")
        self.running = True
        self.thread.start()

    def stop(self) -> None:
        """Stop running the subscriber."""
        self.running = False
        self.thread.join()

    def message_loop(self) -> None:
        """Loop for running the subscriber."""
        if self.group == "":
            group_id = self.default_id()
        else:
            group_id = self.group

        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap,
            group_id=group_id,
            client_id=self.default_id(),
            consumer_timeout_ms=self.timeout,
        )

        topics = ["ers_stream"]
        consumer.subscribe(["monitoring." + s for s in topics])

        self.log.info(
            "ID: %s running with functions: %s ", group_id, *self.functions.keys()
        )

        while self.running:
            try:
                message_it = iter(consumer)
                message = next(message_it)
                # timestamp = message.timestamp # noqa: ERA001
                key = message.key.decode("ascii")
                ## The key from the message is binary
                ## In order to correctly match an ascii regex, we have to convert

                for function in self.functions.values():
                    if function[0].match(key):
                        issue = ersissue.IssueChain()
                        issue.ParseFromString(message.value)
                        function[1](issue)

            except msg.DecodeError:
                self.log.exception("Could not parse message")
            except StopIteration:
                pass
            except Exception:
                self.log.exception()

        self.log.info("Stop")
