#!/usr/bin/env python3
import argparse
import collections
import logging
import itertools
import os
import subprocess
import sys

from XXX import helper

# TODO: handle failed rebalance (add --resume)

class Error(Exception):
    """Base class for errors in this module."""


class DeployerFailedError(Error):
    """Raised when a deploy failed."""


class MissingBinaryError(DeployerFailedError):
    """A specific non-revoverable error."""

def _abort(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)
    sys.exit(1)


class _DeploymentStatus(collections.namedtuple("_DeploymentStatus", "ongoing done failed remarks")):

    @property
    def total(self):
        return self.ongoing + self.done + self.failed

    @property
    def description(self):
        if self.total:
            return "Deployment {:7.2%} done: {:^5} ongoing, {:^5} done, {:^5} failed".format(
                (self.done+self.failed)/self.total, self.ongoing, self.done, self.failed)
        else:
            return "No deployment in progress"

    @property
    def one_char(self):
        if not self.total:
            return " "
        elif self.ongoing:
            return "┅"
        elif self.failed:
            return "✗"
        else:
            assert not self.ongoing
            return "✓"



class Deployer:

    def __init__(self, cluster, json_path):
        self._json_path = json_path
        self.cluster = cluster

    def _run_assignments(self, args):
        raised = False
        command = [
            "kafka-reassign-partitions.sh",
            "--zookeeper", self.cluster.zookeepers,
            "--reassignment-json-file", self._json_path,
        ] + args
        try:
            res = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except FileNotFoundError:
            raise MissingBinaryError("kafka-reassign-partitions.sh is not on the PATH")
        except subprocess.CalledProcessError as e:
            raised = True
            stdout = e.stdout
        else:
            stdout = res.stdout
        stdout_str = stdout.decode("utf-8", errors="ignore")
        if "FailedException" in stdout_str or raised:
            # kafka-reassign-partitions.sh can fail and have an errorcode of 0
            stdout_indented = "\n>>> ".join(stdout_str.splitlines())
            raise DeployerFailedError("kafka-reassign-partitions.sh failed:" + stdout_indented)
        return res.stdout.decode("utf-8", errors="ignore")

    def deploy(self):
        self._run_assignments(args=["--execute"])

    def verify(self):
        stdout = self._run_assignments(args=["--verify"])
        ongoing = 0
        done = 0
        failed = 0
        mismatch = 0
        remarks = []
        for l in stdout.splitlines():
            if l.startswith("ERROR") and "don't match the list of replicas" in l:
                mismatch += 1
                remarks.append(l)
            elif "in progress" in l:
                ongoing += 1
            elif "completed successfully" in l:
                done += 1
            elif "failed" in l:
                failed += 1
        remarks.sort()
        # Mismatching list failures are actually in progress deployments
        ongoing += mismatch
        failed -= mismatch
        return _DeploymentStatus(ongoing=ongoing, done=done, failed=failed, remarks=remarks)


def _get_parser():
    parser = argparse.ArgumentParser(description="Reassigns Kafka partitions up to a max cost.")
    parser.add_argument("input_directory", help="Path of the assignments output")
    parser.add_argument("--deploy", action="store_true", help="Deploy needed changes")
    parser.add_argument("--status", action="store_true", help="Show the status of deployments")
    parser.add_argument("--wait", action="store_true", help="Wait for all deployments to be done")
    parser.add_argument("--verbose", "-v", default=1, action="count", help="Increases verbosity")
    parser.add_argument("--quiet", "-q", default=0, action="count", help="Decreases verbosity")
    return parser


def _print_status(deployer, one_char, description):
    print("{}  {:14} {}".format(one_char, str(deployer.cluster), description))


def main(argv):
    parser = _get_parser()
    args = parser.parse_args(argv)
    verbosity = args.verbose - args.quiet
    input_directory = args.input_directory
    show_status = args.status or args.wait

    if not os.path.isdir(args.input_directory):
        _abort("No such directory: \"%s\"" % input_directory)

    # Python has no ordered set so we use a list
    deployers = []
    for cluster in helper.KAFKA_CLUSTERS:
        assignment_json = os.path.join(input_directory, cluster.name + ".json")
        if os.path.isfile(assignment_json):
            deployers.append(Deployer(cluster, assignment_json))

    if args.deploy:
        for deployer in deployers:
            try:
                if verbosity >= 1:
                    _print_status(deployer, "⇅", "Requesting deployment")
                deployer.deploy()
            except Error as e:
                _abort("%s: Status check failed: %s" % (cluster, e))

    monitored_deployers = []
    if show_status:
        monitored_deployers = deployers
    while monitored_deployers:
        not_done_deployers = []
        for deployer in monitored_deployers:
            cluster = deployer.cluster
            try:
                status = deployer.verify()
            except MissingBinaryError as e:
                _abort("Failed: %s" % e)
            except Error as e:
                print("%s: Status check failed: %s" % (cluster, e), file=sys.stderr)
                continue
            if args.status or verbosity >= 1:
                _print_status(deployer, status.one_char, status.description)
                if verbosity >= 2:
                    for s in status.remarks:
                        _print_status(deployer, status.one_char, s)
            if status.ongoing:
                not_done_deployers.append(deployer)
        if args.wait:
            monitored_deployers = not_done_deployers
        else:
            monitored_deployers = []

def run():
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        pass
