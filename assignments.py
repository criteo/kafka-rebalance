#!/usr/bin/env python3
import argparse
import collections
import hashlib
import heapq
import logging
import itertools
import json
import math
import os
import statistics
import subprocess
import sys
import tempfile

from XXX import helper


class Error(Exception):
    """Base class for errors in this module."""


class Change(collections.namedtuple("_Change", "topic partition replicas cost reason")):

    def __str__(self):
        return "{topic},{partition} ⇨ {replicas} (cost {cost}): {reason}".format(
            topic=self.topic_utf8, partition=self.partition, replicas=self.replicas,
            cost=self.cost, reason=self.reason,
        )

    @property
    def topic_utf8(self):
        return self.topic.decode("utf8")


    def sort_key(self):
        """Helps sorting changes so they happen in phases and across topics."""
        # Self is present on the key so that the order is fully deterministic
        # Seed is present on the key so as to spread updates across topics
        seed = hash("%s-%s" % (self.topic, self.partition))
        return (self.cost, len(self.replicas), self.partition, seed, self.topic, self)


def generate_assignments(cluster):
    assignments = {}
    for tp in cluster.topic_partitions:
        best_brokers = heapq.nlargest(3, cluster.good_broker_ids, key=tp.broker_affinity)
        assignments[tp] = best_brokers
    return assignments


class PrettyPrinter:
    # "pretty" printer means the implementations is ugly

    HEADER_LENGTH = 30

    def __init__(self, cluster, assignments, output_file):
        self._assignments = assignments
        self._num_brokers = cluster.num_good_brokers
        self._broker_ids = cluster.good_broker_ids
        self._output_file = output_file

    def _get_mean_and_spread(self, numbers):
        mean = math.ceil(statistics.mean(numbers))
        spread_min = mean - min(numbers)
        spread_max = max(numbers) - mean
        return mean, max(spread_min, spread_max)

    def _make_per_broker_counter(self):
        return [0] * self._num_brokers

    def _print_header(self, content=""):
        content_len = self.HEADER_LENGTH - 3 # We add three whitespaces
        if len(content) > content_len:
            content = "…" + content[-content_len+1:]
        elif len(content) < content_len:
            content = content.rjust(content_len)
        self._print(content, end="   ")

    def _print_count_list(self, count_list, name=""):
        self._print_header(name)
        for count in count_list:
            self._print("{:^3} ".format(count))
        mean, spread = self._get_mean_and_spread(count_list)
        self._print("\u250A {:^3}±{:}".format(mean, spread))
        self._printl()

    def _print(self, *args, **kwargs):
        kwargs.setdefault("end", "")
        kwargs.setdefault("file", self._output_file)
        print(*args, **kwargs)

    def _printl(self, *args, **kwargs):
        kwargs.setdefault("end", "\n")
        self._print(*args, **kwargs)

    def _print_broker(self, leader=False, replica=False):
        if leader:
            char = "\u2588"
        elif replica:
            char = "\u2592"
        else:
            char = "\u2014"
        self._print(char*3, end="-")

    def _print_separator(self):
        self._print_header()
        self._printl("\u2550" * (4 * self._num_brokers))

    def _format_topic_partition(self, topic_partition):
        topic_str = topic_partition.topic.decode("utf-8", errors="ignore")
        return "{:}/{:04}".format(topic_str, topic_partition.partition)

    def _print_broker_names(self):
        self._print_header("BROKERS ")
        for broker_id in self._broker_ids:
            self._print("{:^3} ".format(broker_id))

    def dump(self):
        prev_topic = None
        topic_partitions_per_broker = None
        partitions_per_broker = self._make_per_broker_counter()
        leaders_per_broker = self._make_per_broker_counter()

        self._print_broker_names()
        for tp, best_brokers in sorted(self._assignments.items()):
            leader = best_brokers[0]
            if prev_topic != tp.topic:
                if prev_topic:
                    self._print_count_list(topic_partitions_per_broker)
                topic_partitions_per_broker = self._make_per_broker_counter()
            prev_topic = tp.topic
            self._print_header(self._format_topic_partition(tp))
            for index, broker in enumerate(self._broker_ids):
                if broker in best_brokers:
                    partitions_per_broker[index] += 1
                    topic_partitions_per_broker[index] += 1
                    if broker == leader:
                        leaders_per_broker[index] += 1
                        self._print_broker(leader=True)
                    else:
                        self._print_broker(replica=True)
                else:
                    self._print_broker(replica=False)
            self._printl()
        if topic_partitions_per_broker:
            self._print_count_list(topic_partitions_per_broker)
        self._print_separator()
        self._print_count_list(partitions_per_broker, "TOTAL PARTITIONS")
        self._print_count_list(leaders_per_broker, "TOTAL LEADERS")


class ChangeScheduler:
    """Given assignments of topic partitions to broker, decides which are safe to apply.

    This class has one public method: schedule(). Other methodes are helpers for it.
    """

    def __init__(self, cluster):
        self._partitions_metadata = cluster.partitions_metadata
        self._cluster = cluster

    def _pad_isr(self, isr, replicas):
        """Completes isr with replicas if there are less then 3 ISR.

        Args:
          isr: An iterable of in sync replicas.
          replicas: An iterable of replicas.

        Returns:
          A list of at replicas from isr plus enough replicas from replicas so that there are 3.
        """
        used = set(isr)
        result = list(isr)
        for r in replicas:
            if r not in used:
                used.add(r)
                result.append(r)
                if len(result) >= 3:
                    break
        return result

    def _generate_partition_change(self, partition_metadata, new_replicas):
        m = partition_metadata
        new_leader = new_replicas[0]
        not_isr = len(m.replicas) - len(m.isr)

        # Just to make sure we got the API right
        assert m.leader in m.isr
        assert m.leader in m.replicas

        if list(m.replicas) == list(new_replicas):
            return None
        elif frozenset(m.isr) == frozenset(new_replicas):
            # Leader should change but is already in sync
            msg = "Changing leader preference"
            cost = 0
        elif not_isr or len(m.isr) < 3:
            msg = "Some replicas are not ISR"
            new_replicas = self._pad_isr(m.isr, new_replicas)
            cost = len(new_replicas) - len(m.isr)
        elif new_leader not in m.replicas:
            msg = "Leader %d to %d" % (m.leader, new_leader)
            other_isr = [r for r in m.isr if r != new_leader]
            new_replicas = [new_leader] + other_isr[:2]
            cost = 1
        else:
            msg = "Moving away from old replicas"
            cost = len(frozenset(new_replicas) - frozenset(m.replicas))
        return Change(m.topic, m.partition, new_replicas, cost=cost, reason=msg)

    def _generate_changes(self, assignments):
        change_list = []
        for m in self._partitions_metadata:
            tp = helper.TopicPartition(m.topic, m.partition)
            change = self._generate_partition_change(m, assignments[tp])
            if change:
                change_list.append(change)
        change_list.sort(key=Change.sort_key)
        return change_list

    def _describe_changes(self, all_changes, kept_changes):
        total_cost = sum(c.cost for c in kept_changes)
        if not all_changes:
            desc = "No changes needed"
        elif not kept_changes:
            desc = "All changes where filtered out to respect cost"
        else:
            cheapest = kept_changes[0]
            most_expensive = kept_changes[-1]
            desc = "Costs range from %d (\"%s\") to %d (\"%s\")" % (
                cheapest.cost, cheapest.reason, most_expensive.cost, most_expensive.reason)
        return "%s: using %d brokers out of %d, total cost %d: %s" % (
            self._cluster, self._cluster.num_good_brokers, self._cluster.num_brokers, total_cost, desc)

    def _compute_topics_max_changes(self, ratio):
        counts = collections.defaultdict(int)
        for tp in self._cluster.topic_partitions:
            counts[tp.topic] += 1
        res = {k: v*ratio for k, v in counts.items()}
        return res

    def schedule(self, assignments, broker_max_cost, topic_max_ratio):
        all_changes = self._generate_changes(assignments)
        kept_changes = []

        total_max_cost = broker_max_cost * len(self._cluster.good_broker_ids)
        total_cost = 0
        topic_max_changes = self._compute_topics_max_changes(topic_max_ratio)
        topic_changes = collections.defaultdict(int)
        for c in all_changes:
            if total_cost + c.cost > total_max_cost:
                break
            if topic_changes[c.topic] + c.cost > topic_max_changes[c.topic]:
                continue
            total_cost += c.cost
            topic_changes[c.topic] += 1
            kept_changes.append(c)
        status = self._describe_changes(all_changes, kept_changes)
        return status, kept_changes


def changes_to_json(changes):
    json_partitions = []
    json_dict = {
        "partitions": json_partitions,
        "version": 1,
    }
    for c in changes:
        json_partitions.append({
            "topic": str(c.topic_utf8),
            "partition": c.partition,
            "replicas": c.replicas,
            })
    return json.dumps(json_dict, indent=2)


def _abort(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)
    sys.exit(1)


def _get_parser():
    parser = argparse.ArgumentParser(description="Reassigns Kafka partitions up to a max cost.")
    parser.add_argument("--output_directory", "-o", help="Name of the output directory", nargs="?")
    parser.add_argument("--status", action="store_true", help="Show the status of deployments")
    parser.add_argument("--verbose", "-v", default=1, action="count", help="Increases verbosity")
    parser.add_argument("--quiet", "-q", default=0, action="count", help="Decreases verbosity")
    parser.add_argument("--max-cost", "-c", metavar="CHANGES_PER_BROKER", type=int, default=5,
                        help="How many changes are allowed per broker ON AVERAGE")
    parser.add_argument("--no-max-cost", dest="max_cost", action="store_const", const=2**32,
                        help="Do not put a cap on average number of changes per broker")
    parser.add_argument("--max-topic-percent", "-p", metavar="TOPIC_PERCENTAGE", type=int, default=10,
                        help="How much of a given topic can change")
    parser.add_argument("--no-max-topic-percent", dest="max_topic_percent", action="store_const", const=2**32,
                        help="Do not put a cap on percentage of partition that can change in a topic")
    parser.add_argument("--cluster", dest="clusters", choices=helper.KAFKA_CLUSTERS_NAMES, action="append")


    for cluster in helper.KAFKA_CLUSTERS:
        parser.add_argument("--bad-brokers-" + cluster.name, metavar="BROKER_ID", nargs="+", type=int,
                            help="Do not use that broker ID for " + cluster.name)
    return parser


def main(argv):
    parser = _get_parser()
    args = parser.parse_args(argv)
    verbosity = args.verbose - args.quiet
    output_directory = args.output_directory

    if output_directory:
        os.makedirs(output_directory, exist_ok=True)

    clusters = helper.KAFKA_CLUSTERS
    if args.clusters is not None:
        clusters = [helper.KafkaCluster.fromString(c) for c in args.clusters]

    for cluster in clusters:
        logging.disable(logging.CRITICAL)
        try:
            cluster.refresh_metadata()
        except helper.Error as e:
            _abort("Failed to comunicate with cluster \"%s\": %s" % (cluster.name, e))
        finally:
            logging.disable(logging.NOTSET)
        blacklist_opt_name = "bad_brokers_" + cluster.name.replace("-", "_")
        blacklist = getattr(args, blacklist_opt_name) or []
        try:
            cluster.blacklist_brokers(blacklist)
        except helper.BadBrokerIdError as e:
            _abort("Invalid broker blacklist for cluster \"%s\": %s" % (cluster.name, e))

    for cluster in clusters:
        if verbosity > 1:
            bad_brokers_desc = ", ".join(str(b) for b in cluster.bad_broker_ids)
            print("%s: %d brokers ignoring brokers <%s>" % (
                cluster, cluster.num_good_brokers, bad_brokers_desc))
        if cluster.num_good_brokers < 3:
            if verbosity > 0:
                print("%s: IGNORED, only %d broker(s) (needs 3)" % (
                    cluster, cluster.num_good_brokers))
            continue

        assignments = generate_assignments(cluster)
        scheduler = ChangeScheduler(cluster)
        status, changes = scheduler.schedule(
            assignments,
            broker_max_cost=args.max_cost,
            topic_max_ratio=args.max_topic_percent/100,
        )

        if verbosity > 1:
            for c in changes:
                print("%s: Changing %s" % (cluster, c))
        if verbosity > 0:
            print(status)

        json_str = changes_to_json(changes)
        if output_directory:
            basename = os.path.join(output_directory, cluster.name)
            with open(basename + "-final-ascii.txt", "w") as f:
                PrettyPrinter(cluster, assignments, f).dump()
            with open(basename + "-current-ascii.txt", "w") as f:
                PrettyPrinter(cluster, cluster.current_assignments, f).dump()
            with open(basename + ".json", "w") as f:
                f.write(json_str)


def run():
    main(sys.argv[1:])
