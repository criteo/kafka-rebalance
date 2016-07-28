#!/usr/bin/env python3
from kafka import client as client_lib
from kafka import common as kafka_common
import collections
import hashlib
import sys

from XXX import topology
from XXX import discovery

class Error(Exception):
    """Base class for errors in this module."""


class BadBrokerIdError(Error):
    """Raised when a broker ID not know to the cluster is passed as an argument."""

class RefreshFailedError(Error):
    """Raised when Cluster fails refreshing metadata."""


class TopicPartition(collections.namedtuple("_TopicPartition", "topic partition")):

    @staticmethod
    def __int_to_bytes(n):
        """Converts an integer to a big-endian array of 8 bytes."""
        # This is similar to Java's ByteBuffer#putLong
        return n.to_bytes(8, "big")

    # TODO: This belongs more to assignments.py
    def broker_affinity(self, broker_id):
        """Returns a comparable key to sort brokers by affinity with this topic/partition."""
        # Effectively this implements a tuned version of RendezVous hashing.
        # See https://en.wikipedia.org/wiki/Rendezvous_hashing
        # Our deployment relies on cross-partition redundancy. So we want to avoid
        # that partitions share the same brokers, even on topics with few partitions.
        # To achieve that, we make it so that consecutive four partitions have opposite
        # preferences.
        partition_group = self.partition // 4
        partition_offset = self.partition % 4
        md5 = hashlib.md5()  # select a specific hash function for reproducibility
        md5.update(self.topic)
        md5.update(self.__int_to_bytes(partition_group))
        md5.update(self.__int_to_bytes(broker_id))
        digest = bytearray(md5.digest())

        # If partition is odd, reverse order.
        if partition_offset == 0:
            pattern = 0b00000000
        elif partition_offset == 1:
            pattern = 0b10101010
        elif partition_offset == 2:
            pattern = 0b01010101
        elif partition_offset == 3:
            pattern = 0b11111111
        for n in range(len(digest)):
            digest[n] ^= pattern
        return (digest, broker_id)



class KafkaCluster(collections.namedtuple("_KafkaCluster", "dc type_")):

    _MAX_BROKER_ID = 1000

    @property
    def bootstrap_hosts(self):
        # This will change soon to kafka-broker-[central|local]:
        return discovery.Discovery().getHostPorts('kafka-broker-%s' % self.type_.value, self.dc)

    def __str__(self):
        return self.name

    @property
    def name(self):
        return "%s-%s" % (self.dc.value, self.type_.value)

    @property
    def num_brokers(self):
        return self.num_bad_brokers + self.num_good_brokers

    @property
    def num_bad_brokers(self):
        return len(self.bad_broker_ids)

    @property
    def num_good_brokers(self):
        return len(self.good_broker_ids)

    @property
    def zookeepers(self):
        type_str = self.type_.value
        if self.type_ == topology.KafkaClusterType.LOCAL:
            type_str= ""
        pattern = "zk{type_}{i:02}-{dc}.XXX"
        hosts = ",".join([pattern.format(type_=type_str, i=i, dc=self.dc.value)
                          for i in range(1,3)])
        return "{hosts}/kafka_{type_}".format(hosts=hosts, type_=self.type_.value)

    def refresh_metadata(self):
        """Returns a dict of dicts: topic name to partition to PartitionMetadata."""
        client = None
        try:
            client = client_lib.KafkaClient(
                hosts=self.bootstrap_hosts,
                client_id="kafka_rebalance",
            )
            client.load_metadata_for_topics()
            self.brokers, self.topic_partitions = client.brokers, client.topic_partitions
        except kafka_common.KafkaError as e:
            raise RefreshFailedError(e)
        finally:
            if client:
                client.close()
        broker_ids = self.brokers.keys()
        self.bad_broker_ids = {b_id for b_id in broker_ids if b_id > self._MAX_BROKER_ID}
        self.good_broker_ids = {b_id for b_id in broker_ids if b_id < self._MAX_BROKER_ID}
        self.partitions_metadata = sorted(self._enumerate_partitions_metadata())
        self.topic_partitions = [TopicPartition(pm.topic, pm.partition)
                                 for pm in self.partitions_metadata]
        self.current_assignments = {TopicPartition(pm.topic, pm.partition): [pm.leader] + list(pm.replicas)
                                    for pm in self.partitions_metadata}

    def blacklist_brokers(self, brokers_list):
        brokers_blacklist = frozenset(brokers_list)
        unknown_brokers = brokers_blacklist - frozenset(self.brokers)
        if unknown_brokers:
            names = ", ".join(str(b_id) for b_id in unknown_brokers)
            raise BadBrokerIdError("Unknown broker(s): %s" % names)
        self.bad_broker_ids.update(brokers_blacklist)
        self.good_broker_ids.difference_update(brokers_blacklist)

    def _enumerate_partitions_metadata(self):
        for topic, topic_metadata in self.topic_partitions.items():
            for partition, partition_metadata in topic_metadata.items():
                yield partition_metadata

    @staticmethod
    def fromString(cluster_name):
        dc, type_ = cluster_name.split('-')
        return KafkaCluster(topology.Dc(dc), topology.KafkaClusterType(type_))


_LOCAL_CLUSTER_DC = topology.Dc
_CENTRAL_CLUSTER_DC = [topology.Dc.XXX]
KAFKA_CLUSTERS = [KafkaCluster(dc, type_=topology.KafkaClusterType.LOCAL) for dc in _LOCAL_CLUSTER_DC]
KAFKA_CLUSTERS += [KafkaCluster(dc, type_=topology.KafkaClusterType.CENTRAL) for dc in _CENTRAL_CLUSTER_DC]
KAFKA_CLUSTERS_NAMES = [c.name for c in KAFKA_CLUSTERS]


def abort(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr)
    sys.exit(1)
