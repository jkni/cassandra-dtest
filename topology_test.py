from dtest import Tester
from tools import insert_c1c2, query_c1c2, no_vnodes, debug, since, new_node
from assertions import assert_almost_equal

import re
import time
from ccmlib.node import NodetoolError
from ccmlib.node import TimeoutError
from cassandra import ConsistencyLevel
from threading import Thread


class TestTopology(Tester):

    def do_not_join_ring_test(self):
        """
        @jira_ticket CASSANDRA-9034
        Check that AssertionError is not thrown on SizeEstimatesRecorder before node joins ring
        """
        cluster = self.cluster.populate(1)
        node1, = cluster.nodelist()

        node1.start(wait_for_binary_proto=True, join_ring=False,
                    jvm_args=["-Dcassandra.size_recorder_interval=1"])

        # initial delay is 30s
        time.sleep(40)

        node1.stop(gently=False)

    def simple_decommission_test(self):
        """
        @jira_ticket CASSANDRA-9912
        Check that AssertionError is not thrown on SizeEstimatesRecorder after node is decommissioned
        """
        cluster = self.cluster
        cluster.populate(3)
        cluster.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.size_recorder_interval=1"])
        [node1, node2, node3] = cluster.nodelist()

        # write some data
        node1.stress(['write', 'n=10K', '-rate', 'threads=8'])

        # Decommision node and wipe its data
        node2.decommission()
        node2.stop(wait_other_notice=True)

        time.sleep(10)

    @since('3.2')
    def forbid_intercluster_gossip_test(self):
        """
        @jira_ticket CASSANDRA-10111
        In the event that a node is bootstrapped into cluster B with a broadcast_address of a node in
        cluster A, nodes in cluster A should not add nodes in cluster B
        """
        cluster = self.cluster
        cluster.populate(3)
        [node1, node2, node3] = cluster.nodelist()

        # We want to split the CCM nodes into two distinct C* clusters - A and B.
        # We update seeds and cluster_name before updating configs on member nodes.
        cluster._config_options['cluster_name'] = 'A'
        cluster.seeds = [node1]
        cluster._update_config()
        node1.import_config_files()
        node2.import_config_files()

        cluster._config_options['cluster_name'] = 'B'
        cluster.seeds = [node3]
        cluster._update_config()
        node3.import_config_files()

        cluster.start(wait_for_binary_proto=True, wait_other_notice=False, no_wait=True)

        # Show each node's status for debugging
        self.show_status(node1)
        self.show_status(node2)
        self.show_status(node3)

        cluster_a_describe_out, _ = node1.nodetool('describecluster')
        cluster_b_describe_out, _ = node3.nodetool('describecluster')

        debug(cluster_a_describe_out)
        debug(cluster_b_describe_out)

        # Confirm that we have successfully produced two distinct clusters
        self.assertIn('Name: A', cluster_a_describe_out)
        self.assertIn('Name: B', cluster_b_describe_out)

        # Bootstrap node into B with broadcast_address set to address of node in A
        node4 = new_node(cluster)
        node4.set_configuration_options(values={'broadcast_address': node2.address()})
        try:
            node4.start()
        except NodeError:
            pass

        node4.watch_log_for('Handshaking version with /' + node3.address(), timeout=60)

        # Wait for gossip to propagate
        time.sleep(2)

        node1_status_out, _ = node1.nodetool('status')
        debug(node1_status_out)

        # Ensure that cluster A doesn't have metadata entries for nodes in cluster B
        self.assertNotIn(node3.address(), node1_status_out)

    @no_vnodes()
    def movement_test(self):
        cluster = self.cluster

        # Create an unbalanced ring
        cluster.populate(3, tokens=[0, 2**48, 2**62]).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.ONE)

        cluster.flush()

        # Move nodes to balance the cluster
        balancing_tokens = cluster.balanced_tokens(3)
        escformat = '%s'
        node1.move(escformat % balancing_tokens[0])  # can't assume 0 is balanced with m3p
        node2.move(escformat % balancing_tokens[1])
        node3.move(escformat % balancing_tokens[2])
        time.sleep(1)

        cluster.cleanup()

        # Check we can get all the keys
        for n in xrange(0, 10000):
            query_c1c2(session, n, ConsistencyLevel.ONE)

        # Now the load should be basically even
        sizes = [node.data_size() for node in [node1, node2, node3]]

        assert_almost_equal(sizes[0], sizes[1])
        assert_almost_equal(sizes[0], sizes[2])
        assert_almost_equal(sizes[1], sizes[2])

    @no_vnodes()
    def decommission_test(self):
        cluster = self.cluster

        tokens = cluster.balanced_tokens(4)
        cluster.populate(4, tokens=tokens).start()
        node1, node2, node3, node4 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 2)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.QUORUM)

        cluster.flush()
        sizes = [node.data_size() for node in cluster.nodelist() if node.is_running()]
        init_size = sizes[0]
        assert_almost_equal(*sizes)

        time.sleep(.5)
        node4.decommission()
        node4.stop()
        cluster.cleanup()
        time.sleep(.5)

        # Check we can get all the keys
        for n in xrange(0, 10000):
            query_c1c2(session, n, ConsistencyLevel.QUORUM)

        sizes = [node.data_size() for node in cluster.nodelist() if node.is_running()]
        three_node_sizes = sizes
        assert_almost_equal(sizes[0], sizes[1])
        assert_almost_equal((2.0 / 3.0) * sizes[0], sizes[2])
        assert_almost_equal(sizes[2], init_size)

    @no_vnodes()
    def move_single_node_test(self):
        """ Test moving a node in a single-node cluster (#4200) """
        cluster = self.cluster

        # Create an unbalanced ring
        cluster.populate(1, tokens=[0]).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.ONE)

        cluster.flush()

        node1.move(2**25)
        time.sleep(1)

        cluster.cleanup()

        # Check we can get all the keys
        for n in xrange(0, 10000):
            query_c1c2(session, n, ConsistencyLevel.ONE)

    @since('3.0')
    @no_vnodes()
    def decommissioned_node_cant_rejoin_test(self):
        '''
        @jira_ticket CASSANDRA-8801

        Test that a decommissioned node can't rejoin the cluster by:

        - creating a cluster,
        - decommissioning a node, and
        - asserting that the "decommissioned node won't rejoin" error is in the
        logs for that node and
        - asserting that the node is not running.
        '''
        rejoin_err = 'This node was decommissioned and will not rejoin the ring'
        try:
            self.ignore_log_patterns = list(self.ignore_log_patterns)
        except AttributeError:
            self.ignore_log_patterns = []
        self.ignore_log_patterns.append(rejoin_err)

        self.cluster.populate(3).start(wait_for_binary_proto=True)
        [node1, node2, node3] = self.cluster.nodelist()

        debug('decommissioning...')
        node3.decommission()
        debug('stopping...')
        node3.stop()
        debug('attempting restart...')
        node3.start()
        try:
            # usually takes 3 seconds, so give it a generous 15
            node3.watch_log_for(rejoin_err, timeout=15)
        except TimeoutError:
            # TimeoutError is not very helpful to the reader of the test output;
            # let that pass and move on to string assertion below
            pass

        self.assertIn(rejoin_err,
                      '\n'.join(['\n'.join(err_list)
                                 for err_list in node3.grep_log_for_errors()]))
        self.assertFalse(node3.is_running())

    @since('3.0')
    def crash_during_decommission_test(self):
        """
        If a node crashes whilst another node is being decommissioned,
        upon restarting the crashed node should not have invalid entries
        for the decommissioned node
        @jira_ticket CASSANDRA-10231
        """
        cluster = self.cluster
        cluster.populate(3).start(wait_other_notice=True)

        node1, node2 = cluster.nodelist()[0:2]

        t = DecommissionInParallel(node1)
        t.start()

        null_status_pattern = re.compile(".N(?:\s*)127\.0\.0\.1(?:.*)null(?:\s*)rack1")
        while t.is_alive():
            out = self.show_status(node2)
            if null_status_pattern.search(out):
                debug("Matched null status entry")
                break
            debug("Restarting node2")
            node2.stop(gently=False)
            node2.start(wait_for_binary_proto=True)

        debug("Waiting for decommission to complete")
        t.join()
        self.show_status(node2)

        debug("Sleeping for 30 seconds to allow gossip updates")
        time.sleep(30)
        out = self.show_status(node2)
        self.assertFalse(null_status_pattern.search(out))

    def show_status(self, node):
        out, err = node.nodetool('status')
        debug("Status as reported by node {}".format(node.address()))
        debug(out)
        return out


class DecommissionInParallel(Thread):

    def __init__(self, node):
        Thread.__init__(self)
        self.node = node

    def run(self):
        node = self.node
        mark = node.mark_log()
        try:
            out, err = node.nodetool("decommission")
            node.watch_log_for("DECOMMISSIONED", from_mark=mark)
            debug(out)
            debug(err)
        except NodetoolError as e:
            debug("Decommission failed with exception: " + str(e))
            pass
