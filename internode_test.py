import re
import time

from dtest import Tester, debug
from ccmlib.node import NodetoolError
from threading import Thread


class TestInternode(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def crash_during_decommission_test(self):
        """
        If a node crashes whilst another node is being decommissioned,
        upon restarting the crashed node should not have invalid entries
        for the decomissioned node
        @jira_ticket CASSANDRA-10231
        """
        cluster = self.cluster
        cluster.populate(3).start(wait_other_notice=True)

        node1, node2 = cluster.nodelist()[0:2]

        t = DecomissionInParallel(node1)
        t.start()

        p = re.compile(".N(?:\s*)127\.0\.0\.1(?:.*)null(?:\s*)rack1")
        while t.is_alive():
            out = self.show_status(node2)
            if p.search(out):
                debug("Matched")
                break
            debug("Restarting node2")
            node2.stop(gently=False)
            node2.start(wait_for_binary_proto=True)

        debug("Waiting for decommission to complete")
        t.join()
        self.show_status(node2)

        debug("Sleeping for 30 seconds")
        time.sleep(30)
        out = self.show_status(node2)
        self.assertFalse(p.search(out))

    def show_status(self, node):
        out, err = node.nodetool('status')
        debug("Status as reported by node {}".format(node.address()))
        debug(out)
        return out


class DecomissionInParallel(Thread):
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
