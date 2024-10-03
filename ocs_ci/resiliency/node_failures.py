import logging
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.resiliency.cluster_failures import get_cluster_object

log = logging.getLogger(__name__)


class NodeFailures:
    SCENARIO_NAME = "NODE_FAILURES"

    def __init__(self, failure_data):
        self.failure_data = failure_data
        self.failure_case_name = self._get_failure_case()
        self.scenario_name = NodeFailures.SCENARIO_NAME
        self.cluster_obj = get_cluster_object()

    def _get_failure_case(self):
        """Retrieve the failure case name from the provided failure data."""
        try:
            return next(
                iter(self.failure_data)
            )  # More efficient way to get the first key
        except Exception as e:
            log.error(f"Error parsing the failure_data: {e}")
            return None

    def run(self):
        """Run the failure scenario based on the failure case."""
        if not self.failure_case_name:
            log.error("No valid failure case name found. Exiting run method.")
            return

        failure_methods = {
            "REBOOT_NODE_RANDOMLY": self._run_reboot_node,
            "NODE_DRAIN": self._run_node_drain,
        }

        # Run the failure method dynamically if it exists
        failure_method = failure_methods.get(self.failure_case_name)
        if failure_method:
            failure_method()
            self._post_scenario_checks()
        else:
            raise NotImplementedError(
                f"Failure method for {self.failure_case_name} is not implemented."
            )

    def _run_reboot_node(self):
        """Simulate the reboot of nodes."""
        log.info("Running Failure Case: REBOOT_NODE_RANDOMLY.")
        for node_type in self.failure_data[self.failure_case_name].get("NODE_TYPE", []):
            for _ in range(2):  # Reboot 2 nodes
                log.info(f"Rebooting {node_type} node.")
                self.cluster_obj.reboot_node(node_type=node_type)

    def _run_node_drain(self):
        """Simulate draining of nodes."""
        log.info("Running Failure Case: NODE_DRAIN.")
        log.info("Draining Node...")

    def _post_scenario_checks(self):
        """Perform post-scenario checks to ensure the cluster is healthy."""
        log.info(f"Running Post scenario checks for {self.scenario_name}.")
        log.info("Verifying that Ceph health is OK (retrying if necessary).")
        ceph_health_check(tries=45, delay=60)
