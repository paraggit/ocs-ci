import logging
import time


log = logging.getLogger(__name__)
from ocs_ci.resiliency.cluster_failures import get_cluster_object


class NetworkFailures:
    SCENARIO_NAME = "NETWORK_FAILURES"

    def __init__(self, failure_data):
        self.scenario_name = NetworkFailures.SCENARIO_NAME
        self.failure_data = failure_data
        self.cluster_obj = get_cluster_object()

    def failure_case(self):
        # Get the first failure case key
        return list(self.failure_data.keys())[0]

    def run(self):
        """Dynamically call the appropriate method based on failure case."""
        case = self.failure_case()

        # Dictionary to map failure cases to corresponding methods
        failure_methods = {
            "POD_NETWORK_FAILURE": self._run_pod_network_failures,
            "NODE_NETWORK_DOWN": self._run_node_network_failure,
        }

        # Dynamically call the appropriate method based on the failure case
        if case in failure_methods:
            failure_methods[case]()  # Call the appropriate method
        else:
            raise NotImplementedError(
                f"Failure method for case '{case}' is not implemented."
            )

    def _run_pod_network_failures(self):
        """Handle Pod Network Failure scenario"""
        log.info("Bringing Down Pod Network Interface.")

    def _run_node_network_failure(self):
        """Handle Node Network Failure scenario"""
        log.info("Bringing Down Node Network.")
        for node_type in ["master", "worker"]:
            node_ip = self.cluster_obj.random_node_ip(node_type)
            self.cluster_obj.change_node_network_interface_state(
                node_ip=node_ip, node_type=node_type, connect=False
            )
            time.sleep(60)  # Simulate network being down
            self.cluster_obj.change_node_network_interface_state(
                node_ip=node_ip, node_type=node_type, connect=True
            )
