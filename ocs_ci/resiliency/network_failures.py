import logging

log = logging.getLogger(__name__)


class NetworkFailures:
    def __init__(self, failure_data):
        self.failure_data = failure_data

    def failure_case(self):
        return self.failure_data["NETWORK_FAILURE"].keys()[0]

    def run(self):
        """ """
        if self.failure_case == "POD_NETWORK_FAILURE":
            self._run_pod_network_failures()
        elif self.failure_case == "NODE_NETWORK_DOWN":
            self._run_node_network_failure()
        else:
            raise NotImplementedError("Failure method is not Implimented")

    def _run_pod_network_failures(self):
        """ """
        log.info("Bringing Down Network Interface.")

    def _run_node_network_failure(self):
        log.info("Bringing Down Node Network ")
