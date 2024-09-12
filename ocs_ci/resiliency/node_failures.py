import logging

log = logging.getLogger(__name__)


class NodeFailures:
    def __init__(self, failure_data):
        self.failure_data = failure_data

    def failure_case(self):
        return self.failure_data["NODE_FAILURE"].keys()[0]

    def run(self):
        """ """
        if self.failure_case == "REBOOT_NODE_RANDOMLY":
            self._run_reboot_node()
        elif self.failure_case == "NODE_DRAIN":
            self._run_node_drain()
        else:
            raise NotImplementedError("Failure method is not Implimented")

    def _run_reboot_node(self):
        """ """
        log.info("Rebooting Node ....")

    def _run_node_drain(self):
        log.info("Draiing Node .... ")
