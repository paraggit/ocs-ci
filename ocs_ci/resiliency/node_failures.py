import logging

from ocs_ci.ocs.node import get_nodes


log = logging.getLogger(__name__)


class NodeFailures:
    def __init__(self, failure_data):
        self.failure_data = failure_data
        self.scenario_name = "NODE_FAILURES"

    def failure_case(self):
        return list(self.failure_data[self.scenario_name].keys())[0]

    def run(self):
        """ """
        if self.failure_case() == "REBOOT_NODE_RANDOMLY":
            self._run_reboot_node()
        elif self.failure_case() == "NODE_DRAIN":
            self._run_node_drain()
        else:
            raise NotImplementedError("Failure method is not Implimented")

    def _run_reboot_node(self):
        """ """
        log.info("Running Failure Case REBOOT_NODE_RANDOMLY .")

        # Get the node list
        for node_type in self.failure_data[self.scenario_name][self.failure_case()][
            "NODE_TYPE"
        ]:
            nodes = get_nodes(node_type, num_of_nodes=1)
            # node = random.choice(nodes)
            # nodes.restart_nodes(nodes=[node])
            log.info(f"Rebooting Node Type: {node_type} , Node Name: {nodes}")
            # node.reload()
            for node in nodes:
                node.restart_nodes(wait=True)
                log.info(f" Node Name: {node} Rebooted.")
        # gracefully_reboot_nodes()

        self._post_scenario_checks()

    def _run_node_drain(self):
        log.info("Draiing Node .... ")

        self._post_scenario_checks()

    def _post_scenario_checks(self):
        """ """
        log.info(" running Post scenario checks : {self.scenario_name}")

    def __del__(self):
        """ """
        pass
