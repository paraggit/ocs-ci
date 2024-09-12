from ocs_ci.resiliency import run


class TestResiliencyScenarios:
    def test_resiliency_node_failure_scenario(self):
        """ """

        assert run.run(scenarios=["NODE_FAILURES", "NETWORK_FAILURES"])
