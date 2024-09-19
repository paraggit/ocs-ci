from ocs_ci.resiliency.resiliency_helper import Resiliency
from ocs_ci.resiliency.resiliency_workload import workload_object
import logging

log = logging.getLogger(__name__)


class TestResiliencyScenarios:
    def test_resiliency_node_failure_scenario(self, pvc_factory):
        """ """
        pvc = pvc_factory()

        scenarios = ["NODE_FAILURES"]

        workload_data = {
            "name": "fio-workload-xyx",
            "namespace": f"{pvc.namespace}",
            "file_name": "fio_workload-xyz",
            "pvc_name": f"{pvc.name}",
            "replicas": 1,
        }

        workload = workload_object("FIO", workload_data)
        workload.start_workload()
        import time

        log.info("Workload Started...")
        time.sleep(10)

        resiliency = Resiliency(scenarios=scenarios)

        # Setup before starting
        assert resiliency.run_workload(pvc)

        # Start the failure injection process
        assert resiliency.start()

        # Cleanup after completion
        resiliency.cleanup()

        workload.stop()
