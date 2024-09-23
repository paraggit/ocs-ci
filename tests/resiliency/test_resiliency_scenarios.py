from ocs_ci.resiliency.resiliency_helper import Resiliency

# from ocs_ci.resiliency.resiliency_workload import workload_object
import logging
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


class TestResiliencyScenarios:
    def test_resiliency_node_failure_scenario(
        self, multi_pvc_factory, fio_resiliency_workload
    ):
        """ """
        # Create pvcs with different access_modes
        size = 5
        access_modes = [constants.ACCESS_MODE_RWO]
        pvc_objs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            access_modes=access_modes,
            # access_mode_dist_ratio=[1, 1],
            size=size,
            num_of_pvc=2,
        )

        for pv_obj in pvc_objs:
            fio_resiliency_workload(pv_obj)

        scenario = "NODE_FAILURES"

        resiliency = Resiliency(scenario)

        resiliency.start()

        resiliency.cleanup()
