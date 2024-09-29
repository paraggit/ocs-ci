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
        cephfs_pvc_objs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            access_modes=access_modes,
            size=size,
            num_of_pvc=2,
        )

        rbd_pvc_objs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_modes=access_modes,
            size=size,
            num_of_pvc=2,
        )

        # Starting Workload on the cluster
        for pv_obj in cephfs_pvc_objs + rbd_pvc_objs:
            # for pv_obj in rbd_pvc_objs :
            fio_resiliency_workload(pv_obj)

        scenario = "NODE_FAILURES"

        resiliency = Resiliency(scenario)

        resiliency.start()

        resiliency.cleanup()
