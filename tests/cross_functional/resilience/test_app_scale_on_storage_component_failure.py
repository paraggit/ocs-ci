import logging
import pytest
import time

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    resiliency,
    polarion_id,
)
from ocs_ci.resiliency.resiliency_helper import Resiliency
from ocs_ci.helpers.vdbench_helpers import (
    get_default_vdbench_configs,
    create_temp_config_file,
)
from concurrent.futures import ThreadPoolExecutor


log = logging.getLogger(__name__)


@green_squad
@resiliency
class TestAppScaleOnStorageComponentFailure:
    def _prepare_pvcs_and_workloads(
        self, project_factory, multi_pvc_factory, resiliency_workload
    ):
        """
        Create a VDBENCH workload and scale it on certain frequency.

        Returns:
            list: List of workload objects
        """
        project = project_factory()
        size = 10
        interfaces = [constants.CEPHFILESYSTEM, constants.CEPHBLOCKPOOL]
        vdbench_configs = get_default_vdbench_configs()
        vdbench_config_file = create_temp_config_file(vdbench_configs["stress"])

        workloads = []
        for interface in interfaces:
            if interface == constants.CEPHFILESYSTEM:
                access_modes = [constants.ACCESS_MODE_RWX, constants.ACCESS_MODE_RWO]
            else:
                access_modes = [
                    f"{constants.ACCESS_MODE_RWO}-Block",
                    f"{constants.ACCESS_MODE_RWX}-Block",
                ]
            pvcs = multi_pvc_factory(
                interface=interface,
                project=project,
                access_modes=access_modes,
                size=size,
                num_of_pvc=4,
            )

            for pvc in pvcs:
                workload = resiliency_workload(
                    "VDBENCH", pvc, vdbench_config_file=vdbench_config_file
                )
                workload.start_workload()
                workloads.append(workload)

        scale_future = self._scale_workloads_background(workloads, delay=30)

        return workloads, scale_future

    def _scale_workloads_background(self, workloads, delay=30):
        """
        Scale the workloads in background using ThreadPoolExecutor.
        """

        def scale_after_delay():
            time.sleep(delay)
            for workload in workloads:
                log.info(f"Scaling workload {workload.deployment_name} in background")
                try:
                    workload.scale_workload(threads=4)
                except Exception as e:
                    log.error(
                        f"Failed to scale workload {workload.deployment_name}: {e}"
                    )

        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(scale_after_delay)
        return future

    def _validate_and_cleanup_workloads(self, workloads):
        """
        Validate workload results and stop/cleanup all workloads.
        """
        for workload in workloads:
            result = workload.get_fio_results()
            assert (
                "error" not in result.lower()
            ), f"Workload {workload.deployment_name} failed after failure injection"

        log.info("All workloads passed after failure injection.")

    @pytest.mark.parametrize(
        argnames=["scenario_name", "failure_case"],
        argvalues=[
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "OSD_POD_FAILURES",
                marks=polarion_id("OCS-6821"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "MGR_POD_FAILURES",
                marks=polarion_id("OCS-6823"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "MDS_POD_FAILURES",
                marks=polarion_id("OCS-6850"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "MON_POD_FAILURES",
                marks=polarion_id("OCS-6822"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "RGW_POD_FAILURES",
                marks=polarion_id("OCS-6808"),
            ),
            pytest.param(
                "STORAGECLUSTER_COMPONENT_FAILURES",
                "CEPHFS_POD_FAILURES",
                marks=polarion_id("OCS-6808"),
            ),
        ],
    )
    def test_storage_component_failure_scenarios(
        self,
        scenario_name,
        failure_case,
        project_factory,
        multi_pvc_factory,
        resiliency_workload,
    ):
        """
        Test that validates ODF platform resiliency under application component
        failures while I/O workloads are actively running.

        Steps:
        1. Create a mix of CephFS and RBD PVCs with multiple access modes.
        2. Deploy FIO-based workloads on these PVCs.
        3. Inject specific failure scenario (e.g., OSD, MGR, MDS pod deletion).
        4. Verify workloads continue to function without I/O errors post recovery.
        5. Clean up workloads and verify system stability.

        """
        log.info(f"Running Scenario: {scenario_name}, Failure Case: {failure_case}")

        workloads, scale_thread = self._prepare_pvcs_and_workloads(
            project_factory, multi_pvc_factory, resiliency_workload
        )

        resiliency_runner = Resiliency(scenario_name, failure_method=failure_case)
        resiliency_runner.start()
        resiliency_runner.cleanup()

        # Wait for scaling to complete if needed
        if scale_thread and scale_thread.is_alive():
            scale_thread.join(timeout=60)  # Wait max 60 seconds

        self._validate_and_cleanup_workloads(workloads)
