import pytest
import os
import fauxfactory
import yaml
import logging
from ocs_ci.ocs.constants import (
    KRKN_REPO_URL,
    KRKN_VERSION,
    KRKN_DIR,
    KRKN_CHAOS_DIR,
    KRKN_CHAOS_SCENARIO_DIR,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import run_cmd

from contextlib import suppress

from ocs_ci.ocs.exceptions import UnexpectedBehaviour

log = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def krkn_setup():
    """
    Fixture to set up Krkn chaos testing environment.

    - Clones the Krkn repo into the data directory
    - Installs Krkn as an editable Python package
    - Validates presence of global config and scenario directories

    This fixture does not return anything.
    """
    # Set KUBECONFIG environment variable to prevent krkn_lib from failing during import
    from ocs_ci.framework import config

    kubeconfig_path = os.path.join(
        config.ENV_DATA["cluster_path"],
        config.RUN["kubeconfig_location"],
    )
    if os.path.exists(kubeconfig_path):
        os.environ["KUBECONFIG"] = kubeconfig_path

        # Create symlink or copy kubeconfig to ~/.kube/config for hardcoded references
        default_kube_dir = os.path.expanduser("~/.kube")
        default_kubeconfig = os.path.join(default_kube_dir, "config")

        # Create ~/.kube directory if it doesn't exist
        os.makedirs(default_kube_dir, exist_ok=True)

        # Remove existing ~/.kube/config if it exists
        if os.path.exists(default_kubeconfig):
            if os.path.islink(default_kubeconfig):
                os.unlink(default_kubeconfig)
            else:
                os.remove(default_kubeconfig)

        # Try to create symlink first, fall back to copy if symlink fails
        try:
            os.symlink(kubeconfig_path, default_kubeconfig)
        except (OSError, NotImplementedError) as e:
            log.error(e)
            # Symlink might fail on some systems, fall back to copy
            import shutil

            shutil.copy2(kubeconfig_path, default_kubeconfig)
    else:
        log.warning(
            f"Kubeconfig file not found at {kubeconfig_path}, krkn_lib import may fail"
        )

    # Cleanup if old krkn dir exists
    if os.path.exists(KRKN_DIR):
        import shutil

        shutil.rmtree(KRKN_DIR)

    # Clone the Krkn repo
    try:
        run_cmd(
            f"git clone --branch {KRKN_VERSION} --single-branch {KRKN_REPO_URL} {KRKN_DIR}"
        )
    except CommandFailed:
        log.error(f"Failed to clone Krkn repository version {KRKN_VERSION}")
        raise

    # Fix setup.cfg if needed (kraken → krkn issue)
    setup_cfg_path = os.path.join(KRKN_DIR, "setup.cfg")
    if os.path.exists(setup_cfg_path):
        with open(setup_cfg_path, "r+") as f:
            content = f.read()
            if "package_dir =\n    =kraken" in content:
                f.seek(0)
                f.write(content.replace("=kraken", "=krkn"))
                f.truncate()

    # Install Krkn in editable mode
    try:
        run_cmd("pip install --upgrade pip")
        run_cmd(
            f"pip install -r {KRKN_DIR}/requirements.txt > /dev/null 2>&1", shell=True
        )
        run_cmd(f"pip install -e {KRKN_DIR}")
    except CommandFailed:
        log.error("Failed to install Krkn package")
        raise CommandFailed("Failed to install Krkn package")


@pytest.fixture(scope="session")
def krkn_scenarios_list():
    """
    Load the hog_scenarios YAML configuration into a Python dictionary.

    Returns:
        dict: Parsed hog_scenarios content
    """
    config_path = os.path.join(KRKN_CHAOS_DIR, "config", "chaos_scenarios_list.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Scenario YAML not found at {config_path}")

    with open(config_path, "r") as f:
        data = yaml.safe_load(f)

    return data


@pytest.fixture()
def krkn_scenario_directory():
    """
    Fixture to provide the path to the Krkn chaos scenarios directory.

    Returns:
        str: Path to the Krkn chaos scenarios directory.
    """
    random_dir_name = fauxfactory.gen_alpha(length=8).lower()
    dir_path = os.path.join(KRKN_CHAOS_SCENARIO_DIR, random_dir_name)
    os.makedirs(dir_path, exist_ok=True)
    return dir_path


class WorkloadOps:
    """
    Helper to manage VDBENCH workloads life cycle (create -> validate -> cleanup).
    """

    def __init__(self, proj_obj, workloads):
        self.proj_obj = proj_obj
        self.workloads = workloads

    @property
    def namespace(self):
        return self.proj_obj.namespace

    def validate_and_cleanup(self):
        """
        Validate workload results and stop/cleanup all workloads.
        """
        validation_errors = []
        for workload in self.workloads:
            try:
                result = workload.workload_impl.get_all_deployment_pod_logs()
                workload.stop_workload()

                if not result:
                    validation_errors.append(
                        f"Workload {workload.workload_impl.deployment_name} returned no logs after network outage"
                    )
                elif "error" in result.lower():
                    validation_errors.append(
                        f"Workload {workload.workload_impl.deployment_name} failed after network outage"
                    )

                workload.cleanup_workload()

            except UnexpectedBehaviour as e:
                validation_errors.append(
                    f"Failed to get results for workload {workload.workload_impl.deployment_name}: {e}"
                )

        if validation_errors:
            log.error("Workload validation errors:\n" + "\n".join(validation_errors))
            pytest.fail("Workload validation failed.")


@pytest.fixture
def workload_ops(request, project_factory, multi_pvc_factory):
    """
    Simplified workload ops fixture with conditional fixture loading.

    This fixture only loads the fixtures that are actually needed based on
    the workload types configured in krkn_chaos_config.yaml.

    Supported workload types:
    - VDBENCH: Traditional VDBENCH workloads on CephFS and RBD
    - CNV_WORKLOAD: CNV-based virtual machine workloads
    - GOSBENCH: S3 performance testing workload against NooBaa/RGW

    Background cluster operations handle validation during chaos testing,
    eliminating the need for separate verification logic.
    """
    from ocs_ci.krkn_chaos.krkn_workload_factory import KrknWorkloadFactory
    from ocs_ci.krkn_chaos.krkn_workload_config import KrknWorkloadConfig

    # Load configuration
    config = KrknWorkloadConfig()

    # Check if workloads should be run
    if not config.should_run_workload():
        # Create a minimal workload ops object for compatibility
        class NoWorkloadOps:
            def __init__(self):
                self.workloads = []
                self.workload_types = []
                self.workloads_by_type = {}
                self.namespace = None
                self.project = None

            def setup_workloads(self):
                """No-op setup when workloads are disabled."""
                log.info("Workloads are disabled in configuration")

            def validate_and_cleanup(self):
                """No-op validation and cleanup when workloads are disabled."""
                log.info("No workloads to clean up")

        try:
            yield NoWorkloadOps()
        finally:
            pass
        return

    # Conditionally load only the fixtures needed for configured workload types
    # This uses the workload registry for automatic fixture discovery
    from ocs_ci.krkn_chaos.krkn_workload_registry import KrknWorkloadRegistry

    workload_types = config.get_workloads()
    log.info(f"Loading fixtures for workload types: {workload_types}")

    fixtures = {}

    # Automatically load fixtures based on workload registry
    for workload_type in workload_types:
        if not KrknWorkloadRegistry.is_registered(workload_type):
            log.warning(f"Workload type '{workload_type}' not registered, skipping")
            continue

        required_fixtures = KrknWorkloadRegistry.get_required_fixtures(workload_type)

        if not required_fixtures:
            log.info(f"Workload type '{workload_type}' needs no fixtures")
            continue

        log.info(f"Loading fixtures for {workload_type}: {required_fixtures}")

        for fixture_name in required_fixtures:
            try:
                fixtures[fixture_name] = request.getfixturevalue(fixture_name)
                log.debug(f"  ✓ Loaded fixture: {fixture_name}")
            except Exception as e:
                log.error(f"  ✗ Failed to load fixture '{fixture_name}': {e}")
                # Don't fail immediately - let factory handle missing fixtures

    # Create workload factory and workloads using registry-based approach
    factory = KrknWorkloadFactory()
    ops = factory.create_workload_ops(
        project_factory,
        multi_pvc_factory,
        loaded_fixtures=fixtures,  # Pass all loaded fixtures
    )

    try:
        yield ops
    finally:
        # Best-effort cleanup if the test aborted before calling validate_and_cleanup
        log.info("Performing best-effort workload cleanup")
        for w in ops.workloads:
            with suppress(Exception):
                if hasattr(w, "stop_workload"):
                    w.stop_workload()
                elif hasattr(w, "vm_obj") and w.vm_obj:
                    w.vm_obj.stop()
            with suppress(Exception):
                if hasattr(w, "cleanup_workload"):
                    w.cleanup_workload()
                elif hasattr(w, "delete_workload"):
                    w.delete_workload()
