import yaml
import os
import logging
from ocs_ci.ocs import constants
from ocs_ci.resiliency.node_failures import NodeFailures
from ocs_ci.resiliency.network_failures import NetworkFailures

# from ocs_ci.helpers.sanity_helpers import Sanity

# Configure the logger
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class ResiliencyConfig:
    def __init__(self):
        # Load the YAML data
        self.data = self.load_yaml(
            os.path.join(constants.RESILIENCY_DIR, "conf", "resiliency.yaml")
        )

        # Extract RUN_CONFIG details
        resiliency_section = self.data.get("RESILIENCY", {})
        run_config = resiliency_section.get("RUN_CONFIG", {})
        self.stop_when_ceph_unhealthy = run_config.get(
            "STOP_WHEN_CEPH_UNHEALTHY", False
        )
        self.iterate_scenarios = run_config.get("ITERATE_SCENARIOS", False)

        # Extract FAILURE_SCENARIOS
        self.failure_scenarios = resiliency_section.get("FAILURE_SCENARIOS", [])

    def load_yaml(self, file_path):
        """Load the YAML file."""
        try:
            with open(file_path, "r") as file:
                return yaml.safe_load(file)
        except yaml.YAMLError as exc:
            log.error(f"Error loading YAML file {file_path}: {exc}")
            return {}
        except FileNotFoundError as fnf_error:
            log.error(f"YAML file not found: {file_path}. Error: {fnf_error}")
            return {}

    def get_run_config(self):
        """Return the run configuration."""
        return {
            "STOP_WHEN_CEPH_UNHEALTHY": self.stop_when_ceph_unhealthy,
            "ITERATE_SCENARIOS": self.iterate_scenarios,
        }

    def get_failure_scenarios(self):
        """Return the failure scenarios."""
        return self.failure_scenarios

    def __repr__(self):
        """Representation of the ResiliencyConfig object."""
        return (
            f"ResiliencyConfig("
            f"STOP_WHEN_CEPH_UNHEALTHY={self.stop_when_ceph_unhealthy}, "
            f"ITERATE_SCENARIOS={self.iterate_scenarios}, "
            f"FAILURE_SCENARIOS={self.failure_scenarios})"
        )


class ResiliencyFailures(ResiliencyConfig):
    def __init__(self, scenario):
        super().__init__()
        self.scenario_name = scenario
        self.failure_cases_data = self.get_failure_cases_data()
        self.failure_list = self.get_failure_cases()  # List of failures
        self.workload = self.get_workload()
        self._index = 0  # Internal index to track iteration

    def get_failure_cases(self):
        if self.failure_cases_data:
            return self.failure_cases_data["FAILURES"]

    def get_workload(self):
        if self.failure_cases_data:
            return self.failure_cases_data["WORKLOAD"]

    def get_failure_cases_data(self):
        """List the failures for the given scenario by iterating over YAML files."""
        # failure_list = []
        dir_loc = os.path.join(constants.RESILIENCY_DIR, "conf")
        log.info(f"Searching for scenario failures in directory: {dir_loc}")

        try:
            for filename in filter(
                lambda f: f.endswith((".yaml", ".yml")), os.listdir(dir_loc)
            ):
                file_path = os.path.join(dir_loc, filename)
                log.debug(f"Processing file: {file_path}")
                data = self.load_yaml(
                    file_path
                )  # Call 'load_yaml' instead of the undefined '_load_yaml_file'

                if self.scenario_name in data.keys():
                    log.info(
                        f"Found scenario '{self.scenario_name}' in file: {filename}"
                    )
                    return data[self.scenario_name]

        except FileNotFoundError as fnf_error:
            log.error(f"Directory not found: {dir_loc}. Error: {fnf_error}")
        except Exception as e:
            log.error(f"Unexpected error accessing directory {dir_loc}: {e}")

        return {}

    def __iter__(self):
        """Return the iterator object itself."""
        self._index = 0  # Reset the index whenever iteration starts
        return self

    def __next__(self):
        """Return the next failure in the list or raise StopIteration."""
        if self._index < len(self.failure_list):
            failure = self.failure_list[self._index]
            self._index += 1
            return failure
        else:
            raise StopIteration


class Resiliency(ResiliencyFailures):
    def __init__(self, scenario):
        super().__init__(scenario)
        self.scenario_name = scenario

        # self.sanity_helpers = Sanity()

    # def run_workload(self, worklod_data):
    #     """Setup method before starting the resiliency scenario."""
    #     log.info(f"Setting up workload... {worklod_data}")
    #     # Start any workload mentioned in the config.
    #     return True

    def post_scenario_check(self):
        """Post scenario check for Ceph health."""
        log.info("Checking CEPH health...")
        # self.sanity_helpers.health_check(tries=40)
        log.info("Running must-gather logs.")

    def start(self):
        """Iterate over the failures and inject them one by one."""

        for failure_case in self:
            self.inject_failure(failure_case)

    def inject_failure(self, failure):
        """Inject the failure into the system."""
        log.info(f"Running Failure Case for scenario {self.scenario_name}")
        failure_obj = InjectFailures(self.scenario_name, failure)
        failure_obj.run_failure_case()

    # def stop_workload(
    #     self,
    # ):
    #     """Stop all workloads."""
    #     log.info("Stopping all workloads...")

    def cleanup(self):
        """Cleanup method after the scenario is completed."""
        log.info("Cleaning up after the scenario.")
        # self.stop_workload()


class InjectFailures:
    def __init__(self, scenario, failure_case):
        self.scenario = scenario
        self.failure_case = failure_case
        # self.failure_case_name = self.failure_case.keys()[0]

    def failure_object(self):

        if self.scenario == "NETWORK_FAILURE":
            return NetworkFailures(self.scenario, self.failure_case)
        elif self.scenario == "NODE_FAILURES":
            return NodeFailures(self.scenario, self.failure_case)
        else:
            raise NotImplementedError(f"No implementation for {self.scenario}")

    def run_failure_case(self):
        """Inject the failure into the cluster."""
        log.info("Injecting failure into the cluster...")
        failure_obj = self.failure_object()
        failure_obj.run()
