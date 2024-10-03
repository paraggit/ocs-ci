import yaml
import os
import logging
from ocs_ci.ocs import constants
from ocs_ci.resiliency.node_failures import NodeFailures
from ocs_ci.resiliency.network_failures import NetworkFailures
from ocs_ci.helpers.sanity_helpers import Sanity

# Configure the logger
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class ResiliencyConfig:
    """Handles loading and parsing of the resiliency configuration."""

    def __init__(self):
        self.data = self.load_yaml(
            os.path.join(constants.RESILIENCY_DIR, "conf", "resiliency.yaml")
        )
        self.run_config = self.data.get("RESILIENCY", {}).get("RUN_CONFIG", {})
        self.stop_when_ceph_unhealthy = self.run_config.get(
            "STOP_WHEN_CEPH_UNHEALTHY", False
        )
        self.iterate_scenarios = self.run_config.get("ITERATE_SCENARIOS", False)
        self.failure_scenarios = self.data.get("RESILIENCY", {}).get(
            "FAILURE_SCENARIOS", []
        )

    @staticmethod
    def load_yaml(file_path):
        """Load and parse the YAML file."""
        try:
            with open(file_path, "r") as file:
                return yaml.safe_load(file)
        except (yaml.YAMLError, FileNotFoundError) as exc:
            log.error(f"Error loading YAML file {file_path}: {exc}")
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
    """Handles loading failure cases from the configuration and iterating over them."""

    def __init__(self, scenario):
        super().__init__()
        self.scenario_name = scenario
        self.failure_cases_data = self.get_failure_cases_data()
        self.failure_list = self.failure_cases_data.get("FAILURES", [])
        self.workload = self.failure_cases_data.get("WORKLOAD", "")
        self._index = 0

    def get_failure_cases_data(self):
        """Load the YAML file containing failure case details for the given scenario."""
        dir_loc = os.path.join(constants.RESILIENCY_DIR, "conf")
        log.info(f"Searching for scenario failures in directory: {dir_loc}")

        for filename in filter(
            lambda f: f.endswith((".yaml", ".yml")), os.listdir(dir_loc)
        ):
            file_path = os.path.join(dir_loc, filename)
            log.debug(f"Processing file: {file_path}")
            data = self.load_yaml(file_path)

            if self.scenario_name in data:
                log.info(f"Found scenario '{self.scenario_name}' in file: {filename}")
                return data[self.scenario_name]

        log.error(f"Scenario '{self.scenario_name}' not found in any YAML files.")
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
        raise StopIteration


class Resiliency(ResiliencyFailures):
    """Main class for running resiliency tests."""

    def __init__(self, scenario):
        super().__init__(scenario)
        self.sanity_helpers = Sanity()

    def post_scenario_check(self):
        """Perform post-scenario checks like Ceph health and logs."""
        log.info("Checking CEPH health...")
        self.sanity_helpers.health_check(tries=40)
        log.info("Running must-gather logs.")

    def start(self):
        """Iterate over and inject the failures one by one."""
        for failure_case in self:
            self.inject_failure(failure_case)

    def inject_failure(self, failure):
        """Inject the failure into the system."""
        log.info(f"Running Failure Case for scenario {self.scenario_name}")
        failure_obj = InjectFailures(self.scenario_name, failure)
        failure_obj.run_failure_case()

    def cleanup(self):
        """Cleanup method after the scenario is completed."""
        log.info("Cleaning up after the scenario.")


class InjectFailures:
    """Handles the actual injection of failures based on the scenario."""

    def __init__(self, scenario, failure_case):
        self.scenario = scenario
        self.failure_case = failure_case

    def failure_object(self):
        if self.scenario == NetworkFailures.SCENARIO_NAME:
            return NetworkFailures(self.failure_case)
        elif self.scenario == NodeFailures.SCENARIO_NAME:
            return NodeFailures(self.failure_case)
        else:
            raise NotImplementedError(
                f"No implementation for scenario '{self.scenario}'"
            )

    def run_failure_case(self):
        """Inject the failure into the cluster."""
        log.info("Injecting failure into the cluster...")
        failure_obj = self.failure_object()
        failure_obj.run()
