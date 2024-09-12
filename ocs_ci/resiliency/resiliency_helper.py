from dynaconf import Dynaconf
import logging
import os
import yaml
from ocs_ci.ocs import constants
from ocs_ci.resiliency.node_failures import NodeFailures
from ocs_ci.resiliency.network_failures import NetworkFailures

# Configure the logger
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class RunConfig:
    def __init__(self):
        self.resiliency_config_file = f"{constants.RESILIENCY_DIR}/conf/resiliency.yaml"
        self.run_config = self.get_run_config()

    def get_run_config(self):
        log.info(f"Loading configuration from {self.resiliency_config_file}")
        try:
            config = Dynaconf(settings_files=[self.resiliency_config_file])
            log.info("Configuration loaded successfully.")
            return config
        except Exception as e:
            log.error(f"Failed to load configuration: {e}")
            raise


class ResiliencyFailures(RunConfig):
    def __init__(self, scenarios):
        super().__init__()
        self.scenarios = scenarios
        log.info(f"Initializing failure listing for scenario: {self.scenarios}")
        self.failure_list = self.list_failures()
        self.all_failures_with_keys = self.collect_failures()  # Collect all failures
        self.current_index = 0  # Initialize an index for iteration

    def list_failures(self):
        """
        List the failures for the given scenario by iterating over YAML files.
        """
        failure_list = {}
        dir_loc = f"{constants.RESILIENCY_DIR}/conf/"
        log.info(f"Searching for scenario failures in directory: {dir_loc}")

        for filename in os.listdir(dir_loc):
            if filename.endswith((".yaml", ".yml")):
                file_path = os.path.join(dir_loc, filename)
                log.info(f"Processing file: {file_path}")

                # Open and read the YAML file
                try:
                    with open(file_path, "r") as file:
                        data = yaml.safe_load(file)

                        # Match the scenario key
                        for scenario_name in data.keys():
                            if scenario_name in self.scenarios:
                                log.info(
                                    f"Found scenario '{scenario_name}' in file: {filename}"
                                )
                                failure_list[scenario_name] = data
                            else:
                                log.info(
                                    f"Scenario '{scenario_name}' not found in self.scenarios"
                                )

                except yaml.YAMLError as exc:
                    log.error(f"Error reading {filename}: {exc}")
                except Exception as e:
                    log.error(f"Unexpected error processing file {filename}: {e}")

        if failure_list:
            log.info(
                f"Failures found for the following scenarios: {', '.join(failure_list.keys())}"
            )
        else:
            log.warning(f"No failures found for scenarios in '{self.scenarios}'")

        return failure_list

    def collect_failures(self):
        """
        Collect all failures with their associated keys into a list for iteration.
        """
        all_failures_with_keys = []

        # Iterate over each failure category
        for category_key, category_value in self.failure_list.items():
            # Each category has its 'FAILURES' section, guard against missing key
            if (
                category_key in category_value
                and "FAILURES" in category_value[category_key]
            ):
                failures = category_value[category_key]["FAILURES"]
                # Collect each failure with its associated main key
                for failure in failures:
                    all_failures_with_keys.append((category_key, failure))
            else:
                log.warning(f"'FAILURES' not found in category {category_key}.")

        return all_failures_with_keys

    def __iter__(self):
        """
        Initialize the iterator by resetting the current index.
        """
        self.current_index = 0  # Reset index when iteration starts
        return self

    def __next__(self):
        """
        Return the next failure in the list. If there are no more failures, raise StopIteration.
        """
        if self.current_index >= len(self.all_failures_with_keys):
            raise StopIteration  # End of iteration

        # Retrieve the next failure based on the current index
        main_key, next_failure = self.all_failures_with_keys[self.current_index]
        self.current_index += 1  # Move to the next failure for the next iteration

        return {main_key: next_failure}


class Resiliency(ResiliencyFailures):
    def __init__(self, scenarios):
        super().__init__(scenarios)
        self.scenarios = scenarios

    def setup(self):
        """Setup method before starting the resiliency scenario."""
        log.info("IN Setup Method")

    def post_scenario_check(self):
        """ """
        log.info("Checking CEPH HEALTH ...")

        log.info("Collect or run must gather logs.")
        # raise ValueError("CEPH is not Healthy state.")

    def start(self):
        """Iterate over the failures and inject them one by one."""
        log.info("Starting the resiliency scenario...")

        # The iterator in ResiliencyFailures is already set up, so we can iterate through it.
        log.info("Injecting Failures:")
        for failure in self:
            log.info(f"Injecting failure: {failure}")
            # Here you can perform the failure injection
            self.inject_failure(failure)
            self.post_scenario_check()

    def inject_failure(self, failure):
        """Inject the failure into the system."""

        log.info(f"Failure {failure} is being processed...")

    def cleanup(self):
        """Cleanup method after the scenario is completed."""
        log.info("Cleanup")

    def action_when_failure(self):
        """ """
        log.info("Collect or run must gather logs.")


class InjectFailures:
    def __init__(self, failure):
        self.failure_data = failure

    def scenario(self):
        return self.failure_data.keys()[0]

    def failure_object(self):

        if self.scenario == "NETWORK_FAILURE":
            return NetworkFailures(self.failure_data)
        elif self.scenario == "NODE_FAILURES":
            return NodeFailures(self.failure_data)
        else:
            raise NotImplementedError(
                f"No Failure code is implimentes for method {self.scenario}"
            )

    def run_failure_case(self):

        fl_obj = self.failure_object()
        fl_obj.run()

        pass
