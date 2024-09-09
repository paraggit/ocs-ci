from dynaconf import Dynaconf
import logging
import os
import yaml
from ocs_ci.ocs import constants

# Configure the logger
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


class ResiliencyConfig(RunConfig):
    def __init__(self):
        super().__init__()
        self.resiliency_scenarios = self.run_config.MAIN.FAILURE_SCENARIOS
        log.info(f"Loaded resiliency scenarios: {self.resiliency_scenarios}")


class ResiliencyFailures(ResiliencyConfig):
    def __init__(self, scenario):
        super().__init__()
        self.scenario = scenario
        log.info(f"Initializing failure listing for scenario: {self.scenario}")
        self.failure_list = self.list_failures()

    def list_failures(self):
        """
        List the failures for the given scenario by iterating over YAML files.
        """
        failure_list = None
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
                        if list(data.keys())[0] == self.scenario:
                            log.info(
                                f"Found scenario '{self.scenario}' in file: {filename}"
                            )
                            failure_list = data
                            break  # Stop once the scenario is found
                except yaml.YAMLError as exc:
                    log.error(f"Error reading {filename}: {exc}")
                except Exception as e:
                    log.error(f"Unexpected error processing file {filename}: {e}")

        if failure_list:
            log.info(f"Failures found for scenario '{self.scenario}'")
        else:
            log.warning(f"No failures found for scenario '{self.scenario}'")

        return failure_list


class Resiliency(ResiliencyFailures):
    def __init__(self):
        super().__init__()

    def setup(self):
        """ """
        log.info("IN Setup Method")

    def inject_failure(self):
        """ """
        log.info("Injecting Failure")

    def cleanup():
        """ """

        log.info("Cleanup")
