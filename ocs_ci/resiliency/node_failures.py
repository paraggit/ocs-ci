from dynaconf import Dynaconf
import logging

log = logging.getLogger(__name__)


class NodeFailureConfig:
    def __init__(self):
        """ """
        self.node_failure = Dynaconf(
            settings_files=["ocs-ci/resiliency/conf/node_failures.yaml"]
        )
        self._run_config_validator()

    def _run_config_validator(self):
        """ """
        # self.run_config.validators.register(
        #     Validator("api_key", must_exist=True),
        #     Validator("project.version", must_exist=True)
        # )

        self.run_config.Validators.validate()


class NodeFailures(NodeFailureConfig):
    def __init__(self):
        super().__init__()

    def list_failures(self):
        """ """
        pass

    def inject_failures(self):
        """ """

        pass

    def reboot_node(self, random=True):
        """ """
        log.info("Running Node Reboot scenario")
