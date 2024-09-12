import logging
from ocs_ci.resiliency.resiliency_helper import Resiliency

log = logging.getLogger(__name__)


def run(scenarios):
    """ """
    # Instantiate the Resiliency class with the scenarios
    resiliency = Resiliency(scenarios)

    # Setup before starting
    resiliency.setup()

    # Start the failure injection process
    resiliency.start()

    # Cleanup after completion
    resiliency.cleanup()

    return True
