import logging
from ocs_ci.resiliency.resiliency_helper import ResiliencyFailures

log = logging.getLogger(__name__)


def run(scenario):
    """ """
    conf_obj = ResiliencyFailures(scenario)
    log.info(f"Resiliency Failures Scenarios {conf_obj}")

    return True
