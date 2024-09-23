import logging

from ocs_ci.ocs.node import get_node_ips
from abc import ABC, abstractmethod
from ocs_ci.framework import config
from ocs_ci.utility.vsphere import VSPHERE
from ocs_ci.ocs import constants
from pyVmomi import vim
import random
import time


log = logging.getLogger(__name__)


class ClusterFailures(ABC):
    def __init__(self, cluster_name):
        self.cluster_name = cluster_name

    @abstractmethod
    def shutdown_node(self, node_ip):
        pass

    @abstractmethod
    def bring_down_network_interface(self, node_name, interface_name):
        pass

    @abstractmethod
    def network_split(self, nodes):
        pass


# vSphere-specific implementation
class VsphereClusterFailures(ClusterFailures):
    def __init__(self):
        self.vsphere_host = config.ENV_DATA["vsphere_server"]
        self.vsphere_password = config.ENV_DATA["vsphere_password"]
        self.vsphere_username = config.ENV_DATA["vsphere_user"]
        self.dc = config.ENV_DATA["vsphere_datacenter"]
        self.vsobj = VSPHERE(
            self.vsphere_host, self.vsphere_username, self.vsphere_password
        )

    def shutdown_node(self, node_ip):
        print(f"Shutting down node {node_ip} on vSphere cluster {self.cluster_name}")

    def reboot_node(self, node_ip):
        """Reboots the VM and waits until it is fully operational."""
        vm = self.vsobj.get_vm_by_ip(node_ip, self.dc)
        vm_name = vm.name  # Store the VM name for logging

        try:
            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                log.info(f"Rebooting VM: {vm_name}")
                vm.RebootGuest()
            else:
                log.info(
                    f"VM {vm_name} is not powered on. Power state: {vm.runtime.powerState}"
                )
                return False
        except vim.fault.ToolsUnavailable:
            log.info(
                f"VMware Tools are not available on {vm_name}. Performing a hard reset."
            )
            reset_task = vm.ResetVM_Task()
            self.vsobj.wait_for_tasks([reset_task])
        except Exception as e:
            log.error(f"Error rebooting the VM {vm_name}: {e}")
            return False

        # Wait for the VM to power on and VMware Tools to be running
        max_wait_time = 300  # seconds
        wait_interval = 5  # seconds
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            time.sleep(wait_interval)
            elapsed_time += wait_interval

            try:
                # Re-fetch the VM object to get updated properties
                vm = self.vsobj.get_vm_by_ip(node_ip, self.dc)

                power_state = vm.runtime.powerState
                tools_status = vm.guest.toolsRunningStatus

                if (
                    power_state == vim.VirtualMachinePowerState.poweredOn
                    and tools_status == "guestToolsRunning"
                ):
                    log.info(f"VM {vm_name} has rebooted and VMware Tools are running.")
                    return True
                else:
                    log.info(
                        f"Waiting for VM {vm_name} to reboot. "
                        f"Power state: {power_state}, VMware Tools status: {tools_status}"
                    )
            except Exception as e:
                log.error(f"Error retrieving VM {vm_name} status: {e}")
                return False

        log.error(
            f"Timeout waiting for VM {vm_name} to reboot after {max_wait_time} seconds."
        )
        return False

    def bring_down_network_interface(self, node_name, interface_name):
        print(
            f"Bringing down network interface {interface_name} "
            f" on node {node_name} in vSphere cluster {self.cluster_name}"
        )
        raise NotImplementedError(
            "Function 'bring_down_network_interface' Not Implimented."
        )

    def network_split(self, nodes):
        raise NotImplementedError("Function 'network_split' Not Implimented.")
        # Add vSphere-specific network split logic


# IBM Cloud-specific implementation
class IbmCloudClusterFailures(ClusterFailures):
    def shutdown_node(self, node_name):
        print(
            f"Shutting down node {node_name} on IBM Cloud cluster {self.cluster_name}"
        )
        raise NotImplementedError("")
        # Add IBM Cloud-specific shutdown logic

    def bring_down_network_interface(self, node_name, interface_name):
        print(
            f"Bringing down network interface {interface_name} "
            f" on node {node_name} in IBM Cloud cluster {self.cluster_name}"
        )
        # Add IBM Cloud-specific logic to bring down network interface

    def network_split(self, nodes):
        print(
            f"Simulating network split on nodes {nodes} in IBM Cloud cluster {self.cluster_name}"
        )
        # Add IBM Cloud-specific network split logic


# AWS-specific implementation
class AwsClusterFailures(ClusterFailures):
    def shutdown_node(self, node_name):
        print(f"Shutting down node {node_name} on AWS cluster {self.cluster_name}")
        # Add AWS-specific shutdown logic (e.g., using boto3 for EC2 instance control)

    def bring_down_network_interface(self, node_name, interface_name):
        print(
            f"Bringing down network interface {interface_name} on node {node_name} in AWS cluster {self.cluster_name}"
        )
        # Add AWS-specific logic to bring down network interface

    def network_split(self, nodes):
        print(
            f"Simulating network split on nodes {nodes} in AWS cluster {self.cluster_name}"
        )
        # Add AWS-specific network split logic


# AWS-specific implementation
class BaremetalClusterFailures(ClusterFailures):
    def shutdown_node(self, node_name):
        print(f"Shutting down node {node_name} on AWS cluster {self.cluster_name}")
        # Add AWS-specific shutdown logic (e.g., using boto3 for EC2 instance control)

    def bring_down_network_interface(self, node_name, interface_name):
        print(
            f"Bringing down network interface {interface_name} on node {node_name} in AWS cluster {self.cluster_name}"
        )
        # Add AWS-specific logic to bring down network interface

    def network_split(self, nodes):
        print(
            f"Simulating network split on nodes {nodes} in AWS cluster {self.cluster_name}"
        )
        # Add AWS-specific network split logic


# Factory Method to create cluster manager for a specific platform
def get_cluster_object():
    platform = config.ENV_DATA["platform"].lower()
    if platform == constants.VSPHERE_PLATFORM:
        return VsphereClusterFailures()
    elif platform == constants.AWS_PLATFORM:
        return AwsClusterFailures()
    elif platform == constants.IBMCLOUD_PLATFORM:
        return IbmCloudClusterFailures()
    elif platform == constants.BAREMETAL_PLATFORM:
        return BaremetalClusterFailures()
    else:
        raise ValueError(f"Unsupported platform: {platform}")


class NodeFailures:
    def __init__(self, scenario_name, failure_data):
        self.failure_data = failure_data
        self.failure_case_name = self._get_failure_case()
        self.scenario_name = scenario_name
        self.cluster_obj = get_cluster_object()

    def _get_failure_case(self):
        try:
            return list(self.failure_data.keys())[0]
        except Exception as e:
            log.error(f"Error parsing the failure_data : {e}")
            return None

    def run(self):
        """ """
        if self.failure_case_name == "REBOOT_NODE_RANDOMLY":
            self._run_reboot_node()
        elif self.failure_case_name == "NODE_DRAIN":
            self._run_node_drain()
        else:
            raise NotImplementedError("Failure method is not Implimented")

    def _run_reboot_node(self):
        """ """
        log.info("Running Failure Case REBOOT_NODE_RANDOMLY .")

        # Get the node list
        for node_type in self.failure_data[self.failure_case_name]["NODE_TYPE"]:
            ips = get_node_ips(node_type=node_type)
            random_ip = random.choice(ips)
            log.info(f"Rebooting {node_type} Node IP: {random_ip}")
            self.cluster_obj.reboot_node(random_ip)
            log.info("Waiting for node status.")

        self._post_scenario_checks()

    def _run_node_drain(self):
        log.info("Draiing Node .... ")

        self._post_scenario_checks()

    def _post_scenario_checks(self):
        """ """
        log.info(" running Post scenario checks : {self.scenario_name}")
