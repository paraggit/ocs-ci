import logging

from ocs_ci.ocs.node import get_node_ips
from abc import ABC, abstractmethod
from ocs_ci.framework import config
from ocs_ci.utility.vsphere import VSPHERE
from ocs_ci.ocs import constants
from pyVmomi import vim
import random


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
        """ """
        vm = self.vsobj.get_vm_by_ip(node_ip, self.dc)

        try:
            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                log.info(f"Rebooting VM : {vm.name}")
                vm.RebootGuest()
            else:
                log.info(
                    f"VM {vm.name} is not powered on. Power state: {vm.runtime.powerState}"
                )
                return False
        except vim.fault.ToolsUnavailable:
            log.info(
                "VMware Tools are not installed or running. Rebooting the VM by power cycling."
            )
            vm.ResetVM_Task()  # Force a reboot if VMware Tools are unavailable
        except Exception as e:
            log.info(f"Error rebooting the VM: {e}")
            return False
        return self.vsobj.wait_for_vm_status(vm, vim.VirtualMachinePowerState.poweredOn)

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
    def __init__(self, failure_data):
        self.failure_data = failure_data
        self.scenario_name = "NODE_FAILURES"
        self.cluster_obj = get_cluster_object()

    def failure_case(self):
        return list(self.failure_data[self.scenario_name].keys())[0]

    def run(self):
        """ """
        if self.failure_case() == "REBOOT_NODE_RANDOMLY":
            self._run_reboot_node()
        elif self.failure_case() == "NODE_DRAIN":
            self._run_node_drain()
        else:
            raise NotImplementedError("Failure method is not Implimented")

    def _run_reboot_node(self):
        """ """
        log.info("Running Failure Case REBOOT_NODE_RANDOMLY .")

        # Get the node list
        for node_type in self.failure_data[self.scenario_name][self.failure_case()][
            "NODE_TYPE"
        ]:
            ips = get_node_ips(node_type=node_type)
            random_ip = random.choice(ips)
            self.cluster_obj.reboot_node(random_ip)

        self._post_scenario_checks()

    def _run_node_drain(self):
        log.info("Draiing Node .... ")

        self._post_scenario_checks()

    def _post_scenario_checks(self):
        """ """
        log.info(" running Post scenario checks : {self.scenario_name}")

    def __del__(self):
        """ """
        pass
