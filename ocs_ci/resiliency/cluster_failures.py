import logging

from ocs_ci.ocs.node import get_node_ips
from abc import ABC, abstractmethod
from ocs_ci.framework import config
from ocs_ci.utility.vsphere import VSPHERE
from ocs_ci.ocs import constants
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
    def change_node_network_interface_state(self, node_ip, node_name, interface_name):
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

    def random_node_ip(self, node_type="worker"):
        """ """
        ips = get_node_ips(node_type=node_type)
        return random.choice(ips)

    def shutdown_node(self, node_type="worker"):
        node_ip = self.random_node_ip(node_type=node_type)
        print(f"Shutting down node {node_ip} on vSphere cluster {self.cluster_name}")

    def reboot_node(self, node_type="worker"):
        """Reboots the VM and waits until it is fully operational."""
        node_ip = self.random_node_ip(node_type=node_type)
        vm = self.vsobj.get_vm_by_ip(node_ip, self.dc)
        vm_name = vm.name  # Store the VM name for logging

        self.vsobj.stop_vms([vm])
        log.info(f"VM instance {vm_name} is STOPPED")
        time.sleep(20)
        self.vsobj.start_vms([vm])
        log.info(f"VM instance {vm_name} Is started.")

    def change_node_network_interface_state(
        self, node_ip=None, node_type="worker", connect=False
    ):
        """_summary_

        Args:
            node_ip (_type_): _description_
            interface_name (_type_): _description_

        Raises:
            NotImplementedError: _description_
        """
        # Change this log message
        if not node_ip:
            node_ip = self.random_node_ip(node_type=node_type)

        log.info(
            f"Changing Network INterface state of node {node_ip} "
            f" on node {node_ip} in vSphere cluster"
        )
        self.vsobj.change_vm_network_state(node_ip, self.dc, connect=connect)

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
