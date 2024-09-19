from abc import ABC, abstractmethod
from kubernetes import client, config
from jinja2 import Environment, FileSystemLoader
from ocs_ci.ocs import constants
import logging
import fauxfactory
from ocs_ci.utility.utils import run_cmd

log = logging.getLogger(__name__)

# Load Kubernetes config (Assuming config is properly set up)
config.load_kube_config()

v1 = client.CoreV1Api()


class Workload(ABC):
    """
    Abstract Base Class for all workloads
    """

    def __init__(self):
        self.template_dir = f"{constants.RESILIENCY_DIR}/workloads"
        self.workload_env = Environment(loader=FileSystemLoader(self.template_dir))

    @abstractmethod
    def start_workload(self, pod_name):
        pass

    @abstractmethod
    def scale_up_pods(self, desired_count):
        pass

    @abstractmethod
    def scale_down_pods(self, desired_count):
        pass

    @abstractmethod
    def stop_workload(self, pod_name):
        pass

    @abstractmethod
    def cleanup_workload(self):
        pass


class FioWorkload(Workload):
    """
    FIO-specific implementation of Workload
    """

    def __init__(self, workload_data):
        super().__init__()
        self.template_file = "fio_workload_template.yaml"
        self.template = self.workload_env.get_template(self.template_file)
        self.output_file = f"/tmp/{fauxfactory.gen_alpha(8)}.yaml"
        self.render_template(workload_data)

    def start_workload(self):
        log.info("Starting FIO workload")
        run_cmd(f"oc create -f {self.output_file}")
        log.info("Started FIO Workload")

        # Implement pod creation logic here using the self.image and v1 API
        # Create FIO pod with Kubernetes API

    def scale_up_pods(self, desired_count):
        log.info(f"Scaling up FIO pods to {desired_count}")
        # Implement logic to scale up pods

    def scale_down_pods(self, desired_count):
        log.info(f"Scaling down FIO pods to {desired_count}")
        # Implement logic to scale down pods

    def stop_workload(self):
        log.info("Stopping FIO workload in ")
        run_cmd(f"oc -f {self.output_file} delete")
        # Implement pod deletion logic here using the v1 API

    def cleanup_workload(self):
        log.info("Cleaning up FIO workload")
        # Implement cleanup logic, e.g., deleting all pods in the workload

    def render_template(self, workload_data):
        self.fio_name = workload_data["name"]
        self.namespace = workload_data["namespace"]
        self.file_name = workload_data["file_name"]
        self.pvc_name = workload_data["pvc_name"]
        self.replicas = workload_data["replicas"]

        rendered_yaml = self.template.render(
            fio_name=self.fio_name,
            replicas=self.replicas,
            namespace=self.namespace,
            fio_file_name=self.file_name,
            pvc_claim_name=self.pvc_name,
        )

        with open(self.output_file, "w") as f:
            f.write(rendered_yaml)

        log.info("Rendering Template")


class SmallFilesWorkload(Workload):
    """
    SmallFiles-specific implementation of Workload
    """

    def __init__(self, namespace="default", image="smallfiles-image:latest"):
        super().__init__(namespace, image)

    def start_workload(self, pod_name):
        log.info(f"Starting SmallFiles workload in pod: {pod_name}")
        # Implement pod creation logic

    def scale_up_pods(self, desired_count):
        log.info(f"Scaling up SmallFiles pods to {desired_count}")
        # Implement logic to scale up pods

    def scale_down_pods(self, desired_count):
        log.info(f"Scaling down SmallFiles pods to {desired_count}")
        # Implement logic to scale down pods

    def stop_workload(self, pod_name):
        log.info(f"Stopping SmallFiles workload in pod: {pod_name}")
        # Implement pod deletion logic

    def cleanup_workload(self):
        log.info("Cleaning up SmallFiles workload")
        # Implement cleanup logic


class VdbenchWorkload(Workload):
    """
    Vdbench-specific implementation of Workload
    """

    def __init__(self, namespace="default", image="vdbench-image:latest"):
        super().__init__(namespace, image)

    def start_workload(self, pod_name):
        log.info(f"Starting Vdbench workload in pod: {pod_name}")
        # Implement pod creation logic

    def scale_up_pods(self, desired_count):
        log.info(f"Scaling up Vdbench pods to {desired_count}")
        # Implement logic to scale up pods

    def scale_down_pods(self, desired_count):
        log.info(f"Scaling down Vdbench pods to {desired_count}")
        # Implement logic to scale down pods

    def stop_workload(self, pod_name):
        log.info(f"Stopping Vdbench workload in pod: {pod_name}")
        # Implement pod deletion logic

    def cleanup_workload(self):
        log.info("Cleaning up Vdbench workload")
        # Implement cleanup logic


def workload_object(workload, workload_data, namespace=None):
    """ """
    if workload == "FIO":
        return FioWorkload(workload_data)
    elif workload == "SMALLFILES":
        return SmallFilesWorkload(namespace)
    elif workload == "VDBENCH":
        return VdbenchWorkload(namespace)


# # Example usage
# def main():
#     fio_workload = FioWorkload(namespace="test-namespace")
#     fio_workload.start_workload("fio-pod-1")
#     fio_workload.scale_up_pods(5)
#     fio_workload.scale_down_pods(3)
#     fio_workload.stop_workload("fio-pod-1")
#     fio_workload.cleanup_workload()

#     smallfiles_workload = SmallFilesWorkload(namespace="test-namespace")
#     smallfiles_workload.start_workload("smallfiles-pod-1")
#     smallfiles_workload.cleanup_workload()

#     vdbench_workload = VdbenchWorkload(namespace="test-namespace")
#     vdbench_workload.start_workload("vdbench-pod-1")
#     vdbench_workload.cleanup_workload()

# if __name__ == "__main__":
#     main()
