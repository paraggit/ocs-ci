

from abc import ABC, abstractmethod

class ResiliencyTest(ABC):
    def __init__(self, openshift_client):
        self.openshift_client = openshift_client

    @abstractmethod
    def setup(self):
        """Setup the environment for the test."""
        pass

    @abstractmethod
    def run(self):
        """Run the resiliency scenario."""
        pass

    @abstractmethod
    def teardown(self):
        """Teardown the environment after the test."""
        pass

    def execute(self):
        """Execute the full lifecycle of the test."""
        self.setup()
        try:
            self.run()
        finally:
            self.teardown()
