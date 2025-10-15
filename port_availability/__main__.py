import socket
import threading
import time
import logging
from dynatrace_extension import Extension, Status, StatusValue

class EndpointChecker(threading.Thread):
    def __init__(self, extension, endpoint):
        super().__init__(daemon=True)
        self.extension = extension
        self.host = endpoint.get("host")
        self.port = int(endpoint.get("port"))
        self.timeout = int(endpoint.get("timeout", 5))
        self.interval = int(endpoint.get("schedule_interval", 1)) * 60  # minutes -> seconds
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def run(self):
        while not self._stop_event.is_set():
            with self._lock:
                try:
                    with socket.create_connection((self.host, self.port), timeout=self.timeout):
                        is_available = 1
                except (socket.timeout, socket.error) as e:
                    is_available = 0
                    self.extension.logger.error(f"Error checking {self.host}:{self.port}: {e}")

                self.extension.report_metric(
                    "custom.port.availability",
                    is_available,
                    dimensions={"host": self.host, "port": str(self.port)}
                )
                status_str = "available" if is_available else "unavailable"
                self.extension.logger.info(f"Port check result: {self.host}:{self.port} is {status_str}")

            # Sleep for the full interval, but check for stop event every second
            for _ in range(self.interval):
                if self._stop_event.is_set():
                    break
                time.sleep(1)

    def stop(self):
        self._stop_event.set()

class ExtensionImpl(Extension):
    def __init__(self, name):
        super().__init__(name)
        self.checkers = {}

    def query(self):
        self.logger.info("Starting endpoint checks...")
        endpoints = self.activation_config.get("endpoints", [])
        for endpoint in endpoints:
            key = (endpoint.get("host"), endpoint.get("port"))
            if key not in self.checkers or not self.checkers[key].is_alive():
                checker = EndpointChecker(self, endpoint)
                self.checkers[key] = checker
                checker.start()
        self.logger.info("All endpoint checkers started.")

    def fastcheck(self):
        return Status(StatusValue.OK)

    def shutdown(self):
        for checker in self.checkers.values():
            checker.stop()
        super().shutdown()

def main():
    ext = ExtensionImpl(name="port_availability")
    try:
        ext.run()
    except KeyboardInterrupt:
        ext.shutdown()

if __name__ == "__main__":
    main()
