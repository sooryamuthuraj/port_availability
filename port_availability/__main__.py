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



# import socket
# from dynatrace_extension import Extension, Status, StatusValue


# class ExtensionImpl(Extension):
#     def query(self):
#         """
#         The query method is automatically scheduled to run every minute.
#         Checks port availability for each configured endpoint.
#         """
#         self.logger.info("Query method started for port_availability.")

#         for endpoint in self.activation_config["endpoints"]:
#             host = endpoint.get("host")
#             port = int(endpoint.get("port"))
#             timeout = int(endpoint.get("timeout", 5))  # default timeout 5 seconds
#             schedule = int(endpoint.get("schedule_interval", 1)) # default schedule 1 minute

#             self.logger.debug(f"Checking connectivity to {host}:{port} (timeout={timeout}s)")

#             # Perform port check
#             is_available = self._check_port(host, port, timeout)

#             # Report metric (1 = available, 0 = unavailable)
#             self.report_metric(
#                 "custom.port.availability",
#                 1 if is_available else 0,
#                 dimensions={"host": host, "port": str(port)}
#             )

#             status_str = "available" if is_available else "unavailable"
#             self.logger.info(f"Port check result: {host}:{port} is {status_str}")

#         self.logger.info("Query method ended for port_availability.")

#     def _check_port(self, host: str, port: int, timeout: int) -> bool:
#         """
#         Attempts to connect to the given host:port using TCP.
#         Returns True if the connection succeeds, False otherwise.
#         """
#         try:
#             with socket.create_connection((host, port), timeout=timeout):
#                 return True
#         except (socket.timeout, socket.error) as e:
#             self.logger.debug(f"Port check failed for {host}:{port} — {e}")
#             return False

#     def fastcheck(self) -> Status:
#         """
#         Quick validation to check if the extension can run properly.
#         """
#         self.logger.info("Running fastcheck for port_availability extension.")
#         return Status(StatusValue.OK)


# def main():
#     ExtensionImpl(name="port_availability").run()


# if __name__ == "__main__":
#     main()
