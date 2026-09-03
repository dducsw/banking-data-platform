"""
Pytest configuration and test environment bootstrapping.
Patches socketserver on Windows platforms where UnixStreamServer is absent.
"""

import sys
import socketserver

# Windows compatibility patch for PySpark accumulators
if sys.platform == "win32" and not hasattr(socketserver, "UnixStreamServer"):
    class UnixStreamServer(socketserver.TCPServer):
        pass
    socketserver.UnixStreamServer = UnixStreamServer
