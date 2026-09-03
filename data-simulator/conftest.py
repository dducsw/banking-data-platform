import os
import sys

# Ensure data-simulator root is on sys.path
simulator_dir = os.path.dirname(os.path.abspath(__file__))
if simulator_dir not in sys.path:
    sys.path.insert(0, simulator_dir)
