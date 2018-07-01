#!/usr/bin/env python3

import sys
import os

parent_path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_path)

from common import download_user

if len(sys.argv) != 2:
    print("Exactly one cmdline argument (screen name) is required!")
    sys.exit(2)

screenname = sys.argv[1]
download_user(screenname)
