"""Adds host context (load average, time) to a RESULT JSON line on stdin."""
import json
import os
import sys
import time

r = json.loads(sys.stdin.read())
r["load1"] = round(os.getloadavg()[0], 2)
r["ts"] = int(time.time())
print(json.dumps(r))
