"""`kopf run -m reconciler.controllers.entrypoint` — the operator's main module.

Separate from `task_sweep` because kopf requires handlers to be registered at IMPORT time,
while the rest of this package keeps registration behind an explicit `register()` call so the
tests can import the module without side effects. One file reconciles the two conventions.
"""

from reconciler.controllers import task_sweep

task_sweep.register()
