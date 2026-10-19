FROM python:3.13-slim

WORKDIR /app

# ca-certificates: every adapter this tier owns is plain `urllib` over HTTPS — Doppler, the
# n8n relays, CraftMyPDF, the Vector gateway — and all of them verify against the system bundle.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Four packages, not the project — see requirements.txt for why quixstreams must stay out.
COPY workers/reconciler/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# The tier is `src/reconciler` plus its definitions. The LAYOUT is load-bearing, not
# incidental: `app.DEFAULT_PROCESSES_DIR` is `parents[2] / "processes"` relative to
# `app.py`, so `/app/src/reconciler/app.py` resolves to `/app/processes`. Flatten this and
# every run fails with "no such process", which reads like a definition problem.
COPY src/reconciler/ ./src/reconciler/
COPY processes/ ./processes/

# `reconciler` is importable without being installed — the same bootstrap `flow.py` does by
# hand for the Prefect clone, done once here where an image can just say it.
ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

RUN useradd -u 1000 -m worker
USER 1000

# ENTRYPOINT, not CMD: a CronWorkflow supplies only the subcommand (`sweep-tasks`,
# `reconcile --process ...`), so the manifests stay readable and cannot accidentally
# reach a different interpreter.
ENTRYPOINT ["python", "-m", "reconciler.run"]
