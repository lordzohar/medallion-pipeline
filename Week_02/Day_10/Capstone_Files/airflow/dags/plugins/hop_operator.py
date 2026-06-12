"""Custom Airflow operator that runs work inside the `hop` container.

Modes:
  - "python"   (default): `docker exec hop python3 <script> <args>`
                Uses the Python reference transforms under hop/transforms/.
                Works out of the box — no Hop project configuration needed.
  - "hop-run":            `docker exec hop /opt/hop/hop-run.sh <args>`
                Runs the actual Hop workflow / pipeline. Requires the Hop
                project to be configured (one-time GUI step or via
                metadata files under hop/metadata/).

Picks up the target container name from env HOP_CONTAINER (default 'hop').
"""
from __future__ import annotations

import os
import shlex
from typing import Sequence

import docker
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


class HopOperator(BaseOperator):
    template_fields: Sequence[str] = ("script", "args", "workflow", "pipeline", "hop_params")

    def __init__(
        self,
        *,
        mode: str = "python",
        script: str | None = None,
        args: list[str] | None = None,
        workflow: str | None = None,
        pipeline: str | None = None,
        hop_params: dict[str, str] | None = None,
        container: str | None = None,
        timeout: int = 600,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if mode not in ("python", "hop-run"):
            raise ValueError(f"mode must be python|hop-run, got {mode}")
        self.mode      = mode
        self.script    = script
        self.args      = args or []
        self.workflow  = workflow
        self.pipeline  = pipeline
        self.hop_params = hop_params or {}
        self.container = container or os.environ.get("HOP_CONTAINER", "hop")
        self.timeout   = timeout

    def execute(self, context):
        client = docker.from_env()
        try:
            container = client.containers.get(self.container)
        except docker.errors.NotFound as e:
            raise AirflowException(f"Container '{self.container}' not running: {e}")

        if self.mode == "python":
            if not self.script:
                raise AirflowException("python mode requires 'script'")
            cmd = ["python3", f"/files/project/transforms/{self.script}", *self.args]
        else:
            if not (self.workflow or self.pipeline):
                raise AirflowException("hop-run mode requires 'workflow' or 'pipeline'")
            cmd = [
                "/opt/hop/hop-run.sh",
                "--environment=day10",
                "--project=day10",
                "--runconfig=local",
            ]
            if self.workflow:
                cmd.append(f"--workflow=/files/project/workflows/{self.workflow}")
            if self.pipeline:
                cmd.append(f"--file=/files/project/pipelines/{self.pipeline}")
            for k, v in self.hop_params.items():
                cmd.append(f"--parameters={k}={v}")

        self.log.info("Hop exec: %s", " ".join(shlex.quote(c) for c in cmd))
        exit_code, output = container.exec_run(
            cmd, demux=False, stream=False, tty=False,
        )
        out_text = output.decode("utf-8", errors="replace") if isinstance(output, (bytes, bytearray)) else str(output)
        self.log.info(out_text)
        if exit_code != 0:
            raise AirflowException(f"Hop step failed exit={exit_code}")
        return out_text
