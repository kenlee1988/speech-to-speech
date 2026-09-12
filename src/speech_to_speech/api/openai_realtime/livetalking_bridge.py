"""Best-effort forwarding of pipeline PCM to a local LiveTalking session."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class LiveTalkingPCMBridge:
    """Forward PCM without ever applying backpressure to the speech client."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float = 1.0,
        queue_size: int = 64,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self._queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=queue_size)
        self._client = client
        self._owns_client = client is None
        self._task: asyncio.Task[None] | None = None
        self._session_id: str | None = None
        self._next_discovery_at = 0.0
        self._dropped_chunks = 0

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout_s)
        if self._task is None:
            self._task = asyncio.create_task(self._worker())
        logger.info("LiveTalking PCM bridge enabled: %s", self.base_url)

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    def enqueue(self, pcm: bytes) -> None:
        """Queue a pipeline-rate PCM16 chunk, dropping it if the bridge lags."""
        if self._task is None or not pcm:
            return
        try:
            self._queue.put_nowait(pcm)
        except asyncio.QueueFull:
            self._dropped_chunks += 1
            if self._dropped_chunks == 1 or self._dropped_chunks % 100 == 0:
                logger.warning(
                    "LiveTalking PCM bridge queue full; dropped %d chunk(s)",
                    self._dropped_chunks,
                )

    async def _discover_session(self) -> str | None:
        now = time.monotonic()
        if now < self._next_discovery_at:
            return None
        self._next_discovery_at = now + 1.0
        assert self._client is not None
        response = await self._client.get(f"{self.base_url}/api/admin/sessions")
        response.raise_for_status()
        payload: dict[str, Any] = response.json()
        sessions = payload.get("data", {}).get("sessions", []) if payload.get("code") == 0 else []
        if not sessions:
            return None
        # SessionManager preserves insertion order; the last entry is the most
        # recently connected browser when more than one session exists.
        session_id = str(sessions[-1].get("sessionid", ""))
        return session_id or None

    async def _post_pcm(self, pcm: bytes) -> bool:
        assert self._client is not None
        assert self._session_id is not None

        response = await self._client.post(
            f"{self.base_url}/humanpcm",
            params={"sessionid": self._session_id, "sample_rate": 16_000},
            content=pcm,
            headers={"content-type": "application/octet-stream"},
        )
        response.raise_for_status()
        payload = response.json()
        return payload.get("code") == 0

    async def _forward(self, pcm: bytes) -> None:
        if self._session_id is None:
            self._session_id = await self._discover_session()
        if self._session_id is None:
            return
        if await self._post_pcm(pcm):
            return
        # A browser reconnect changes the LiveTalking session id. Clear the
        # cached id and retry once after rediscovery.
        self._session_id = None
        self._next_discovery_at = 0.0
        self._session_id = await self._discover_session()
        if self._session_id is not None:
            await self._post_pcm(pcm)

    async def _worker(self) -> None:
        while True:
            pcm = await self._queue.get()
            try:
                await self._forward(pcm)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - bridge must be fail-open
                self._session_id = None
                logger.warning("LiveTalking PCM forwarding failed: %s", exc)
            finally:
                self._queue.task_done()


def bridge_from_env() -> LiveTalkingPCMBridge | None:
    enabled = os.getenv("LIVETALKING_PCM_BRIDGE", "0").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        return None
    return LiveTalkingPCMBridge(
        os.getenv("LIVETALKING_URL", "http://127.0.0.1:8010"),
        timeout_s=float(os.getenv("LIVETALKING_PCM_TIMEOUT", "1.0")),
        queue_size=int(os.getenv("LIVETALKING_PCM_QUEUE_SIZE", "64")),
    )
