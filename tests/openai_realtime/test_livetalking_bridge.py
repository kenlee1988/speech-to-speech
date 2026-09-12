import asyncio

import httpx
import pytest

from speech_to_speech.api.openai_realtime.livetalking_bridge import LiveTalkingPCMBridge


@pytest.mark.asyncio
async def test_forwards_pcm_to_discovered_session():
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/admin/sessions":
            return httpx.Response(200, json={"code": 0, "data": {"sessions": [{"sessionid": "newest"}]}})
        return httpx.Response(200, json={"code": 0, "msg": "ok"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        bridge = LiveTalkingPCMBridge("http://livetalking", client=client)
        await bridge.start()
        bridge.enqueue(b"\x01\x00" * 320)
        await asyncio.wait_for(bridge._queue.join(), timeout=1)
        await bridge.close()

    assert [request.url.path for request in requests] == ["/api/admin/sessions", "/humanpcm"]
    assert requests[-1].url.params["sessionid"] == "newest"
    assert requests[-1].url.params["sample_rate"] == "16000"
    assert requests[-1].content == b"\x01\x00" * 320


@pytest.mark.asyncio
async def test_refreshes_stale_session_and_retries_pcm():
    discoveries = iter(("old", "new"))
    posted_sessions: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/admin/sessions":
            sid = next(discoveries)
            return httpx.Response(200, json={"code": 0, "data": {"sessions": [{"sessionid": sid}]}})
        sid = request.url.params["sessionid"]
        posted_sessions.append(sid)
        return httpx.Response(200, json={"code": 0 if sid == "new" else -1})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        bridge = LiveTalkingPCMBridge("http://livetalking", client=client)
        await bridge.start()
        bridge.enqueue(b"pcm")
        await asyncio.wait_for(bridge._queue.join(), timeout=1)
        await bridge.close()

    assert posted_sessions == ["old", "new"]


@pytest.mark.asyncio
async def test_network_failure_does_not_stop_worker():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        bridge = LiveTalkingPCMBridge("http://livetalking", client=client)
        await bridge.start()
        bridge.enqueue(b"pcm")
        await asyncio.wait_for(bridge._queue.join(), timeout=1)
        assert bridge._task is not None and not bridge._task.done()
        await bridge.close()


@pytest.mark.asyncio
async def test_missing_session_discovery_is_rate_limited():
    discoveries = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal discoveries
        discoveries += 1
        return httpx.Response(200, json={"code": 0, "data": {"sessions": []}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        bridge = LiveTalkingPCMBridge("http://livetalking", client=client)
        await bridge.start()
        for _ in range(20):
            bridge.enqueue(b"pcm")
        await asyncio.wait_for(bridge._queue.join(), timeout=1)
        await bridge.close()

    assert discoveries == 1
