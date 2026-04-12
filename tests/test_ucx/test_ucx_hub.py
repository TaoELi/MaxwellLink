import asyncio
import inspect
import json
import queue
import threading

import numpy as np
import pytest

from maxwelllink.mxl_drivers.python.mxl_driver_ucx import run_driver_ucx_async
from maxwelllink.mxl_drivers.python.models.dummy_model import DummyModel
from maxwelllink.sockets.ucx_hub import SocketHubUCX
from maxwelllink.sockets.ucx_protocol import (
    INIT,
    STEP_REQUEST,
    pack_hello,
    unpack_init,
    unpack_message,
    unpack_step_request,
)

_CLOSE_SENTINEL = object()


@pytest.fixture(autouse=True)
def _force_master_rank(monkeypatch):
    monkeypatch.setattr("maxwelllink.sockets.ucx_hub.am_master", lambda: True)


class _AsyncThread(threading.Thread):
    def __init__(self, coro_factory):
        super().__init__(daemon=True)
        self._coro_factory = coro_factory
        self.exc = None

    def run(self):
        try:
            asyncio.run(self._coro_factory())
        except BaseException as exc:
            self.exc = exc

    def join_and_raise(self, timeout=2.0):
        self.join(timeout=timeout)
        assert not self.is_alive(), "background async thread did not exit"
        if self.exc is not None:
            raise self.exc


class FakeEndpoint:
    def __init__(self):
        self._inbound = queue.Queue()
        self._peer = None
        self._closed = False

    def attach_peer(self, peer):
        self._peer = peer

    async def am_send(self, payload):
        if self._closed or self._peer is None or self._peer._closed:
            raise ConnectionError("endpoint closed")
        self._peer._inbound.put(bytes(payload))

    async def am_recv(self):
        item = await asyncio.to_thread(self._inbound.get)
        if item is _CLOSE_SENTINEL:
            raise ConnectionError("endpoint closed")
        return item

    async def close(self):
        if self._closed:
            return
        self._closed = True
        if self._peer is not None and not self._peer._closed:
            self._peer._inbound.put(_CLOSE_SENTINEL)

    async def abort(self):
        await self.close()


class FakeListener:
    def __init__(self, module, callback, port, loop):
        self._module = module
        self.callback = callback
        self.port = port
        self.ip = "127.0.0.1"
        self.loop = loop
        self._closed = False

    def close(self):
        if self._closed:
            return
        self._closed = True
        with self._module._lock:
            self._module._listeners.pop(self.port, None)


class FakeUCXModule:
    def __init__(self):
        self._listeners = {}
        self._next_port = 24000
        self._lock = threading.Lock()

    def init(self, *args, **kwargs):
        return None

    def reset(self):
        with self._lock:
            self._listeners.clear()

    def get_address(self, ifname=None):
        del ifname
        return "127.0.0.1"

    def create_listener(self, callback, port=0):
        loop = asyncio.get_running_loop()
        with self._lock:
            actual_port = int(port or self._next_port)
            if actual_port in self._listeners:
                raise RuntimeError(f"listener already exists on port {actual_port}")
            if not port:
                self._next_port += 1
            listener = FakeListener(self, callback, actual_port, loop)
            self._listeners[actual_port] = listener
        return listener

    async def create_endpoint(self, address=None, port=None, ip_address=None):
        del address, ip_address
        with self._lock:
            listener = self._listeners[int(port)]

        client_ep = FakeEndpoint()
        server_ep = FakeEndpoint()
        client_ep.attach_peer(server_ep)
        server_ep.attach_peer(client_ep)

        def _dispatch():
            result = listener.callback(server_ep)
            if inspect.isawaitable(result):
                asyncio.create_task(result)

        if listener.loop is asyncio.get_running_loop():
            _dispatch()
        else:
            listener.loop.call_soon_threadsafe(_dispatch)
        return client_ep


class EchoModel(DummyModel):
    def __init__(self, scale=1.0, tag="driver"):
        super().__init__(verbose=False)
        self.scale = float(scale)
        self.tag = str(tag)
        self._last_field = np.zeros(3, dtype=float)

    def propagate(self, effective_efield_vec):
        self._last_field = np.asarray(effective_efield_vec, dtype=float).copy()
        self.t += self.dt

    def calc_amp_vector(self):
        return self.scale * self._last_field

    def append_additional_data(self):
        return {"tag": self.tag, "time_au": self.t}


async def _disconnect_after_first_step(address, port, ucx_module):
    endpoint = await ucx_module.create_endpoint(address=address, port=port)
    try:
        await endpoint.am_send(pack_hello({"hostname": "reconnector", "pid": 1}))

        init_msg = unpack_message(await endpoint.am_recv())
        assert init_msg.opcode == INIT
        unpack_init(init_msg.payload)

        step_msg = unpack_message(await endpoint.am_recv())
        assert step_msg.opcode == STEP_REQUEST
        unpack_step_request(step_msg.payload)
    finally:
        await endpoint.close()


def test_sockethub_ucx_multidriver_step_barrier():
    fake_ucx = FakeUCXModule()
    hub = SocketHubUCX(
        host="127.0.0.1",
        port=0,
        timeout=2.0,
        latency=1e-4,
        _ucx_module=fake_ucx,
    )

    threads = []
    try:
        mid0 = hub.register_molecule_return_id()
        mid1 = hub.register_molecule_return_id()

        threads.append(
            _AsyncThread(
                lambda: run_driver_ucx_async(
                    address=hub.host,
                    port=hub.port,
                    driver=EchoModel(scale=2.0, tag="a"),
                    ucx_module=fake_ucx,
                )
            )
        )
        threads.append(
            _AsyncThread(
                lambda: run_driver_ucx_async(
                    address=hub.host,
                    port=hub.port,
                    driver=EchoModel(scale=-1.0, tag="b"),
                    ucx_module=fake_ucx,
                )
            )
        )
        for th in threads:
            th.start()

        init_payloads = {
            mid0: {"molecule_id": mid0, "dt_au": 0.25},
            mid1: {"molecule_id": mid1, "dt_au": 0.25},
        }
        assert hub.wait_until_bound(init_payloads, require_init=True, timeout=2.0)

        requests = {
            mid0: {
                "efield_au": [1.0, 2.0, 3.0],
                "meta": {"t": 0.0},
                "init": init_payloads[mid0],
            },
            mid1: {
                "efield_au": [4.0, 5.0, 6.0],
                "meta": {"t": 0.0},
                "init": init_payloads[mid1],
            },
        }
        responses = hub.step_barrier(requests)
        seen_tags = set()
        for mid, response in responses.items():
            extra = json.loads(response["extra"].decode("utf-8"))
            tag = extra["tag"]
            seen_tags.add(tag)
            scale = {"a": 2.0, "b": -1.0}[tag]
            np.testing.assert_allclose(
                response["amp"], scale * np.asarray(requests[mid]["efield_au"])
            )
            assert extra["time_au"] == 0.25
        assert seen_tags == {"a", "b"}
    finally:
        hub.stop()
        for th in threads:
            th.join_and_raise()


def test_sockethub_ucx_reconnect_recovers_frozen_barrier():
    fake_ucx = FakeUCXModule()
    hub = SocketHubUCX(
        host="127.0.0.1",
        port=0,
        timeout=2.0,
        latency=1e-4,
        _ucx_module=fake_ucx,
    )

    reconnect_thread = _AsyncThread(
        lambda: _disconnect_after_first_step(hub.host, hub.port, fake_ucx)
    )
    replacement_thread = _AsyncThread(
        lambda: run_driver_ucx_async(
            address=hub.host,
            port=hub.port,
            driver=EchoModel(scale=3.0, tag="replacement"),
            ucx_module=fake_ucx,
        )
    )

    try:
        mid = hub.register_molecule_return_id()
        init_payload = {mid: {"molecule_id": mid, "dt_au": 0.5}}
        request = {
            mid: {
                "efield_au": [1.0, 0.0, -1.0],
                "meta": {"t": 0.0},
                "init": init_payload[mid],
            }
        }

        reconnect_thread.start()
        assert hub.wait_until_bound(init_payload, require_init=True, timeout=2.0)
        assert hub.step_barrier(request) == {}
        assert hub.paused is True

        reconnect_thread.join_and_raise()

        replacement_thread.start()
        assert hub.wait_until_bound(init_payload, require_init=True, timeout=2.0)
        responses = hub.step_barrier(request)
        np.testing.assert_allclose(responses[mid]["amp"], [3.0, 0.0, -3.0])
        extra = json.loads(responses[mid]["extra"].decode("utf-8"))
        assert extra == {"tag": "replacement", "time_au": 0.5}
    finally:
        hub.stop()
        replacement_thread.join_and_raise()
