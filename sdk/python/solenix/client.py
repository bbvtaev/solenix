"""
solenix-core Python SDK

Пример использования:

    from solenix import Client

    client = Client("127.0.0.1:8731")

    client.write("cpu.usage", {"host": "srv1"}, 72.5)

    results = client.query("cpu.usage", from_ns=0, to_ns=0)
    for series in results:
        print(series.metric, series.points)

    # real-time подписка
    for point in client.subscribe("cpu.usage"):
        print(point.timestamp, point.value)

    client.close()

Зависимости:
    pip install grpcio protobuf

Генерация proto-стабов (один раз из корня репо):
    cd sdk/python
    ./generate_proto.sh
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Generator, Iterator

import grpc

from .proto import solenix_pb2, solenix_pb2_grpc


@dataclass
class Point:
    timestamp: int   # Unix nanoseconds
    value: float


@dataclass
class SeriesResult:
    metric: str
    labels: dict[str, str]
    points: list[Point] = field(default_factory=list)


class Client:
    """gRPC клиент для solenix-core."""

    def __init__(self, addr: str = "127.0.0.1:8731", timeout: float = 5.0) -> None:
        self._channel = grpc.insecure_channel(addr)
        self._stub = solenix_pb2_grpc.SolenixDBStub(self._channel)
        self._timeout = timeout

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    # ── Write ──────────────────────────────────────────────────────────────────

    def write(
        self,
        metric: str,
        labels: dict[str, str] | None = None,
        value: float = 0.0,
    ) -> None:
        """Записывает одно значение с текущим timestamp (UnixNano)."""
        self.write_batch(
            metric,
            labels,
            [Point(timestamp=time.time_ns(), value=value)],
        )

    def write_batch(
        self,
        metric: str,
        labels: dict[str, str] | None = None,
        points: list[Point] | None = None,
    ) -> None:
        """Записывает несколько точек с произвольными timestamp."""
        pb_points = [
            solenix_pb2.DataPoint(timestamp=p.timestamp, value=p.value)
            for p in (points or [])
        ]
        self._stub.Write(
            solenix_pb2.WriteRequest(
                series=[
                    solenix_pb2.Series(
                        metric=metric,
                        labels=labels or {},
                        points=pb_points,
                    )
                ]
            ),
            timeout=self._timeout,
        )

    # ── Query ──────────────────────────────────────────────────────────────────

    def query(
        self,
        metric: str,
        labels: dict[str, str] | None = None,
        from_ns: int = 0,
        to_ns: int = 0,
    ) -> list[SeriesResult]:
        """Запрашивает данные по диапазону. from_ns/to_ns = 0 — без ограничения."""
        resp = self._stub.Query(
            solenix_pb2.QueryRequest(
                metric=metric,
                labels=labels or {},
                from_=from_ns,
                to=to_ns,
            ),
            timeout=self._timeout,
        )
        return [
            SeriesResult(
                metric=s.metric,
                labels=dict(s.labels),
                points=[Point(p.timestamp, p.value) for p in s.points],
            )
            for s in resp.series
        ]

    # ── Delete ─────────────────────────────────────────────────────────────────

    def delete(
        self,
        metric: str,
        labels: dict[str, str] | None = None,
        from_ns: int = 0,
        to_ns: int = 0,
    ) -> None:
        self._stub.Delete(
            solenix_pb2.DeleteRequest(
                metric=metric,
                labels=labels or {},
                from_=from_ns,
                to=to_ns,
            ),
            timeout=self._timeout,
        )

    # ── Subscribe ──────────────────────────────────────────────────────────────

    def subscribe(
        self,
        metric: str,
        labels: dict[str, str] | None = None,
    ) -> Iterator[Point]:
        """
        Генератор новых точек в реальном времени.

        Пример:
            for point in client.subscribe("cpu.usage", {"host": "srv1"}):
                print(point.value)
        """
        stream = self._stub.Subscribe(
            solenix_pb2.SubscribeRequest(metric=metric, labels=labels or {})
        )
        for p in stream:
            yield Point(timestamp=p.timestamp, value=p.value)

    # ── Health ─────────────────────────────────────────────────────────────────

    def health(self) -> tuple[str, str]:
        """Возвращает (status, version) сервера."""
        resp = self._stub.Health(solenix_pb2.HealthRequest(), timeout=self._timeout)
        return resp.status, resp.version
