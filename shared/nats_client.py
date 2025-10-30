"""
Async NATS client wrapper with auto-reconnect and error handling.
Provides publish/subscribe and request/reply patterns.
"""

import asyncio
import json
from typing import Any, Callable, Optional, Dict

import nats
from nats.aio.msg import Msg
from pydantic import BaseModel

from shared.logger import get_logger

logger = get_logger(__name__)


class NATSClient:
    """Async NATS client wrapper."""

    def __init__(
        self,
        url: str = "nats://localhost:4222",
        name: str = "service",
        max_reconnect_attempts: int = 10,
        reconnect_wait_sec: float = 1.0,
        request_timeout_sec: float = 5.0,
    ):
        """
        Initialize NATS client.

        Args:
            url: NATS server URL.
            name: Client name for logging/identification.
            max_reconnect_attempts: Max reconnection attempts.
            reconnect_wait_sec: Wait time between reconnections.
            request_timeout_sec: Request timeout in seconds.
        """
        self.url = url
        self.name = name
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_wait_sec = reconnect_wait_sec
        self.request_timeout_sec = request_timeout_sec

        self.nc: Optional[nats.NATS] = None
        self.js: Optional[nats.aio.jetstream.JetStreamContext] = None
        self._subscriptions: Dict[str, Any] = {}
        self._connected = False

    async def connect(self) -> None:
        """
        Connect to NATS server with auto-reconnect.

        Raises:
            nats.Error: If connection fails after max retries.
        """
        if self._connected:
            logger.info("Already connected to NATS")
            return

        try:
            self.nc = await nats.connect(
                self.url,
                name=self.name,
                max_reconnect_attempts=self.max_reconnect_attempts,
                reconnect_wait=self.reconnect_wait_sec,
            )

            # Enable JetStream
            self.js = self.nc.jetstream()

            self._connected = True
            logger.info("Connected to NATS", url=self.url, name=self.name)

        except Exception as e:
            logger.error("Failed to connect to NATS", error=str(e), url=self.url)
            raise

    async def close(self) -> None:
        """Close NATS connection."""
        if self.nc:
            await self.nc.close()
            self._connected = False
            logger.info("Closed NATS connection")

    async def publish(
        self,
        subject: str,
        data: Any,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Publish message to NATS.

        Args:
            subject: Subject/topic (e.g., "features.v1").
            data: Message data (Pydantic model or dict).
            timeout: Publish timeout in seconds.

        Raises:
            RuntimeError: If not connected.
            nats.Error: On publish error.
        """
        if not self._connected or not self.nc:
            raise RuntimeError("Not connected to NATS")

        try:
            # Serialize data
            if isinstance(data, BaseModel):
                payload = data.model_dump_json().encode()
            elif isinstance(data, dict):
                payload = json.dumps(data).encode()
            elif isinstance(data, str):
                payload = data.encode()
            else:
                payload = str(data).encode()

            # Publish with timeout
            if timeout:
                await asyncio.wait_for(
                    self.nc.publish(subject, payload),
                    timeout=timeout,
                )
            else:
                await self.nc.publish(subject, payload)

            logger.debug("Published message", subject=subject, size_bytes=len(payload))

        except asyncio.TimeoutError:
            logger.error("Publish timeout", subject=subject)
            raise
        except Exception as e:
            logger.error("Publish error", subject=subject, error=str(e))
            raise

    async def subscribe(
        self,
        subject: str,
        callback: Callable[[Msg], Any],
        queue_group: Optional[str] = None,
    ) -> str:
        """
        Subscribe to NATS subject.

        Args:
            subject: Subject/topic to subscribe to.
            callback: Async callback function that processes messages.
            queue_group: Optional queue group name for load balancing.

        Returns:
            Subscription ID.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self.nc:
            raise RuntimeError("Not connected to NATS")

        try:
            sub_id = f"{subject}:{len(self._subscriptions)}"

            async def handler(msg: Msg) -> None:
                try:
                    await callback(msg)
                except Exception as e:
                    logger.error(
                        "Callback error",
                        subject=subject,
                        error=str(e),
                    )

            sub = await self.nc.subscribe(
                subject,
                cb=handler,
                queue=queue_group,
            )

            self._subscriptions[sub_id] = sub
            logger.info("Subscribed", subject=subject, sub_id=sub_id)

            return sub_id

        except Exception as e:
            logger.error("Subscribe error", subject=subject, error=str(e))
            raise

    async def unsubscribe(self, sub_id: str) -> None:
        """
        Unsubscribe from a subscription.

        Args:
            sub_id: Subscription ID returned by subscribe().
        """
        if sub_id in self._subscriptions:
            sub = self._subscriptions[sub_id]
            await sub.unsubscribe()
            del self._subscriptions[sub_id]
            logger.info("Unsubscribed", sub_id=sub_id)

    async def request(
        self,
        subject: str,
        data: Any,
        timeout: Optional[float] = None,
    ) -> Msg:
        """
        Send request and wait for reply (request/reply pattern).

        Args:
            subject: Subject to send request to.
            data: Request data (Pydantic model or dict).
            timeout: Request timeout in seconds.

        Returns:
            Response message.

        Raises:
            RuntimeError: If not connected.
            nats.Error: On request error.
        """
        if not self._connected or not self.nc:
            raise RuntimeError("Not connected to NATS")

        try:
            # Serialize data
            if isinstance(data, BaseModel):
                payload = data.model_dump_json().encode()
            elif isinstance(data, dict):
                payload = json.dumps(data).encode()
            else:
                payload = str(data).encode()

            # Send request
            timeout_sec = timeout or self.request_timeout_sec
            response = await self.nc.request(subject, payload, timeout=timeout_sec)

            logger.debug("Request completed", subject=subject)
            return response

        except asyncio.TimeoutError:
            logger.error("Request timeout", subject=subject)
            raise
        except Exception as e:
            logger.error("Request error", subject=subject, error=str(e))
            raise

    def is_connected(self) -> bool:
        """Check if connected to NATS."""
        return self._connected and self.nc is not None

    async def flush(self, timeout: Optional[float] = None) -> None:
        """
        Flush pending messages.

        Args:
            timeout: Flush timeout in seconds.
        """
        if self.nc:
            if timeout:
                await asyncio.wait_for(self.nc.flush(), timeout=timeout)
            else:
                await self.nc.flush()

    async def ping(self) -> bool:
        """
        Check if server is reachable with PING.

        Returns:
            True if ping successful.
        """
        try:
            if not self.nc:
                return False
            await self.nc.flush(timeout=1)
            return True
        except Exception:
            return False
