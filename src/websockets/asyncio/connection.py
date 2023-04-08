from __future__ import annotations

import asyncio
import logging
import random
import struct
import uuid
from types import TracebackType
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Type,
    Union,
)

from ..asyncio.deadline import Deadline
from ..exceptions import ConnectionClosed, ConnectionClosedOK
from ..frames import DATA_OPCODES, BytesLike, Frame, Opcode, prepare_ctrl
from ..http11 import Request, Response
from ..protocol import Event, Protocol
from ..typing import AsyncIterator, Data, LoggerLike, Subprotocol
from .messages import Assembler


__all__ = ["Connection"]

logger = logging.getLogger(__name__)


class Connection(asyncio.Protocol):
    """
    :mod:`asyncio` implementation of a WebSocket connection.

    :class:`Connection` provides APIs shared between WebSocket servers and
    clients.

    You shouldn't use it directly. Instead, use
    :class:`~websockets.asyncio.client.ClientConnection` or
    :class:`~websockets.asyncio.server.ServerConnection`.

    """

    def __init__(
        self,
        protocol: Protocol,
        *,
        close_timeout: Optional[float] = 10,
    ):
        self.protocol = protocol
        self.close_timeout = close_timeout

        # Inject reference to this instance in the protocol's logger.
        self.protocol.logger = logging.LoggerAdapter(
            self.protocol.logger,
            {"websocket": self},
        )

        # Copy attributes from the protocol for convenience.
        self.id: uuid.UUID = self.protocol.id
        """Unique identifier of the connection. Useful in logs."""
        self.logger: LoggerLike = self.protocol.logger
        """Logger for this connection."""
        self.debug = self.protocol.debug

        # HTTP handshake request and response.
        self.request: Optional[Request] = None
        """Opening handshake request."""
        self.response: Optional[Response] = None
        """Opening handshake response."""

        # Assembler turning frames into messages and serializing reads.
        self.recv_messages = Assembler()

        # Whether we are busy sending a fragmented message.
        self.send_in_progress = False

        # Deadline for the closing handshake.
        self.close_deadline: Optional[Deadline] = None

        # Mapping of ping IDs to pong waiters, in chronological order.
        self.pings: Dict[bytes, asyncio.Future] = {}

        # Exception raised in recv_events, to be chained to ConnectionClosed
        # in the user thread in order to show why the TCP connection dropped.
        self.recv_events_exc: Optional[BaseException] = None

    # Public attributes

    @property
    def local_address(self) -> Any:
        """
        Local address of the connection.

        For IPv4 connections, this is a ``(host, port)`` tuple.

        The format of the address depends on the address family.
        See :meth:`~socket.socket.getsockname`.

        """
        return self.transport.get_extra_info("sockname")

    @property
    def remote_address(self) -> Any:
        """
        Remote address of the connection.

        For IPv4 connections, this is a ``(host, port)`` tuple.

        The format of the address depends on the address family.
        See :meth:`~socket.socket.getpeername`.

        """
        return self.transport.get_extra_info("peername")

    @property
    def subprotocol(self) -> Optional[Subprotocol]:
        """
        Subprotocol negotiated during the opening handshake.

        :obj:`None` if no subprotocol was negotiated.

        """
        return self.protocol.subprotocol

    # Public methods

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.close(1000 if exc_type is None else 1011)

    async def __aiter__(self) -> Iterator[Data]:
        """
        Iterate on incoming messages.

        The iterator calls :meth:`recv` and yields messages in an infinite loop.

        It exits when the connection is closed normally. It raises a
        :exc:`~websockets.exceptions.ConnectionClosedError` exception after a
        protocol error or a network failure.

        """
        try:
            while True:
                yield await self.recv()
        except ConnectionClosedOK:
            return

    async def recv(self) -> Data:
        """
        Receive the next message.

        When the connection is closed, :meth:`recv` raises
        :exc:`~websockets.exceptions.ConnectionClosed`. Specifically, it raises
        :exc:`~websockets.exceptions.ConnectionClosedOK` after a normal closure
        and :exc:`~websockets.exceptions.ConnectionClosedError` after a protocol
        error or a network failure. This is how you detect the end of the
        message stream.

        Canceling :meth:`recv` is safe. There's no risk of losing the next
        message. The next invocation of :meth:`recv` will return it.

        This makes it possible to enforce a timeout by wrapping :meth:`recv` in
        :func:`~asyncio.timeout` or :func:`~asyncio.wait_for`.

        If the message is fragmented, wait until all fragments are received,
        reassemble them, and return the whole message.

        Returns:
            A string (:class:`str`) for a Text_ frame or a bytestring
            (:class:`bytes`) for a Binary_ frame.

            .. _Text: https://www.rfc-editor.org/rfc/rfc6455.html#section-5.6
            .. _Binary: https://www.rfc-editor.org/rfc/rfc6455.html#section-5.6

        Raises:
            ConnectionClosed: When the connection is closed.
            RuntimeError: If two coroutines call :meth:`recv` or
                :meth:`recv_streaming` concurrently.

        """
        try:
            return await self.recv_messages.get()
        except EOFError:
            raise self.protocol.close_exc from self.recv_events_exc
        except RuntimeError:
            raise RuntimeError(
                "cannot call recv while another coroutine "
                "is already running recv or recv_streaming"
            ) from None

    async def recv_streaming(self) -> AsyncIterator[Data]:
        """
        Receive the next message frame by frame.

        If the message is fragmented, yield each fragment as it is received.
        The iterator must be fully consumed, or else the connection will become
        unusable.

        :meth:`recv_streaming` raises the same exceptions as :meth:`recv`.

        Returns:
            An iterator of strings (:class:`str`) for a Text_ frame or
            bytestrings (:class:`bytes`) for a Binary_ frame.

            .. _Text: https://www.rfc-editor.org/rfc/rfc6455.html#section-5.6
            .. _Binary: https://www.rfc-editor.org/rfc/rfc6455.html#section-5.6

        Raises:
            ConnectionClosed: When the connection is closed.
            RuntimeError: If two threads call :meth:`recv` or
                :meth:`recv_streaming` concurrently.

        """
        try:
            async for frame in self.recv_messages.get_iter():
                yield frame
        except EOFError:
            raise self.protocol.close_exc from self.recv_events_exc
        except RuntimeError:
            raise RuntimeError(
                "cannot call recv_streaming while another thread "
                "is already running recv or recv_streaming"
            ) from None

    async def send(self, message: Union[Data, Iterable[Data]]) -> None:
        """
        Send a message.

        A string (:class:`str`) is sent as a Text_ frame. A bytestring or
        bytes-like object (:class:`bytes`, :class:`bytearray`, or
        :class:`memoryview`) is sent as a Binary_ frame.

        .. _Text: https://www.rfc-editor.org/rfc/rfc6455.html#section-5.6
        .. _Binary: https://www.rfc-editor.org/rfc/rfc6455.html#section-5.6

        :meth:`send` also accepts an iterable of strings, bytestrings, or
        bytes-like objects to enable fragmentation_. Each item is treated as a
        message fragment and sent in its own frame. All items must be of the
        same type, or else :meth:`send` will raise a :exc:`TypeError` and the
        connection will be closed.

        .. _fragmentation: https://www.rfc-editor.org/rfc/rfc6455.html#section-5.4

        :meth:`send` rejects dict-like objects because this is often an error.
        (If you really want to send the keys of a dict-like object as fragments,
        call its :meth:`~dict.keys` method and pass the result to :meth:`send`.)

        When the connection is closed, :meth:`send` raises
        :exc:`~websockets.exceptions.ConnectionClosed`. Specifically, it
        raises :exc:`~websockets.exceptions.ConnectionClosedOK` after a normal
        connection closure and
        :exc:`~websockets.exceptions.ConnectionClosedError` after a protocol
        error or a network failure.

        Args:
            message: Message to send.

        Raises:
            ConnectionClosed: When the connection is closed.
            RuntimeError: If the connection busy sending a fragmented message.
            TypeError: If ``message`` doesn't have a supported type.

        """
        # Unfragmented message -- this case must be handled first because
        # strings and bytes-like objects are iterable.

        if isinstance(message, str):
            with self.send_context():
                if self.send_in_progress:
                    raise RuntimeError(
                        "cannot call send while another thread "
                        "is already running send"
                    )
                self.protocol.send_text(message.encode("utf-8"))

        elif isinstance(message, BytesLike):
            with self.send_context():
                if self.send_in_progress:
                    raise RuntimeError(
                        "cannot call send while another thread "
                        "is already running send"
                    )
                self.protocol.send_binary(message)

        # Catch a common mistake -- passing a dict to send().

        elif isinstance(message, Mapping):
            raise TypeError("data is a dict-like object")

        # Fragmented message -- regular iterator.

        elif isinstance(message, Iterable):
            chunks = iter(message)
            try:
                chunk = next(chunks)
            except StopIteration:
                return

            try:
                # First fragment.
                if isinstance(chunk, str):
                    text = True
                    with self.send_context():
                        if self.send_in_progress:
                            raise RuntimeError(
                                "cannot call send while another thread "
                                "is already running send"
                            )
                        self.send_in_progress = True
                        self.protocol.send_text(
                            chunk.encode("utf-8"),
                            fin=False,
                        )
                elif isinstance(chunk, BytesLike):
                    text = False
                    with self.send_context():
                        if self.send_in_progress:
                            raise RuntimeError(
                                "cannot call send while another thread "
                                "is already running send"
                            )
                        self.send_in_progress = True
                        self.protocol.send_binary(
                            chunk,
                            fin=False,
                        )
                else:
                    raise TypeError("data iterable must contain bytes or str")

                # Other fragments
                for chunk in chunks:
                    if isinstance(chunk, str) and text:
                        with self.send_context():
                            assert self.send_in_progress
                            self.protocol.send_continuation(
                                chunk.encode("utf-8"),
                                fin=False,
                            )
                    elif isinstance(chunk, BytesLike) and not text:
                        with self.send_context():
                            assert self.send_in_progress
                            self.protocol.send_continuation(
                                chunk,
                                fin=False,
                            )
                    else:
                        raise TypeError("data iterable must contain uniform types")

                # Final fragment.
                with self.send_context():
                    self.protocol.send_continuation(b"", fin=True)
                    self.send_in_progress = False

            except RuntimeError:
                # We didn't start sending a fragmented message.
                raise

            except Exception:
                # We're half-way through a fragmented message and we can't
                # complete it. This makes the connection unusable.
                with self.send_context():
                    self.protocol.fail(1011, "error in fragmented message")
                raise

        else:
            raise TypeError("data must be bytes, str, or iterable")

    def close(self, code: int = 1000, reason: str = "") -> None:
        """
        Perform the closing handshake.

        :meth:`close` waits for the other end to complete the handshake and
        for the TCP connection to terminate.

        :meth:`close` is idempotent: it doesn't do anything once the
        connection is closed.

        Args:
            code: WebSocket close code.
            reason: WebSocket close reason.

        """
        try:
            # The context manager takes care of waiting for the TCP connection
            # to terminate after calling a method that sends a close frame.
            with self.send_context():
                if self.send_in_progress:
                    self.protocol.fail(1011, "close during fragmented message")
                else:
                    self.protocol.send_close(code, reason)
        except ConnectionClosed:
            # Ignore ConnectionClosed exceptions raised from send_context().
            # They mean that the connection is closed, which was the goal.
            pass

    def ping(self, data: Optional[Data] = None) -> asyncio.Future[None]:
        """
        Send a Ping_.

        .. _Ping: https://www.rfc-editor.org/rfc/rfc6455.html#section-5.5.2

        A ping may serve as a keepalive or as a check that the remote endpoint
        received all messages up to this point

        Args:
            data: Payload of the ping. A :class:`str` will be encoded to UTF-8.
                If ``data`` is :obj:`None`, the payload is four random bytes.

        Returns:
            An event that will be set when the corresponding pong is received.
            You can ignore it if you don't intend to wait.

            ::

                pong_event = ws.ping()
                pong_event.wait()  # only if you want to wait for the pong

        Raises:
            ConnectionClosed: When the connection is closed.
            RuntimeError: If another ping was sent with the same data and
                the corresponding pong wasn't received yet.

        """
        if data is not None:
            data = prepare_ctrl(data)

        with self.send_context():
            # Protect against duplicates if a payload is explicitly set.
            if data in self.pings:
                raise RuntimeError("already waiting for a pong with the same data")

            # Generate a unique random payload otherwise.
            while data is None or data in self.pings:
                data = struct.pack("!I", random.getrandbits(32))

            pong_waiter = self.loop.create_future()
            self.pings[data] = pong_waiter
            self.protocol.send_ping(data)
            return pong_waiter

    async def pong(self, data: Data = b"") -> None:
        """
        Send a Pong_.

        .. _Pong: https://www.rfc-editor.org/rfc/rfc6455.html#section-5.5.3

        An unsolicited pong may serve as a unidirectional heartbeat.

        Args:
            data: Payload of the pong. A :class:`str` will be encoded to UTF-8.

        Raises:
            ConnectionClosed: When the connection is closed.

        """
        data = prepare_ctrl(data)

        with self.send_context():
            self.protocol.send_pong(data)

    # Private methods

    def process_event(self, event: Event) -> None:
        """
        Process one incoming event.

        This method is overridden in subclasses to handle the handshake.

        """
        assert isinstance(event, Frame)
        if event.opcode in DATA_OPCODES:
            self.recv_messages.put(event)  # XXX

        if event.opcode is Opcode.PONG:
            self.acknowledge_pings(bytes(event.data))

    # asyncio.Protocol methods

    def connection_made(self, transport: asyncio.Transport) -> None:
        self.transport = transport
        # TODO set write buffer limits

    def connection_lost(self, exc: Optional[Exception]) -> None:
        ...

    # TODO error handling
    def data_received(self, data: bytes) -> None:
        # Feed incoming data to the protocol.
        self.protocol.receive_data(data)

        # This isn't expected to raise an exception.
        events = self.protocol.events_received()

        try:
            self.send_data()
        except Exception as exc:
            if self.debug:
                self.logger.debug("error while sending data", exc_info=True)
            # Similarly to the above, avoid overriding an exception
            # set by send_context(), in case of a race condition
            # i.e. send_context() closes the socket after recv()
            # returns above but before send_data() calls send().
            self.set_recv_events_exc(exc)

        if self.protocol.close_expected():
            # If the connection is expected to close soon, set the
            # close deadline based on the close timeout.
            if self.close_deadline is None:
                self.close_deadline = Deadline(self.close_timeout)

        for event in events:
            # This isn't expected to raise an exception.
            self.process_event(event)

    def eof_received(self) -> None:
        # Feed the end of the data stream to the connection.
        self.protocol.receive_eof()

        # This isn't expected to generate events.
        assert not self.protocol.events_received()

        # There is no error handling because send_data() can only write
        # the end of the data stream here and it handles errors itself.
        self.send_data()
