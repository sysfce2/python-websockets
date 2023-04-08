from __future__ import annotations

import contextlib
import logging
import random
import socket
import struct
import threading
import uuid
from types import TracebackType
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional, Type, Union

from ..asyncio.deadline import Deadline
from ..exceptions import ConnectionClosed, ConnectionClosedOK, ProtocolError
from ..frames import DATA_OPCODES, BytesLike, CloseCode, Frame, Opcode, prepare_ctrl
from ..http11 import Request, Response
from ..protocol import CLOSED, OPEN, Event, Protocol, State
from ..typing import Data, LoggerLike, Subprotocol
from .messages import Assembler


__all__ = ["Connection"]

logger = logging.getLogger(__name__)


class Connection:
    """
    :mod:`threading` implementation of a WebSocket connection.

    :class:`Connection` provides APIs shared between WebSocket servers and
    clients.

    You shouldn't use it directly. Instead, use
    :class:`~websockets.sync.client.ClientConnection` or
    :class:`~websockets.sync.server.ServerConnection`.

    """

    recv_bufsize = 65536

    def __init__(
        self,
        socket: socket.socket,
        protocol: Protocol,
        *,
        close_timeout: Optional[float] = 10,
    ) -> None:
        self.socket = socket
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

        # Mutex serializing interactions with the protocol.
        self.protocol_mutex = threading.Lock()

        # Assembler turning frames into messages and serializing reads.
        self.recv_messages = Assembler()

        # Whether we are busy sending a fragmented message.
        self.send_in_progress = False

        # Deadline for the closing handshake.
        self.close_deadline: Optional[Deadline] = None

        # Mapping of ping IDs to pong waiters, in chronological order.
        self.pings: Dict[bytes, threading.Event] = {}

        # Receiving events from the socket.
        self.recv_events_thread = threading.Thread(target=self.recv_events)
        self.recv_events_thread.start()

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
        return self.socket.getsockname()

    @property
    def remote_address(self) -> Any:
        """
        Remote address of the connection.

        For IPv4 connections, this is a ``(host, port)`` tuple.

        The format of the address depends on the address family.
        See :meth:`~socket.socket.getpeername`.

        """
        return self.socket.getpeername()

    @property
    def subprotocol(self) -> Optional[Subprotocol]:
        """
        Subprotocol negotiated during the opening handshake.

        :obj:`None` if no subprotocol was negotiated.

        """
        return self.protocol.subprotocol

    # Public methods

    def __enter__(self) -> Connection:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if exc_type is None:
            self.close()
        else:
            self.close(CloseCode.INTERNAL_ERROR)

    def __iter__(self) -> Iterator[Data]:
        """
        Iterate on incoming messages.

        The iterator calls :meth:`recv` and yields messages in an infinite loop.

        It exits when the connection is closed normally. It raises a
        :exc:`~websockets.exceptions.ConnectionClosedError` exception after a
        protocol error or a network failure.

        """
        try:
            while True:
                yield self.recv()
        except ConnectionClosedOK:
            return

    def recv(self, timeout: Optional[float] = None) -> Data:
        """
        Receive the next message.

        When the connection is closed, :meth:`recv` raises
        :exc:`~websockets.exceptions.ConnectionClosed`. Specifically, it raises
        :exc:`~websockets.exceptions.ConnectionClosedOK` after a normal closure
        and :exc:`~websockets.exceptions.ConnectionClosedError` after a protocol
        error or a network failure. This is how you detect the end of the
        message stream.

        If ``timeout`` is :obj:`None`, block until a message is received. If
        ``timeout`` is set and no message is received within ``timeout``
        seconds, raise :exc:`TimeoutError`. Set ``timeout`` to ``0`` to check if
        a message was already received.

        If the message is fragmented, wait until all fragments are received,
        reassemble them, and return the whole message.

        Returns:
            A string (:class:`str`) for a Text_ frame or a bytestring
            (:class:`bytes`) for a Binary_ frame.

            .. _Text: https://www.rfc-editor.org/rfc/rfc6455.html#section-5.6
            .. _Binary: https://www.rfc-editor.org/rfc/rfc6455.html#section-5.6

        Raises:
            ConnectionClosed: When the connection is closed.
            RuntimeError: If two threads call :meth:`recv` or
                :meth:`recv_streaming` concurrently.

        """
        try:
            return self.recv_messages.get(timeout)
        except EOFError:
            raise self.protocol.close_exc from self.recv_events_exc
        except RuntimeError:
            raise RuntimeError(
                "cannot call recv while another thread "
                "is already running recv or recv_streaming"
            ) from None

    def recv_streaming(self) -> Iterator[Data]:
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
            for frame in self.recv_messages.get_iter():
                yield frame
        except EOFError:
            raise self.protocol.close_exc from self.recv_events_exc
        except RuntimeError:
            raise RuntimeError(
                "cannot call recv_streaming while another thread "
                "is already running recv or recv_streaming"
            ) from None

    def send(self, message: Union[Data, Iterable[Data]]) -> None:
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
            RuntimeError: If the connection is sending a fragmented message.
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
                    self.protocol.fail(
                        CloseCode.INTERNAL_ERROR,
                        "error in fragmented message",
                    )
                raise

        else:
            raise TypeError("data must be bytes, str, or iterable")

    def close(self, code: int = CloseCode.NORMAL_CLOSURE, reason: str = "") -> None:
        """
        Perform the closing handshake.

        :meth:`close` waits for the other end to complete the handshake, for the
        TCP connection to terminate, and for all incoming messages to be read
        with :meth:`recv`.

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
                    self.protocol.fail(
                        CloseCode.INTERNAL_ERROR,
                        "close during fragmented message",
                    )
                else:
                    self.protocol.send_close(code, reason)
        except ConnectionClosed:
            # Ignore ConnectionClosed exceptions raised from send_context().
            # They mean that the connection is closed, which was the goal.
            pass

    def ping(self, data: Optional[Data] = None) -> threading.Event:
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

            pong_waiter = threading.Event()
            self.pings[data] = pong_waiter
            self.protocol.send_ping(data)
            return pong_waiter

    def pong(self, data: Data = b"") -> None:
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
            self.recv_messages.put(event)

        if event.opcode is Opcode.PONG:
            self.acknowledge_pings(bytes(event.data))

    def acknowledge_pings(self, data: bytes) -> None:
        """
        Acknowledge pings when receiving a pong.

        """
        with self.protocol_mutex:
            # Ignore unsolicited pong.
            if data not in self.pings:
                return
            # Sending a pong for only the most recent ping is legal.
            # Acknowledge all previous pings too in that case.
            ping_id = None
            ping_ids = []
            for ping_id, ping in self.pings.items():
                ping_ids.append(ping_id)
                ping.set()
                if ping_id == data:
                    break
            else:
                raise AssertionError("solicited pong not found in pings")
            # Remove acknowledged pings from self.pings.
            for ping_id in ping_ids:
                del self.pings[ping_id]

    def recv_events(self) -> None:
        """
        Read incoming data from the socket and process events.

        Run this method in a thread as long as the connection is alive.

        ``recv_events()`` exits immediately when the ``self.socket`` is closed.

        """
        try:
            while True:
                try:
                    if self.close_deadline is not None:
                        self.socket.settimeout(self.close_deadline.timeout())
                    data = self.socket.recv(self.recv_bufsize)
                except Exception as exc:
                    if self.debug:
                        self.logger.debug("error while receiving data", exc_info=True)
                    # When the closing handshake is initiated by our side,
                    # recv() may block until send_context() closes the socket.
                    # In that case, send_context() already set recv_events_exc.
                    # Calling set_recv_events_exc() avoids overwriting it.
                    with self.protocol_mutex:
                        self.set_recv_events_exc(exc)
                    break

                if data == b"":
                    break

                # Acquire the connection lock.
                with self.protocol_mutex:
                    # Feed incoming data to the protocol.
                    self.protocol.receive_data(data)

                    # This isn't expected to raise an exception.
                    events = self.protocol.events_received()

                    # Write outgoing data to the socket.
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
                        break

                    if self.protocol.close_expected():
                        # If the connection is expected to close soon, set the
                        # close deadline based on the close timeout.
                        if self.close_deadline is None:
                            self.close_deadline = Deadline(self.close_timeout)

                # Unlock conn_mutex before processing events. Else, the
                # application can't send messages in response to events.

                # If self.send_data raised an exception, then events are lost.
                # Given that automatic responses write small amounts of data,
                # this should be uncommon, so we don't handle the edge case.

                try:
                    for event in events:
                        # This may raise EOFError if the closing handshake
                        # times out while a message is waiting to be read.
                        self.process_event(event)
                except EOFError:
                    break

            # Breaking out of the while True: ... loop means that we believe
            # that the socket doesn't work anymore.
            with self.protocol_mutex:
                # Feed the end of the data stream to the protocol.
                self.protocol.receive_eof()

                # This isn't expected to generate events.
                assert not self.protocol.events_received()

                # There is no error handling because send_data() can only write
                # the end of the data stream here and it handles errors itself.
                self.send_data()

        except Exception as exc:
            # This branch should never run. It's a safety net in case of bugs.
            self.logger.error("unexpected internal error", exc_info=True)
            with self.protocol_mutex:
                self.set_recv_events_exc(exc)
            # We don't know where we crashed. Force protocol state to CLOSED.
            self.protocol.state = CLOSED
        finally:
            # This isn't expected to raise an exception.
            self.close_socket()

    @contextlib.contextmanager
    def send_context(
        self,
        *,
        expected_state: State = OPEN,  # CONNECTING during the opening handshake
    ) -> Iterator[None]:
        """
        Create a context for writing to the connection from user code.

        On entry, :meth:`send_context` acquires the connection lock and checks
        that the connection is open; on exit, it writes outgoing data to the
        socket::

            with self.send_context():
                self.protocol.send_text(message.encode("utf-8"))

        When the connection isn't open on entry, when the connection is expected
        to close on exit, or when an unexpected error happens, terminating the
        connection, :meth:`send_context` waits until the connection is closed
        then raises :exc:`~websockets.exceptions.ConnectionClosed`.

        """
        # Should we wait until the connection is closed?
        wait_for_close = False
        # Should we close the socket and raise ConnectionClosed?
        raise_close_exc = False
        # What exception should we chain ConnectionClosed to?
        original_exc: Optional[BaseException] = None

        # Acquire the protocol lock.
        with self.protocol_mutex:
            if self.protocol.state is expected_state:
                # Let the caller interact with the protocol.
                try:
                    yield
                except (ProtocolError, RuntimeError):
                    # The protocol state wasn't changed. Exit immediately.
                    raise
                except Exception as exc:
                    self.logger.error("unexpected internal error", exc_info=True)
                    # This branch should never run. It's a safety net in case of
                    # bugs. Since we don't know what happened, we will close the
                    # connection and raise the exception to the caller.
                    wait_for_close = False
                    raise_close_exc = True
                    original_exc = exc
                else:
                    # Check if the connection is expected to close soon.
                    if self.protocol.close_expected():
                        wait_for_close = True
                        # If the connection is expected to close soon, set the
                        # close deadline based on the close timeout.

                        # Since we tested earlier that protocol.state was OPEN
                        # (or CONNECTING) and we didn't release protocol_mutex,
                        # it is certain that self.close_deadline is still None.
                        assert self.close_deadline is None
                        self.close_deadline = Deadline(self.close_timeout)
                    # Write outgoing data to the socket.
                    try:
                        self.send_data()
                    except Exception as exc:
                        if self.debug:
                            self.logger.debug("error while sending data", exc_info=True)
                        # While the only expected exception here is OSError,
                        # other exceptions would be treated identically.
                        wait_for_close = False
                        raise_close_exc = True
                        original_exc = exc

            else:  # self.protocol.state is not expected_state
                # Minor layering violation: we assume that the connection
                # will be closing soon if it isn't in the expected state.
                wait_for_close = True
                raise_close_exc = True

        # To avoid a deadlock, release the connection lock by exiting the
        # context manager before waiting for recv_events() to terminate.

        # If the connection is expected to close soon and the close timeout
        # elapses, close the socket to terminate the connection.
        if wait_for_close:
            if self.close_deadline is None:
                timeout = self.close_timeout
            else:
                # Thread.join() returns immediately if timeout is negative.
                timeout = self.close_deadline.timeout(raise_if_elapsed=False)
            self.recv_events_thread.join(timeout)

            if self.recv_events_thread.is_alive():
                # There's no risk to overwrite another error because
                # original_exc is never set when wait_for_close is True.
                assert original_exc is None
                original_exc = TimeoutError("timed out while closing connection")
                # Set recv_events_exc before closing the socket in order to get
                # proper exception reporting.
                raise_close_exc = True
                with self.protocol_mutex:
                    self.set_recv_events_exc(original_exc)

        # If an error occurred, close the socket to terminate the connection and
        # raise an exception.
        if raise_close_exc:
            self.close_socket()
            self.recv_events_thread.join()
            raise self.protocol.close_exc from original_exc

    def send_data(self) -> None:
        """
        Send outgoing data.

        This method requires holding protocol_mutex.

        Raises:
            OSError: When a socket operations fails.

        """
        assert self.protocol_mutex.locked()
        for data in self.protocol.data_to_send():
            if data:
                if self.close_deadline is not None:
                    self.socket.settimeout(self.close_deadline.timeout())
                self.socket.sendall(data)
            else:
                try:
                    self.socket.shutdown(socket.SHUT_WR)
                except OSError:  # socket already closed
                    pass

    def set_recv_events_exc(self, exc: Optional[BaseException]) -> None:
        """
        Set recv_events_exc, if not set yet.

        This method requires holding protocol_mutex.

        """
        assert self.protocol_mutex.locked()
        if self.recv_events_exc is None:
            self.recv_events_exc = exc

    def close_socket(self) -> None:
        """
        Shutdown and close socket. Close message assembler.

        Calling close_socket() guarantees that recv_events() terminates. Indeed,
        recv_events() may block only on socket.recv() or on recv_messages.put().

        """
        # shutdown() is required to interrupt recv() on Linux.
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # socket is already closed
        self.socket.close()
        self.recv_messages.close()
