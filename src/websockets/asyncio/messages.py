from __future__ import annotations

import asyncio
import codecs
from typing import AsyncIterator, List, Optional

from ..frames import Frame, Opcode
from ..typing import Data


__all__ = ["Assembler"]

UTF8Decoder = codecs.getincrementaldecoder("utf-8")


class Assembler:
    """
    Assemble messages from frames.

    """

    def __init__(self) -> None:
        self.loop = asyncio.get_event_loop()

        # We create a latch with two futures to ensure proper interleaving of
        # writing and reading messages.
        # put() sets this future to tell get() that a message can be fetched.
        self.message_complete: asyncio.Future[None] = self.loop.create_future()
        # get() sets this future to let put() that the message was fetched.
        self.message_fetched: asyncio.Future[None] = self.loop.create_future()

        # This flag prevents concurrent calls to get() by user code.
        self.get_in_progress = False
        # This flag prevents concurrent calls to put() by library code.
        self.put_in_progress = False

        # Decoder for text frames, None for binary frames.
        self.decoder: Optional[codecs.IncrementalDecoder] = None

        # Buffer of frames belonging to the same message.
        self.chunks: List[Data] = []

        # When switching from "buffering" to "streaming", we use a queue for
        # transferring frames from the writing coroutine (library code) to the
        # reading coroutine (user code). We're buffering when chunks_queue is
        # None and streaming when it's a Queue. None is a sentinel value marking
        # the end of the message, superseding message_complete.

        # Stream data from frames belonging to the same message.
        self.chunks_queue: Optional[asyncio.Queue[Optional[Data]]] = None

        # This flag marks the end of the connection.
        self.closed = False

    async def get(self) -> Data:
        """
        Read the next message.

        :meth:`get` returns a single :class:`str` or :class:`bytes`.

        If the message is fragmented, :meth:`get` waits until the last frame is
        received, then it reassembles the message and returns it. To receive
        messages frame by frame, use :meth:`get_iter` instead.

        Raises:
            EOFError: If the stream of frames has ended.
            RuntimeError: If two coroutines run :meth:`get` or :meth:`get_iter`
                concurrently.

        """
        if self.closed:
            raise EOFError("stream of frames ended")

        if self.get_in_progress:
            raise RuntimeError("get or get_iter is already running")

        # If the message_complete future isn't set yet, yield control to allow
        # put() to run and eventually set it.
        # Locking with get_in_progress ensures only one coroutine can get here.
        self.get_in_progress = True
        try:
            await self.message_complete
        finally:
            self.get_in_progress = False

        # get() was unblocked by close() rather than put().
        if self.closed:
            raise EOFError("stream of frames ended")

        assert self.message_complete.done()
        self.message_complete = self.loop.create_future()

        joiner: Data = b"" if self.decoder is None else ""
        # mypy cannot figure out that chunks have the proper type.
        message: Data = joiner.join(self.chunks)  # type: ignore

        self.chunks = []
        assert self.chunks_queue is None

        self.message_fetched.set_result(None)

        return message

    async def get_iter(self) -> AsyncIterator[Data]:
        """
        Stream the next message.

        Iterating the return value of :meth:`get_iter` asynchronously yields a
        :class:`str` or :class:`bytes` for each frame in the message.

        The iterator must be fully consumed before calling :meth:`get_iter` or
        :meth:`get` again. Else, :exc:`RuntimeError` is raised.

        This method only makes sense for fragmented messages. If messages aren't
        fragmented, use :meth:`get` instead.

        Raises:
            EOFError: If the stream of frames has ended.
            RuntimeError: If two coroutines run :meth:`get` or :meth:`get_iter`
                concurrently.

        """
        if self.closed:
            raise EOFError("stream of frames ended")

        if self.get_in_progress:
            raise RuntimeError("get or get_iter is already running")

        chunks = self.chunks
        self.chunks = []
        self.chunks_queue = asyncio.Queue()

        # Sending None in chunk_queue supersedes setting message_complete
        # when switching to "streaming". If message is already complete
        # when the switch happens, put() didn't send None, so we have to.
        if self.message_complete.done():
            self.chunks_queue.put_nowait(None)

        # Locking with get_in_progress ensures only one coroutine can get here.
        self.get_in_progress = True
        try:
            chunk: Optional[Data]
            for chunk in chunks:
                yield chunk
            while (chunk := await self.chunks_queue.get()) is not None:
                yield chunk
        finally:
            self.get_in_progress = False

        # get_iter() was unblocked by close() rather than put().
        if self.closed:
            raise EOFError("stream of frames ended")

        assert self.message_complete.done()
        self.message_complete = self.loop.create_future()

        assert self.chunks == []
        self.chunks_queue = None

        self.message_fetched.set_result(None)

    async def put(self, frame: Frame) -> None:
        """
        Add ``frame`` to the next message.

        When ``frame`` is the final frame in a message, :meth:`put` waits until
        the message is fetched, which can be achieved by calling :meth:`get` or
        by fully consuming the return value of :meth:`get_iter`.

        :meth:`put` assumes that the stream of frames respects the protocol. If
        it doesn't, the behavior is undefined.

        Raises:
            EOFError: If the stream of frames has ended.
            RuntimeError: If two coroutines run :meth:`put` concurrently.

        """
        if self.closed:
            raise EOFError("stream of frames ended")

        if self.put_in_progress:
            raise RuntimeError("put is already running")

        if frame.opcode is Opcode.TEXT:
            self.decoder = UTF8Decoder(errors="strict")
        elif frame.opcode is Opcode.BINARY:
            self.decoder = None
        elif frame.opcode is Opcode.CONT:
            pass
        else:
            # Ignore control frames.
            return

        data: Data
        if self.decoder is not None:
            data = self.decoder.decode(frame.data, frame.fin)
        else:
            data = frame.data

        if self.chunks_queue is None:
            self.chunks.append(data)
        else:
            self.chunks_queue.put_nowait(data)

        if not frame.fin:
            return

        # Message is complete. Wait until it's fetched to return.

        self.message_complete.set_result(None)

        if self.chunks_queue is not None:
            self.chunks_queue.put_nowait(None)

        # Yield control to allow get() to run and eventually set the future.
        # Locking with put_in_progress ensures only one coroutine can get here.
        self.put_in_progress = True
        try:
            assert not self.message_fetched.done()
            await self.message_fetched
        finally:
            self.put_in_progress = False

        # put() was unblocked by close() rather than get() or get_iter().
        if self.closed:
            raise EOFError("stream of frames ended")

        assert self.message_fetched.done()
        self.message_fetched = self.loop.create_future()

        self.decoder = None

    def close(self) -> None:
        """
        End the stream of frames.

        Callling :meth:`close` concurrently with :meth:`get`, :meth:`get_iter`,
        or :meth:`put` is safe. They will raise :exc:`EOFError`.

        """
        if self.closed:
            return

        self.closed = True

        # Unblock get or get_iter.
        if self.get_in_progress:
            self.message_complete.set_result(None)
            if self.chunks_queue is not None:
                self.chunks_queue.put_nowait(None)

        # Unblock put().
        if self.put_in_progress:
            self.message_fetched.set_result(None)
