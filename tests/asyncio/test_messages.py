import asyncio
import unittest

from websockets.asyncio.messages import *
from websockets.frames import OP_BINARY, OP_CONT, OP_PING, OP_PONG, OP_TEXT, Frame


class AssemblerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.assembler = Assembler()

    def tearDown(self):
        """
        Check that the assembler goes back to its default state after each test.

        This removes the need for testing various sequences.

        """
        self.assertFalse(self.assembler.get_in_progress)
        self.assertFalse(self.assembler.put_in_progress)
        if not self.assembler.closed:
            self.assertFalse(self.assembler.message_complete.done())
            self.assertFalse(self.assembler.message_fetched.done())
            self.assertIsNone(self.assembler.decoder)
            self.assertEqual(self.assembler.chunks, [])
            self.assertIsNone(self.assembler.chunks_queue)

    # Test get

    async def test_get_text_message_already_received(self):
        """get returns a text message that is already received."""

        async def putter():
            await self.assembler.put(Frame(OP_TEXT, b"caf\xc3\xa9"))

        asyncio.create_task(putter())
        message = await self.assembler.get()

        self.assertEqual(message, "café")

    async def test_get_binary_message_already_received(self):
        """get returns a binary message that is already received."""

        async def putter():
            await self.assembler.put(Frame(OP_BINARY, b"tea"))

        asyncio.create_task(putter())
        message = await self.assembler.get()

        self.assertEqual(message, b"tea")

    async def test_get_text_message_not_received_yet(self):
        """get returns a text message when it is received."""
        getter_task = asyncio.create_task(self.assembler.get())
        await self.assembler.put(Frame(OP_TEXT, b"caf\xc3\xa9"))
        message = getter_task.result()

        self.assertEqual(message, "café")

    async def test_get_binary_message_not_received_yet(self):
        """get returns a binary message when it is received."""
        getter_task = asyncio.create_task(self.assembler.get())
        await self.assembler.put(Frame(OP_BINARY, b"tea"))
        message = getter_task.result()

        self.assertEqual(message, b"tea")

    async def test_get_fragmented_text_message_already_received(self):
        """get reassembles a fragmented a text message that is already received."""

        async def putter():
            await self.assembler.put(Frame(OP_TEXT, b"ca", fin=False))
            await self.assembler.put(Frame(OP_CONT, b"f\xc3", fin=False))
            await self.assembler.put(Frame(OP_CONT, b"\xa9"))

        asyncio.create_task(putter())
        message = await self.assembler.get()

        self.assertEqual(message, "café")

    async def test_get_fragmented_binary_message_already_received(self):
        """get reassembles a fragmented binary message that is already received."""

        async def putter():
            await self.assembler.put(Frame(OP_BINARY, b"t", fin=False))
            await self.assembler.put(Frame(OP_CONT, b"e", fin=False))
            await self.assembler.put(Frame(OP_CONT, b"a"))

        asyncio.create_task(putter())
        message = await self.assembler.get()

        self.assertEqual(message, b"tea")

    async def test_get_fragmented_text_message_being_received(self):
        """get reassembles a fragmented text message that is partially received."""
        await self.assembler.put(Frame(OP_TEXT, b"ca", fin=False))
        getter_task = asyncio.create_task(self.assembler.get())
        await self.assembler.put(Frame(OP_CONT, b"f\xc3", fin=False))
        await self.assembler.put(Frame(OP_CONT, b"\xa9"))
        message = getter_task.result()

        self.assertEqual(message, "café")

    async def test_get_fragmented_binary_message_being_received(self):
        """get reassembles a fragmented binary message that is partially received."""
        await self.assembler.put(Frame(OP_BINARY, b"t", fin=False))
        getter_task = asyncio.create_task(self.assembler.get())
        await self.assembler.put(Frame(OP_CONT, b"e", fin=False))
        await self.assembler.put(Frame(OP_CONT, b"a"))
        message = getter_task.result()

        self.assertEqual(message, b"tea")

    async def test_get_fragmented_text_message_not_received_yet(self):
        """get reassembles a fragmented text message when it is received."""
        getter_task = asyncio.create_task(self.assembler.get())
        await self.assembler.put(Frame(OP_TEXT, b"ca", fin=False))
        await self.assembler.put(Frame(OP_CONT, b"f\xc3", fin=False))
        await self.assembler.put(Frame(OP_CONT, b"\xa9"))
        message = getter_task.result()

        self.assertEqual(message, "café")

    async def test_get_fragmented_binary_message_not_received_yet(self):
        """get reassembles a fragmented binary message when it is received."""
        getter_task = asyncio.create_task(self.assembler.get())
        await self.assembler.put(Frame(OP_BINARY, b"t", fin=False))
        await self.assembler.put(Frame(OP_CONT, b"e", fin=False))
        await self.assembler.put(Frame(OP_CONT, b"a"))
        message = getter_task.result()

        self.assertEqual(message, b"tea")

    # Test get_iter

    async def run_get_iter(self):
        self.fragments = []
        async for fragment in self.assembler.get_iter():
            self.fragments.append(fragment)

    async def test_get_iter_text_message_already_received(self):
        """get_iter yields a text message that is already received."""

        async def putter():
            await self.assembler.put(Frame(OP_TEXT, b"caf\xc3\xa9"))

        asyncio.create_task(putter())
        await self.run_get_iter()

        self.assertEqual(self.fragments, ["café"])

    async def test_get_iter_binary_message_already_received(self):
        """get_iter yields a binary message that is already received."""

        async def putter():
            await self.assembler.put(Frame(OP_BINARY, b"tea"))

        asyncio.create_task(putter())
        await self.run_get_iter()

        self.assertEqual(self.fragments, [b"tea"])

    async def test_get_iter_text_message_not_received_yet(self):
        """get_iter yields a text message when it is received."""
        asyncio.create_task(self.run_get_iter())
        await self.assembler.put(Frame(OP_TEXT, b"caf\xc3\xa9"))
        await asyncio.sleep(0)

        self.assertEqual(self.fragments, ["café"])

    async def test_get_iter_binary_message_not_received_yet(self):
        """get_iter yields a binary message when it is received."""
        asyncio.create_task(self.run_get_iter())
        await self.assembler.put(Frame(OP_BINARY, b"tea"))
        await asyncio.sleep(0)

        self.assertEqual(self.fragments, [b"tea"])

    async def test_get_iter_fragmented_text_message_already_received(self):
        """get_iter yields a fragmented text message that is already received."""

        async def putter():
            await self.assembler.put(Frame(OP_TEXT, b"ca", fin=False))
            await self.assembler.put(Frame(OP_CONT, b"f\xc3", fin=False))
            await self.assembler.put(Frame(OP_CONT, b"\xa9"))

        asyncio.create_task(putter())
        await self.run_get_iter()

        self.assertEqual(self.fragments, ["ca", "f", "é"])

    async def test_get_iter_fragmented_binary_message_already_received(self):
        """get_iter yields a fragmented binary message that is already received."""

        async def putter():
            await self.assembler.put(Frame(OP_BINARY, b"t", fin=False))
            await self.assembler.put(Frame(OP_CONT, b"e", fin=False))
            await self.assembler.put(Frame(OP_CONT, b"a"))

        asyncio.create_task(putter())
        await self.run_get_iter()

        self.assertEqual(self.fragments, [b"t", b"e", b"a"])

    async def test_get_iter_fragmented_text_message_being_received(self):
        """get_iter yields a fragmented text message that is partially received."""
        await self.assembler.put(Frame(OP_TEXT, b"ca", fin=False))
        asyncio.create_task(self.run_get_iter())
        await asyncio.sleep(0)
        self.assertEqual(self.fragments, ["ca"])
        await self.assembler.put(Frame(OP_CONT, b"f\xc3", fin=False))
        await asyncio.sleep(0)
        self.assertEqual(self.fragments, ["ca", "f"])
        await self.assembler.put(Frame(OP_CONT, b"\xa9"))

        self.assertEqual(self.fragments, ["ca", "f", "é"])

    async def test_get_iter_fragmented_binary_message_being_received(self):
        """get_iter yields a fragmented binary message that is partially received."""
        await self.assembler.put(Frame(OP_BINARY, b"t", fin=False))
        asyncio.create_task(self.run_get_iter())
        await asyncio.sleep(0)
        self.assertEqual(self.fragments, [b"t"])
        await self.assembler.put(Frame(OP_CONT, b"e", fin=False))
        await asyncio.sleep(0)
        self.assertEqual(self.fragments, [b"t", b"e"])
        await self.assembler.put(Frame(OP_CONT, b"a"))

        self.assertEqual(self.fragments, [b"t", b"e", b"a"])

    async def test_get_iter_fragmented_text_message_not_received_yet(self):
        """get_iter yields a fragmented text message when it is received."""
        asyncio.create_task(self.run_get_iter())
        await asyncio.sleep(0)
        await self.assembler.put(Frame(OP_TEXT, b"ca", fin=False))
        await asyncio.sleep(0)
        self.assertEqual(self.fragments, ["ca"])
        await self.assembler.put(Frame(OP_CONT, b"f\xc3", fin=False))
        await asyncio.sleep(0)
        self.assertEqual(self.fragments, ["ca", "f"])
        await self.assembler.put(Frame(OP_CONT, b"\xa9"))

        self.assertEqual(self.fragments, ["ca", "f", "é"])

    async def test_get_iter_fragmented_binary_message_not_received_yet(self):
        """get_iter yields a fragmented binary message when it is received."""
        asyncio.create_task(self.run_get_iter())
        await asyncio.sleep(0)
        await self.assembler.put(Frame(OP_BINARY, b"t", fin=False))
        await asyncio.sleep(0)
        self.assertEqual(self.fragments, [b"t"])
        await self.assembler.put(Frame(OP_CONT, b"e", fin=False))
        await asyncio.sleep(0)
        self.assertEqual(self.fragments, [b"t", b"e"])
        await self.assembler.put(Frame(OP_CONT, b"a"))

        self.assertEqual(self.fragments, [b"t", b"e", b"a"])

    # Test control frames

    async def test_control_frame_before_message_is_ignored(self):
        """get ignores control frames between messages."""

        async def putter():
            await self.assembler.put(Frame(OP_PING, b""))
            await self.assembler.put(Frame(OP_TEXT, b"caf\xc3\xa9"))

        asyncio.create_task(putter())
        message = await self.assembler.get()

        self.assertEqual(message, "café")

    async def test_control_frame_in_fragmented_message_is_ignored(self):
        """get ignores control frames within fragmented messages."""

        async def putter():
            await self.assembler.put(Frame(OP_BINARY, b"t", fin=False))
            await self.assembler.put(Frame(OP_PING, b""))
            await self.assembler.put(Frame(OP_CONT, b"e", fin=False))
            await self.assembler.put(Frame(OP_PONG, b""))
            await self.assembler.put(Frame(OP_CONT, b"a"))

        asyncio.create_task(putter())
        message = await self.assembler.get()

        self.assertEqual(message, b"tea")

    # Test concurrency

    async def test_get_fails_when_get_is_running(self):
        """get cannot be called concurrently with itself."""
        asyncio.create_task(self.assembler.get())
        await asyncio.sleep(0)
        with self.assertRaises(RuntimeError):
            await self.assembler.get()
        await self.assembler.put(Frame(OP_TEXT, b""))  # unlock other coroutine

    async def test_get_fails_when_get_iter_is_running(self):
        """get cannot be called concurrently with get_iter."""
        asyncio.create_task(self.run_get_iter())
        await asyncio.sleep(0)
        with self.assertRaises(RuntimeError):
            await self.assembler.get()
        await self.assembler.put(Frame(OP_TEXT, b""))  # unlock other coroutine

    async def test_get_iter_fails_when_get_is_running(self):
        """get_iter cannot be called concurrently with get."""
        asyncio.create_task(self.assembler.get())
        await asyncio.sleep(0)
        with self.assertRaises(RuntimeError):
            await self.run_get_iter()
        await self.assembler.put(Frame(OP_TEXT, b""))  # unlock other coroutine

    async def test_get_iter_fails_when_get_iter_is_running(self):
        """get_iter cannot be called concurrently with itself."""
        asyncio.create_task(self.run_get_iter())
        await asyncio.sleep(0)
        with self.assertRaises(RuntimeError):
            await self.run_get_iter()
        await self.assembler.put(Frame(OP_TEXT, b""))  # unlock other coroutine

    async def test_put_fails_when_put_is_running(self):
        """put cannot be called concurrently with itself."""
        asyncio.create_task(self.assembler.put(Frame(OP_TEXT, b"caf\xc3\xa9")))
        await asyncio.sleep(0)
        with self.assertRaises(RuntimeError):
            await self.assembler.put(Frame(OP_BINARY, b"tea"))
        await self.assembler.get()  # unblock other coroutine

    # Test termination

    async def test_get_fails_when_interrupted_by_close(self):
        """get raises EOFError when close is called."""
        asyncio.get_event_loop().call_soon(self.assembler.close)
        with self.assertRaises(EOFError):
            await self.assembler.get()

    async def test_get_iter_fails_when_interrupted_by_close(self):
        """get_iter raises EOFError when close is called."""
        asyncio.get_event_loop().call_soon(self.assembler.close)
        with self.assertRaises(EOFError):
            async for _ in self.assembler.get_iter():
                self.fail("no fragment expected")

    async def test_put_fails_when_interrupted_by_close(self):
        """put raises EOFError when close is called."""
        asyncio.get_event_loop().call_soon(self.assembler.close)
        with self.assertRaises(EOFError):
            await self.assembler.put(Frame(OP_TEXT, b"caf\xc3\xa9"))

    async def test_get_fails_after_close(self):
        """get raises EOFError after close is called."""
        self.assembler.close()
        with self.assertRaises(EOFError):
            await self.assembler.get()

    async def test_get_iter_fails_after_close(self):
        """get_iter raises EOFError after close is called."""
        self.assembler.close()
        with self.assertRaises(EOFError):
            async for _ in self.assembler.get_iter():
                self.fail("no fragment expected")

    async def test_put_fails_after_close(self):
        """put raises EOFError after close is called."""
        self.assembler.close()
        with self.assertRaises(EOFError):
            await self.assembler.put(Frame(OP_TEXT, b"caf\xc3\xa9"))

    async def test_close_is_idempotent(self):
        """close can be called multiple times safely."""
        self.assembler.close()
        self.assembler.close()
