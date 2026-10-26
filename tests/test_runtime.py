import asyncio
import gc
import inspect
import os
import signal
import tempfile
import threading
import time
import unittest
from unittest import mock

from tinyjobs import runtime
from tinyjobs.runtime import FORK_PAUSE_SECONDS, LoopRunner


def loop_threads() -> list[threading.Thread]:
    return [
        thread
        for thread in threading.enumerate()
        if thread.name == runtime.LOOP_THREAD_PREFIX
    ]


def wait_running(loop: asyncio.AbstractEventLoop) -> None:
    deadline = time.monotonic() + 5
    while not loop.is_running() and time.monotonic() < deadline:
        time.sleep(0.005)


class ForkPauseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = LoopRunner()
        self.addCleanup(self.runner.close)
        self.loop = self.runner.own_loop()
        wait_running(self.loop)

    def wait_until_stopped(self) -> None:
        deadline = time.monotonic() + 5
        while self.loop.is_running() and time.monotonic() < deadline:
            time.sleep(0.01)

    def test_a_paused_loop_resumes_as_the_same_loop(self):
        threads = len(loop_threads())
        self.assertTrue(self.runner.pause())
        self.assertFalse(self.loop.is_running())
        self.assertEqual(len(loop_threads()), threads - 1)
        self.runner.resume()
        self.assertIs(self.runner.own_loop(), self.loop)
        wait_running(self.loop)
        self.assertTrue(self.loop.is_running())
        self.assertEqual(len(loop_threads()), threads)

    def test_a_call_while_paused_restarts_the_paused_loop_once(self):
        threads = len(loop_threads())
        self.assertTrue(self.runner.pause())
        self.assertEqual(self.runner.run(lambda: asyncio.sleep(0, "ran")), "ran")
        self.assertIs(self.runner.own_loop(), self.loop)
        self.runner.resume()
        self.assertEqual(len(loop_threads()), threads)
        self.assertEqual(self.runner.run(lambda: asyncio.sleep(0, "again")), "again")

    def test_the_loop_thread_does_not_pause_itself(self):
        async def pause_from_inside() -> bool:
            return self.runner.pause()

        self.assertFalse(self.runner.run(pause_from_inside))
        self.assertTrue(self.loop.is_running())

    def test_a_loop_that_does_not_stop_in_time_keeps_serving(self):
        async def block() -> None:
            time.sleep(FORK_PAUSE_SECONDS * 4)

        self.loop.call_soon_threadsafe(self.loop.create_task, block())
        time.sleep(FORK_PAUSE_SECONDS / 2)
        threads = len(loop_threads())
        started = time.monotonic()
        self.assertTrue(self.runner.pause())
        self.assertLess(time.monotonic() - started, FORK_PAUSE_SECONDS * 2.5)
        self.runner.resume()
        self.assertEqual(len(loop_threads()), threads)
        time.sleep(FORK_PAUSE_SECONDS * 6)
        self.assertTrue(self.loop.is_running())
        self.assertEqual(len(loop_threads()), threads)
        self.runner.resume()
        self.assertEqual(len(loop_threads()), threads)
        self.assertEqual(self.runner.run(lambda: asyncio.sleep(0, "ran")), "ran")

    def block_for(self, seconds: float) -> None:
        self.loop.call_soon_threadsafe(time.sleep, seconds)
        time.sleep(0.01)

    def delay_the_next_run(self, seconds: float) -> threading.Event:
        delayed = threading.Event()
        run_forever = self.loop.run_forever

        def late_run_forever() -> None:
            del self.loop.run_forever
            delayed.set()
            time.sleep(seconds)
            run_forever()

        self.loop.run_forever = late_run_forever
        return delayed

    def test_a_close_while_a_pause_keeps_the_loop_serving_stops_it(self):
        failures: list = []
        self.addCleanup(setattr, threading, "excepthook", threading.excepthook)
        threading.excepthook = failures.append
        self.block_for(FORK_PAUSE_SECONDS * 3)
        self.assertTrue(self.runner.pause())
        self.assertTrue(self.runner._keep_serving)
        self.runner.close()
        self.assertTrue(self.loop.is_closed())
        self.assertEqual(failures, [])
        self.assertFalse(self.loop.is_running())
        self.assertTrue(self.loop.is_closed())
        self.assertIsNot(self.runner.own_loop(), self.loop)

    def test_a_close_while_paused_finishes_the_paused_loop(self):
        self.assertTrue(self.runner.pause())
        self.runner.close()
        deadline = time.monotonic() + 5
        while not self.loop.is_closed() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(self.loop.is_closed())
        self.runner.resume()
        fresh = self.runner.own_loop()
        self.assertIsNot(fresh, self.loop)
        wait_running(fresh)
        self.assertTrue(fresh.is_running())

    def test_a_call_while_a_slow_pause_restarts_the_loop_keeps_that_loop(self):
        threads = len(loop_threads())
        self.block_for(FORK_PAUSE_SECONDS * 2)
        delayed = self.delay_the_next_run(FORK_PAUSE_SECONDS * 2)
        self.assertTrue(self.runner.pause())
        self.assertTrue(delayed.wait(5))
        self.assertIs(self.runner.own_loop(), self.loop)
        self.assertEqual(len(loop_threads()), threads)
        self.assertEqual(self.runner.run(lambda: asyncio.sleep(0, "ran")), "ran")
        self.assertTrue(self.runner.pause())
        self.assertFalse(self.loop.is_running())
        self.runner.resume()

    def test_a_call_while_a_paused_loop_starts_again_keeps_that_loop(self):
        threads = len(loop_threads())
        self.assertTrue(self.runner.pause())
        delayed = self.delay_the_next_run(FORK_PAUSE_SECONDS * 2)
        resumer = threading.Thread(target=self.runner.resume)
        resumer.start()
        self.assertTrue(delayed.wait(5))
        self.assertIs(self.runner._ensure_loop(), self.loop)
        resumer.join(5)
        self.assertEqual(len(loop_threads()), threads)

    def test_a_sync_call_in_flight_waits_out_a_pause(self):
        results: list = []
        caller = threading.Thread(
            target=lambda: results.append(
                self.runner.run(lambda: asyncio.sleep(FORK_PAUSE_SECONDS, "ran"))
            )
        )
        caller.start()
        time.sleep(FORK_PAUSE_SECONDS / 2)
        self.assertTrue(self.runner.pause())
        time.sleep(runtime.LOOP_POLL_SECONDS * 3)
        self.runner.resume()
        caller.join(5)
        self.assertEqual(results, ["ran"])

    def test_an_async_call_from_another_loop_waits_out_a_pause(self):
        async def call() -> str:
            pending = asyncio.ensure_future(
                runtime.run_on(
                    self.loop, asyncio.sleep(FORK_PAUSE_SECONDS, "ran"), "call"
                )
            )
            await asyncio.sleep(FORK_PAUSE_SECONDS / 2)
            self.assertTrue(await asyncio.to_thread(self.runner.pause))
            await asyncio.sleep(runtime.LOOP_POLL_SECONDS * 3)
            self.runner.resume()
            return await pending

        self.assertEqual(asyncio.run(call()), "ran")

    def test_an_async_call_made_during_a_pause_waits_for_the_resume(self):
        self.assertTrue(self.runner.pause())

        async def call() -> str:
            pending = asyncio.ensure_future(
                runtime.run_on(self.loop, asyncio.sleep(0, "ran"), "call")
            )
            await asyncio.sleep(runtime.LOOP_POLL_SECONDS * 3)
            self.runner.resume()
            return await pending

        self.assertEqual(asyncio.run(call()), "ran")

    def test_an_async_call_to_a_stopped_loop_is_refused_at_once(self):
        from tinyjobs.exceptions import EventLoopStoppedError

        stopped = asyncio.new_event_loop()
        self.addCleanup(stopped.close)

        async def call() -> float:
            started = time.monotonic()
            with self.assertRaises(EventLoopStoppedError):
                await runtime.run_on(stopped, asyncio.sleep(0), "call")
            return time.monotonic() - started

        self.assertLess(asyncio.run(call()), runtime.LOOP_POLL_SECONDS / 2)

    def test_a_sync_call_that_cannot_get_its_loop_makes_no_coroutine(self):
        made: list = []
        with mock.patch.object(
            self.runner, "_ensure_loop", side_effect=OSError("no loop")
        ):
            with self.assertRaises(OSError):
                self.runner.run(lambda: made.append(True))
        self.assertEqual(made, [])

    def test_a_second_pause_while_the_first_keeps_the_loop_serving_is_refused(self):
        self.block_for(FORK_PAUSE_SECONDS * 4)
        self.assertTrue(self.runner.pause())
        self.assertFalse(self.runner.pause())
        self.runner.resume()
        self.assertIs(self.runner.own_loop(), self.loop)
        self.assertEqual(self.runner.run(lambda: asyncio.sleep(0, "ran")), "ran")

    def test_a_loop_paused_and_then_closed_is_let_go(self):
        import gc
        import weakref

        self.assertTrue(self.runner.pause())
        self.runner.resume()
        let_go = weakref.ref(self.loop)
        self.runner.close()
        del self.loop
        gc.collect()
        self.assertIsNone(let_go())

    def test_a_child_refuses_a_call_to_a_loop_paused_in_its_parent(self):
        import os
        import signal
        import warnings

        from tinyjobs.exceptions import EventLoopStoppedError

        async def call() -> None:
            await runtime.run_on(self.loop, asyncio.sleep(0), "call")

        self.assertTrue(self.runner.pause())
        self.addCleanup(self.runner.resume)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            child = os.fork()
        if child == 0:
            signal.alarm(5)
            try:
                asyncio.run(call())
            except EventLoopStoppedError:
                os._exit(0)
            os._exit(1)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)

    def test_the_pool_put_in_at_a_pause_serves_the_loop(self):
        self.assertTrue(self.runner.pause())
        self.runner.resume()
        self.assertEqual(
            self.runner.run(lambda: asyncio.to_thread(lambda: "ran")), "ran"
        )

    def test_a_loop_thread_that_died_before_it_was_ready_is_replaced(self):
        runner = LoopRunner()
        self.addCleanup(runner.close)
        runner._serve = lambda loop, ready: None
        dead = runner.own_loop()
        runner._thread.join(5)
        del runner._serve
        fresh = runner.own_loop()
        self.assertIsNot(fresh, dead)
        wait_running(fresh)
        self.assertTrue(fresh.is_running())
        dead.close()

    def test_a_loop_with_work_in_its_worker_threads_is_not_paused(self):
        release = threading.Event()
        started = threading.Event()

        async def wait_in_a_thread() -> None:
            await asyncio.to_thread(lambda: (started.set(), release.wait(5)))

        future = asyncio.run_coroutine_threadsafe(wait_in_a_thread(), self.loop)
        started.wait(5)
        self.assertFalse(self.runner.pause())
        self.assertTrue(self.loop.is_running())
        release.set()
        future.result(5)

    def test_a_pool_that_gets_work_while_stopping_is_kept(self):
        pool = self.runner._pool
        with mock.patch.object(
            runtime.CountingPool,
            "idle",
            new_callable=mock.PropertyMock,
            side_effect=[True, False],
        ):
            self.assertTrue(self.runner.pause())
        self.assertIs(self.runner._pool, pool)
        self.assertFalse(pool._shutdown)
        self.runner.resume()

    def test_a_call_during_a_pause_in_progress_keeps_the_loop_serving(self):
        with self.runner._guard:
            self.runner._paused, self.runner._keep_serving = self.loop, False
        self.assertIs(self.runner.own_loop(), self.loop)
        self.loop.call_soon_threadsafe(self.loop.stop)
        time.sleep(0.3)
        self.assertTrue(self.loop.is_running())
        self.assertEqual(self.runner.run(lambda: asyncio.sleep(0, "ran")), "ran")

    def test_concurrent_first_calls_share_one_loop(self):
        for _ in range(3):
            runner = LoopRunner()
            self.addCleanup(runner.close)

            def slow_serve(loop, ready, serve=runner._serve) -> None:
                time.sleep(0.2)
                serve(loop, ready)

            runner._serve = slow_serve
            start = threading.Barrier(8)
            loops: list = []

            def first_call() -> None:
                start.wait(5)
                loops.append(runner.own_loop())

            callers = [threading.Thread(target=first_call) for _ in range(8)]
            for caller in callers:
                caller.start()
            for caller in callers:
                caller.join(10)
            self.assertEqual(len({id(loop) for loop in loops}), 1)

    def test_a_pause_that_finds_its_guard_held_by_another_thread_gives_up(self):
        held, release = threading.Event(), threading.Event()

        def hold() -> None:
            with self.runner._guard:
                held.set()
                release.wait(5)

        holder = threading.Thread(target=hold)
        holder.start()
        held.wait(5)
        try:
            started = time.monotonic()
            self.assertFalse(self.runner.pause())
            self.assertLess(time.monotonic() - started, FORK_PAUSE_SECONDS)
        finally:
            release.set()
            holder.join(5)
        self.assertTrue(self.loop.is_running())

    def test_a_child_keeps_the_loops_it_inherited_alive(self):
        self.runner.forget_inherited_loop()
        self.addCleanup(runtime._inherited_loops.remove, self.loop)
        self.addCleanup(self.wait_until_stopped)
        self.addCleanup(self.loop.call_soon_threadsafe, self.loop.stop)
        self.assertIn(self.loop, runtime._inherited_loops)


class CountingPoolTests(unittest.TestCase):
    def test_a_refused_submit_leaves_the_pool_idle(self):
        pool = runtime.CountingPool(max_workers=1)
        pool.shutdown()
        with self.assertRaises(RuntimeError):
            pool.submit(lambda: None)
        self.assertTrue(pool.idle)


def fork_quietly() -> int:
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return os.fork()


def wait_resumed(case: unittest.TestCase) -> None:
    deadline = time.monotonic() + 5
    while runtime._pausing or runtime._paused_for_fork:
        case.assertLess(time.monotonic(), deadline, "the fork never resumed")
        time.sleep(0.005)


class ControlTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = LoopRunner()
        self.addCleanup(self.runner.close)
        self.loop = self.runner.own_loop()
        wait_running(self.loop)

    def call_in_background(self, factory) -> tuple[threading.Thread, list]:
        outcome: list = []

        def call() -> None:
            try:
                outcome.append(self.runner.run(factory))
            except BaseException as error:
                outcome.append(error)

        caller = threading.Thread(target=call)
        caller.start()
        return caller, outcome

    def pause_control(self, *, loops: bool = False) -> int:
        import contextlib

        ident = threading.get_ident()
        runtime._fork_marks[ident] = os.getpid()
        self.addCleanup(runtime._fork_marks.pop, ident, None)
        acked = threading.Lock()
        acked.acquire()
        resume = runtime._after_fork_actions[ident] = mock.Mock()
        self.addCleanup(runtime._after_fork_actions.pop, ident, None)
        kept = (
            contextlib.nullcontext()
            if loops
            else mock.patch.object(LoopRunner, "pause", return_value=False)
        )
        with kept:
            runtime._inbox.put((runtime._pause, ident, os.getpid(), acked, resume))
            runtime._ensure_control()
            self.assertTrue(acked.acquire(timeout=5))
            runtime._control_thread.join(5)
        self.assertTrue(runtime._pausing)
        return ident

    def test_a_call_made_during_a_fork_pause_is_served_once_it_resumes(self):
        ident = self.pause_control()
        caller, outcome = self.call_in_background(lambda: asyncio.sleep(0, "ran"))
        caller.join(0.3)
        self.assertEqual(outcome, [])
        runtime._fork_marks.pop(ident, None)
        runtime._RESUME()
        caller.join(5)
        self.assertEqual(outcome, ["ran"])
        wait_resumed(self)

    def test_a_call_waiting_on_a_resume_that_never_came_resumes_by_itself(self):
        ident = self.pause_control()
        caller, outcome = self.call_in_background(lambda: asyncio.sleep(0, "ran"))
        caller.join(0.3)
        self.assertEqual(outcome, [])
        runtime._fork_marks.pop(ident, None)
        caller.join(5)
        self.assertEqual(outcome, ["ran"])
        wait_resumed(self)

    def test_an_async_call_waiting_on_a_resume_that_never_came_resumes_it(self):
        ident = self.pause_control(loops=True)
        self.assertFalse(self.loop.is_running())
        runtime._fork_marks.pop(ident, None)

        async def call():
            return await runtime.run_on(self.loop, asyncio.sleep(0, "ran"), "test")

        self.assertEqual(asyncio.run(asyncio.wait_for(call(), 5)), "ran")
        wait_resumed(self)

    def test_a_pause_that_is_no_longer_wanted_is_acknowledged_and_ignored(self):
        for ident, pid in ((threading.get_ident(), os.getpid()), (1, os.getpid() + 1)):
            acked = threading.Lock()
            acked.acquire()
            self.assertFalse(runtime._pause(ident, pid, acked, mock.Mock()))
            self.assertFalse(acked.locked())
            self.assertFalse(runtime._pausing)

    def test_a_pause_inherited_from_another_threads_fork_is_ignored_in_the_child(self):
        ident = 1
        resume = runtime._after_fork_actions[ident] = mock.Mock()
        self.addCleanup(runtime._after_fork_actions.pop, ident, None)
        acked = threading.Lock()
        acked.acquire()
        with mock.patch.object(LoopRunner, "pause") as pause:
            self.assertFalse(runtime._pause(ident, os.getpid() + 1, acked, resume))
        pause.assert_not_called()
        self.assertFalse(acked.locked())
        self.assertFalse(runtime._pausing)

    def test_a_running_control_thread_is_not_started_again(self):
        self.assertEqual(self.runner.run(lambda: asyncio.sleep(0, "ran")), "ran")
        with mock.patch.object(runtime._thread, "start_new_thread") as start:
            runtime._ensure_control()
        start.assert_not_called()

    def test_a_child_forked_while_the_control_lock_was_held_still_serves(self):
        with mock.patch.object(runtime, "_pause_wanted", return_value=False):
            with runtime._control_lock:
                child = fork_quietly()
                if child == 0:
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)
                    signal.alarm(10)
                    try:
                        served = (
                            self.runner.run(lambda: asyncio.sleep(0, "ran")) == "ran"
                        )
                    except BaseException:
                        served = False
                    os._exit(0 if served else 1)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)

    def test_a_pause_left_from_an_earlier_fork_is_ignored_during_the_next(self):
        ident, pid = threading.get_ident(), os.getpid()
        runtime._fork_marks[ident] = pid
        self.addCleanup(runtime._fork_marks.pop, ident, None)
        runtime._after_fork_actions[ident] = mock.Mock()
        self.addCleanup(runtime._after_fork_actions.pop, ident, None)
        acked = threading.Lock()
        acked.acquire()
        with mock.patch.object(LoopRunner, "pause") as pause:
            self.assertFalse(runtime._pause(ident, pid, acked, mock.Mock()))
        pause.assert_not_called()
        self.assertFalse(acked.locked())
        self.assertFalse(runtime._pausing)
        self.assertEqual(runtime._paused_for_fork, [])

    def test_a_pause_that_outlasts_its_fork_resumes_what_it_paused(self):
        ident, pid = threading.get_ident(), os.getpid()
        resume = runtime._after_fork_actions[ident] = mock.Mock()
        self.addCleanup(runtime._after_fork_actions.pop, ident, None)
        runner = mock.Mock()

        def paused() -> bool:
            runtime._after_fork_actions.pop(ident, None)
            return True

        runner.pause.side_effect = paused
        acked = threading.Lock()
        acked.acquire()
        with mock.patch.object(runtime, "_runners", weakref_set(runner)):
            self.assertFalse(runtime._pause(ident, pid, acked, resume))
        runner.resume.assert_called_once_with()
        self.assertFalse(runtime._pausing)
        self.assertEqual(runtime._paused_for_fork, [])

    def test_a_request_from_another_process_is_dropped(self):
        made: list = []
        request = runtime._Request(self.runner, lambda: made.append(True), "call")
        request.pid += 1
        self.assertFalse(runtime._start(request))
        self.assertFalse(runtime._drive_off(request))
        request.loop = mock.Mock()
        self.assertFalse(runtime._cancel(request))
        request.loop.call_soon_threadsafe.assert_not_called()
        self.assertEqual(made, [])
        self.assertEqual(self.runner._awaiting.get(self.loop, set()), set())

    def test_a_wait_in_a_forked_child_gives_up_at_once(self):
        request = runtime._Request(self.runner, lambda: None, "call")
        request.pid += 1
        started = time.monotonic()
        with self.assertRaises(runtime.EventLoopStoppedError):
            runtime._wait(request)
        self.assertLess(time.monotonic() - started, runtime.LOOP_POLL_SECONDS * 3)

    def test_a_loop_that_ends_settles_the_calls_it_never_began(self):
        with mock.patch.object(runtime, "_begin", lambda request: None):
            caller, outcome = self.call_in_background(lambda: asyncio.sleep(0))
            deadline = time.monotonic() + 5
            while (
                not self.runner._awaiting.get(self.loop) and time.monotonic() < deadline
            ):
                time.sleep(0.005)
            self.runner.close()
            caller.join(5)
        self.assertEqual(len(outcome), 1)
        self.assertIsInstance(outcome[0], runtime.AppClosedError)
        self.assertEqual(self.runner._awaiting, {})

    def test_a_call_cancelled_before_it_began_never_makes_its_coroutine(self):
        made: list = []
        self.loop.call_soon_threadsafe(time.sleep, 0.3)
        time.sleep(0.05)
        request = runtime._Request(self.runner, lambda: made.append(True), "call")
        runtime._inbox.put(request.run_message)
        runtime._inbox.put(request.cancel_message)
        runtime._ensure_control()
        with self.assertRaises(runtime.EventLoopStoppedError):
            runtime._wait(request)
        self.assertEqual(made, [])

    def test_a_call_that_brings_its_loop_down_gets_why_and_the_next_one_works(self):
        async def leave() -> None:
            raise SystemExit(3)

        failures: list = []
        self.addCleanup(setattr, threading, "excepthook", threading.excepthook)
        threading.excepthook = failures.append
        with self.assertRaises(SystemExit):
            self.runner.run(leave)
        self.runner._thread.join(5)
        self.assertEqual(len(failures), 1)
        self.assertEqual(self.runner._awaiting, {})
        self.assertEqual(self.runner.run(lambda: asyncio.sleep(0, "ran")), "ran")
        self.loop.close()

    def test_the_control_thread_outlives_a_message_that_fails(self):
        def fail() -> bool:
            raise RuntimeError("failed")

        runtime._inbox.put((fail,))
        thread = runtime._control_thread
        self.assertEqual(self.runner.run(lambda: asyncio.sleep(0, "ran")), "ran")
        self.assertIs(runtime._control_thread, thread)

    def test_a_close_that_cannot_start_its_thread_reports_why(self):
        with mock.patch.object(
            threading.Thread, "start", side_effect=RuntimeError("no thread")
        ):
            request = runtime._Request(None, lambda: None, "close")
            runtime._drive_off(request)
        with self.assertRaises(RuntimeError):
            runtime._wait(request)
        self.assertEqual(runtime._closing, set())

    def test_a_close_interrupted_while_waiting_still_finishes(self):
        finished = threading.Event()

        def slow_close() -> None:
            time.sleep(0.2)
            finished.set()

        def interrupt(request) -> None:
            raise KeyboardInterrupt

        with mock.patch.object(runtime, "_wait", interrupt):
            with self.assertRaises(KeyboardInterrupt):
                runtime.drive(slow_close, name="close")
        self.assertTrue(finished.wait(5))


@unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
class ForkHookTests(unittest.TestCase):
    def test_the_after_hooks_undo_what_the_before_hook_marked(self):
        gate = mock.Mock()
        ident = threading.get_ident()
        with (
            mock.patch.object(runtime, "_fork_gates", [gate]),
            mock.patch.object(runtime, "_pause_wanted", return_value=False),
            runtime.tracked_lock(),
        ):
            runtime._before_fork()
            self.assertEqual(runtime._fork_marks, {ident: os.getpid()})
            self.assertEqual(len(runtime._kept_at_fork[ident]), 1)
            self.assertTrue(runtime.holds_tinyjobs())
            runtime._RUN_ACTION()
            runtime._POP_MARK()
            runtime._POP_KEPT()
        gate.wait_quiet.assert_called_once_with(ident, os.getpid())
        self.assertEqual(runtime._fork_marks, {})
        self.assertEqual(runtime._kept_at_fork, {})
        self.assertFalse(runtime.holds_tinyjobs())

    def test_a_fork_keeps_waiting_on_its_gates_through_a_raising_handler(self):
        gate = mock.Mock()
        gate.wait_quiet.side_effect = [KeyboardInterrupt, None]
        with (
            mock.patch.object(runtime, "_fork_gates", [gate]),
            mock.patch.object(runtime, "_pause_wanted", return_value=False),
        ):
            try:
                with self.assertRaises(KeyboardInterrupt):
                    runtime._before_fork()
            finally:
                runtime._RUN_ACTION()
                runtime._POP_MARK()
                runtime._POP_KEPT()
        self.assertEqual(gate.wait_quiet.call_count, 2)

    def test_a_fork_gives_up_on_a_wait_that_keeps_failing(self):
        failing = mock.Mock(side_effect=KeyboardInterrupt)
        failures: list = []
        runtime._through(failures, failing)
        self.assertEqual(failing.call_count, runtime.FORK_WAIT_TRIES)
        self.assertEqual(len(failures), 1)

    def test_a_handler_raising_during_a_fork_does_not_cut_the_gate_short(self):
        import subprocess
        import sys

        script = (
            "import os, signal, sys, threading, time, warnings\n"
            "warnings.simplefilter('ignore')\n"
            "from tinyjobs.backends.sqlite.connection import ConnectionPool\n"
            "pool = ConnectionPool(sys.argv[1])\n"
            "inside = threading.Event()\n"
            "def pause():\n"
            "    inside.set()\n"
            "    time.sleep(1.0)\n"
            "def hold():\n"
            "    with pool.write() as connection:\n"
            "        connection.create_function('pause', 0, pause)\n"
            "        connection.execute('SELECT pause()').fetchone()\n"
            "holder = threading.Thread(target=hold)\n"
            "holder.start()\n"
            "inside.wait(5)\n"
            "def stop(signum, frame):\n"
            "    raise SystemExit(3)\n"
            "signal.signal(signal.SIGALRM, stop)\n"
            "signal.setitimer(signal.ITIMER_REAL, 0.2)\n"
            "started = time.monotonic()\n"
            "child = os.fork()\n"
            "if child == 0:\n"
            "    os._exit(0)\n"
            "took = time.monotonic() - started\n"
            "os.waitpid(child, 0)\n"
            "holder.join()\n"
            "pool.close()\n"
            "assert took >= 0.7, took\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            finished = subprocess.run(
                [sys.executable, "-c", script, os.path.join(directory, "jobs.db")],
                capture_output=True,
                text=True,
                timeout=60,
            )
        self.assertEqual(finished.returncode, 0, finished.stderr)

    def test_the_child_hooks_drop_the_resume_and_count_the_fork(self):
        action = mock.Mock()
        runtime._after_fork_actions[threading.get_ident()] = action
        generation = runtime.fork_state.generation
        self.addCleanup(setattr, runtime.fork_state, "generation", generation)
        runtime._DROP_ACTION()
        runtime._NEXT_GENERATION()
        action.assert_not_called()
        self.assertEqual(runtime._after_fork_actions, {})
        self.assertNotEqual(runtime.fork_state.generation, generation)

    def test_the_parent_hook_runs_the_resume_of_its_own_thread_only(self):
        mine, theirs = mock.Mock(), mock.Mock()
        runtime._after_fork_actions[threading.get_ident()] = mine
        runtime._after_fork_actions[1] = theirs
        self.addCleanup(runtime._after_fork_actions.clear)
        runtime._RUN_ACTION()
        mine.assert_called_once_with()
        theirs.assert_not_called()

    def test_a_fork_from_the_main_thread_pauses_once_and_resumes(self):
        runner = LoopRunner()
        self.addCleanup(runner.close)
        loop = runner.own_loop()
        wait_running(loop)
        pause, pauses = runtime._pause, []

        def counted_pause(*args) -> bool:
            pauses.append(args)
            return pause(*args)

        with (
            mock.patch.object(runtime, "_pause", counted_pause),
            mock.patch.object(runtime, "_pause_wanted", return_value=True),
        ):
            child = fork_quietly()
            if child == 0:
                os._exit(0)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.assertEqual(len(pauses), 1)
        wait_resumed(self)
        self.assertEqual(runner.run(lambda: asyncio.sleep(0, "ran")), "ran")
        self.assertIs(runner.own_loop(), loop)
        self.assertEqual(runtime._fork_marks, {})

    def test_a_child_starts_without_fork_state_or_a_control_thread(self):
        runner = LoopRunner()
        self.addCleanup(runner.close)
        wait_running(runner.own_loop())
        with mock.patch.object(runtime, "_pause_wanted", return_value=True):
            child = fork_quietly()
            if child == 0:
                time.sleep(0.2)
                clean = (
                    runtime._fork_marks == {}
                    and runtime._after_fork_actions == {}
                    and runtime._settled_pid != os.getpid()
                    and not any(
                        thread.name == runtime.CONTROL_THREAD_NAME
                        for thread in threading.enumerate()
                    )
                )
                os._exit(0 if clean else 1)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        wait_resumed(self)

    def test_a_fork_from_another_thread_does_not_pause(self):
        runner = LoopRunner()
        self.addCleanup(runner.close)
        wait_running(runner.own_loop())
        children: list[int] = []

        def fork() -> None:
            child = fork_quietly()
            if child == 0:
                os._exit(0)
            children.append(child)

        with mock.patch.object(runtime, "_pause") as pause:
            forker = threading.Thread(target=fork)
            forker.start()
            forker.join(10)
        _, status = os.waitpid(children[0], 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        pause.assert_not_called()

    def test_a_raising_handler_in_the_fork_wait_leaves_the_fork_whole(self):
        import sys

        class Stop(Exception):
            pass

        def stop(signum, frame) -> None:
            raise Stop

        runner = LoopRunner()
        self.addCleanup(runner.close)
        wait_running(runner.own_loop())
        previous = signal.signal(signal.SIGUSR1, stop)
        self.addCleanup(signal.signal, signal.SIGUSR1, previous)
        wait, reported = runtime._wait_for_pause, []

        def interrupted_wait(acked) -> None:
            signal.raise_signal(signal.SIGUSR1)
            wait(acked)

        with (
            mock.patch.object(runtime, "_wait_for_pause", interrupted_wait),
            mock.patch.object(runtime, "_pause_wanted", return_value=True),
            mock.patch.object(sys, "unraisablehook", reported.append),
        ):
            child = fork_quietly()
            if child == 0:
                os._exit(0)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)
        self.assertEqual([type(report.exc_value) for report in reported], [Stop])
        wait_resumed(self)
        self.assertEqual(runtime._fork_marks, {})
        self.assertEqual(runtime._after_fork_actions, {})
        self.assertFalse(runtime.holds_tinyjobs())
        self.assertEqual(runner.run(lambda: asyncio.sleep(0, "ran")), "ran")

    def test_a_child_lets_go_of_what_its_parent_had_paused(self):
        runner = mock.Mock()
        with mock.patch.object(runtime, "_paused_for_fork", [runner]):
            child = fork_quietly()
            if child == 0:
                runtime._settle_here(os.getpid())
                settled = runtime._paused_for_fork == [] and not runtime._pausing
                os._exit(0 if settled else 1)
            _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)

    def test_a_fork_that_fails_still_resumes_everything(self):
        import subprocess
        import sys

        script = (
            "import os, resource, sys, time, warnings\n"
            "warnings.simplefilter('ignore')\n"
            "from tinyjobs import TinyJobs, runtime\n"
            "app = TinyJobs(sys.argv[1]).start()\n"
            "app.counts()\n"
            "soft, hard = resource.getrlimit(resource.RLIMIT_NPROC)\n"
            "resource.setrlimit(resource.RLIMIT_NPROC, (1, hard))\n"
            "try:\n"
            "    child = os.fork()\n"
            "except OSError:\n"
            "    child = None\n"
            "if child == 0:\n"
            "    os._exit(0)\n"
            "_, hard = resource.getrlimit(resource.RLIMIT_NPROC)\n"
            "resource.setrlimit(resource.RLIMIT_NPROC, (min(soft, hard), hard))\n"
            "assert child is None, 'the fork did not fail'\n"
            "assert app.counts() == {}, app.counts()\n"
            "deadline = time.monotonic() + 5\n"
            "while runtime._pausing or not app._runner._loop.is_running():\n"
            "    assert time.monotonic() < deadline, 'not resumed'\n"
            "    time.sleep(0.01)\n"
            "assert runtime._fork_marks == {} and runtime._after_fork_actions == {}\n"
            "app.close()\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            finished = subprocess.run(
                [sys.executable, "-c", script, f"sqlite:///{directory}/jobs.db"],
                capture_output=True,
                text=True,
                timeout=60,
            )
        self.assertEqual(finished.returncode, 0, finished.stderr)


class TrackedLockTests(unittest.TestCase):
    def test_a_failed_acquire_does_not_count_as_held(self):
        lock = runtime.tracked_lock()
        held, done = threading.Event(), threading.Event()

        def hold() -> None:
            with lock:
                held.set()
                done.wait(5)

        holder = threading.Thread(target=hold)
        holder.start()
        held.wait(5)
        self.assertFalse(lock.acquire(blocking=False))
        self.assertFalse(runtime.holds_tinyjobs())
        done.set()
        holder.join(5)

    def test_a_condition_wait_keeps_the_hold(self):
        condition = threading.Condition(runtime.tracked_lock())
        with condition:
            self.assertTrue(runtime.holds_tinyjobs())
            condition.wait(0.01)
            self.assertTrue(runtime.holds_tinyjobs())
        self.assertFalse(runtime.holds_tinyjobs())

    def test_a_lock_that_is_gone_is_forgotten(self):
        import gc

        before = set(runtime._tracked)
        lock = runtime.tracked_lock()
        (ref,) = runtime._tracked - before
        self.assertIs(ref(), lock)
        del lock
        gc.collect()
        self.assertNotIn(ref, runtime._tracked)

    @unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
    def test_a_child_frees_the_locks_other_threads_held_and_keeps_its_own(self):
        theirs, mine = runtime.tracked_lock(), runtime.tracked_lock()
        held, release = threading.Event(), threading.Event()

        def hold() -> None:
            with theirs:
                held.set()
                release.wait(5)

        holder = threading.Thread(target=hold)
        holder.start()
        held.wait(5)
        try:
            with mine:
                child = fork_quietly()
                if child == 0:
                    runtime._settle_here(os.getpid())
                    kept = mine._is_owned()
                    os._exit(0 if kept and theirs.acquire(blocking=False) else 1)
        finally:
            release.set()
            holder.join(5)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 0)


class SignalStateTests(unittest.TestCase):
    API = {
        "signal",
        "siginterrupt",
        "set_wakeup_fd",
        "pthread_sigmask",
        "pthread_kill",
        "raise_signal",
        "interrupt_main",
        "setitimer",
        "alarm",
    }

    def setUp(self) -> None:
        self.runner = LoopRunner()
        self.addCleanup(self.runner.close)
        wait_running(self.runner.own_loop())

    def test_tinyjobs_calls_no_signal_api_outside_its_worker_handlers(self):
        import ast
        import pathlib

        package = pathlib.Path(runtime.__file__).parent
        found = []
        for path in package.rglob("*.py"):
            if path == package / "worker" / "signals.py":
                continue
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    names = [alias.name for alias in node.names]
                    module = getattr(node, "module", None) or ""
                    if {"signal", "_signal"} & {module, *names}:
                        found.append((path.name, node.lineno))
                if isinstance(node, ast.Attribute) and node.attr in self.API - {
                    "signal"
                }:
                    found.append((path.name, node.lineno, node.attr))
        self.assertEqual(found, [])

    def calls_and_a_fork(self) -> None:
        for _ in range(3):
            self.runner.run(lambda: asyncio.sleep(0))
        child = fork_quietly()
        if child == 0:
            os._exit(0)
        os.waitpid(child, 0)
        wait_resumed(self)
        self.runner.run(lambda: asyncio.sleep(0))

    def test_a_signal_during_a_sync_call_writes_one_wakeup_byte(self):
        seen: list[int] = []
        previous = signal.signal(
            signal.SIGUSR1, lambda signum, frame: seen.append(signum)
        )
        self.addCleanup(signal.signal, signal.SIGUSR1, previous)
        read, write = os.pipe()
        self.addCleanup(os.close, read)
        self.addCleanup(os.close, write)
        os.set_blocking(read, False)
        os.set_blocking(write, False)
        old = signal.set_wakeup_fd(write)
        self.addCleanup(signal.set_wakeup_fd, old)
        main = threading.get_ident()
        threading.Timer(0.05, signal.pthread_kill, (main, signal.SIGUSR1)).start()
        self.runner.run(lambda: asyncio.sleep(0.2))
        self.assertEqual(seen, [signal.SIGUSR1])
        self.assertEqual(os.read(read, 64), bytes([signal.SIGUSR1]))

    @unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
    def test_sync_calls_and_forks_leave_a_c_level_handler_alone(self):
        import ctypes
        import ctypes.util

        libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
        libc.signal.restype = ctypes.c_void_p
        libc.signal.argtypes = [ctypes.c_int, ctypes.c_void_p]
        ignore = 1
        original = libc.signal(signal.SIGUSR2, ignore)
        self.addCleanup(libc.signal, signal.SIGUSR2, original)
        self.calls_and_a_fork()
        self.assertEqual(libc.signal(signal.SIGUSR2, ignore), ignore)

    @unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
    def test_sync_calls_and_forks_keep_restarting_interrupted_reads(self):
        import ctypes
        import ctypes.util

        libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
        libc.read.restype = ctypes.c_ssize_t
        libc.read.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_size_t]
        seen: list[int] = []
        previous = signal.signal(
            signal.SIGUSR1, lambda signum, frame: seen.append(signum)
        )
        self.addCleanup(signal.signal, signal.SIGUSR1, previous)
        signal.siginterrupt(signal.SIGUSR1, False)
        self.calls_and_a_fork()
        read, write = os.pipe()
        self.addCleanup(os.close, read)
        self.addCleanup(os.close, write)
        main = threading.get_ident()

        def interrupt_then_write() -> None:
            time.sleep(0.1)
            signal.pthread_kill(main, signal.SIGUSR1)
            time.sleep(0.1)
            os.write(write, b"x")

        threading.Thread(target=interrupt_then_write).start()
        buffer = ctypes.create_string_buffer(1)
        self.assertEqual(libc.read(read, buffer, 1), 1, os.strerror(ctypes.get_errno()))
        self.assertEqual(buffer.raw, b"x")
        time.sleep(0)
        self.assertEqual(seen, [signal.SIGUSR1])


class LockScanTests(unittest.TestCase):
    def test_a_lock_held_off_the_main_thread_does_not_count(self):
        lock = runtime.tracked_lock()
        seen = []

        def hold() -> None:
            with lock:
                seen.append(runtime.holds_tinyjobs())
                ident = threading.get_ident()
                runtime._fork_marks[ident] = os.getpid()
                try:
                    seen.append(runtime.holds_tinyjobs())
                finally:
                    runtime._fork_marks.pop(ident, None)

        worker = threading.Thread(target=hold)
        worker.start()
        worker.join(5)
        self.assertEqual(seen, [False, True])

    def test_a_fork_mark_from_another_process_does_not_count(self):
        ident = threading.get_ident()
        runtime._fork_marks[ident] = os.getpid() + 1
        try:
            self.assertFalse(runtime.holds_tinyjobs())
        finally:
            runtime._fork_marks.pop(ident, None)

    def test_the_scan_survives_locks_made_and_dropped_alongside_it(self):
        stop = threading.Event()

        def churn() -> None:
            while not stop.is_set():
                runtime.tracked_lock()

        workers = [threading.Thread(target=churn) for _ in range(2)]
        for worker in workers:
            worker.start()
        try:
            deadline = time.monotonic() + 0.5
            with runtime.tracked_lock():
                while time.monotonic() < deadline:
                    self.assertTrue(runtime._holds_a_lock())
        finally:
            stop.set()
            for worker in workers:
                worker.join(5)

    def test_an_async_call_refused_for_a_held_lock_closes_its_coroutine(self):
        runner = LoopRunner()
        self.addCleanup(runner.close)
        loop = runner.own_loop()
        coroutine = asyncio.sleep(0)

        async def call() -> None:
            with runtime.tracked_lock():
                await runtime.run_on(loop, coroutine, "call")

        with self.assertRaises(runtime.InterruptedCallError):
            asyncio.run(call())
        self.assertEqual(inspect.getcoroutinestate(coroutine), inspect.CORO_CLOSED)


class CollectedTests(unittest.TestCase):
    def test_a_collected_object_runs_its_step_on_another_thread(self):
        import gc

        ran: list = []
        done = threading.Event()

        class Target:
            pass

        def step(value) -> None:
            ran.append((value, threading.get_ident(), threading.current_thread().name))
            done.set()

        def dummies() -> set:
            return {
                key
                for key, thread in list(threading._active.items())
                if isinstance(thread, threading._DummyThread)
            }

        before = dummies()

        target = Target()
        runtime.when_collected(target, step, "value")
        watched = len(runtime._watched)
        del target
        gc.collect()
        self.assertTrue(done.wait(5))
        self.assertEqual(ran[0][0], "value")
        self.assertNotEqual(ran[0][1], threading.get_ident())
        self.assertEqual(ran[0][2], runtime.CLOSE_THREAD_NAME)
        deadline = time.monotonic() + 5
        while len(runtime._watched) >= watched and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertLess(len(runtime._watched), watched)
        self.assertEqual(dummies() - before, set())

    def test_a_collected_runner_closes_without_leaving_a_dummy_thread(self):
        import gc

        before = {
            key
            for key, thread in list(threading._active.items())
            if isinstance(thread, threading._DummyThread)
        }

        class Owner:
            pass

        owner = Owner()
        runner = runtime.LoopRunner()
        runner.run(lambda: asyncio.sleep(0), name="tick")
        loop_thread = runner._thread
        runtime.when_collected(owner, runner.close_here)
        del owner
        gc.collect()
        loop_thread.join(5)
        self.assertFalse(loop_thread.is_alive())
        deadline = time.monotonic() + 5
        while runtime._closing and time.monotonic() < deadline:
            time.sleep(0.005)
        after = {
            key
            for key, thread in list(threading._active.items())
            if isinstance(thread, threading._DummyThread)
        }
        self.assertEqual(after - before, set())

    def test_a_forgotten_target_runs_no_step_when_it_is_collected(self):
        import gc

        ran: list = []

        class Target:
            pass

        target = Target()
        forget = runtime.when_collected(target, ran.append, "ran")
        watched = len(runtime._watched)
        forget()
        self.assertEqual(len(runtime._watched), watched - 1)
        del target
        gc.collect()
        time.sleep(0.1)
        self.assertEqual(ran, [])

    def test_the_exit_cancels_the_steps_still_waiting_for_a_collection(self):
        spawns = [mock.Mock()]
        watched = {mock.sentinel.ref: spawns}
        with (
            mock.patch.object(runtime, "_watched", watched),
            mock.patch.object(runtime, "_runners", weakref_set()),
            mock.patch.object(runtime, "_exit_steps", []),
            mock.patch.object(runtime, "_closing", set()),
            mock.patch.object(runtime, "_exiting", threading.Event()) as exiting,
        ):
            runtime._at_exit()
        self.assertTrue(exiting.is_set())
        self.assertEqual((spawns, watched), ([], {}))
        spawns.clear()

    def test_a_collected_step_is_tracked_until_it_ends(self):
        import gc

        seen: list[bool] = []
        done = threading.Event()

        class Target:
            pass

        def step() -> None:
            seen.append(threading.current_thread() in runtime._closing)
            done.set()

        target = Target()
        runtime.when_collected(target, step)
        del target
        gc.collect()
        self.assertTrue(done.wait(5))
        self.assertEqual(seen, [True])

    def test_the_exit_waits_for_a_closing_thread_that_is_still_starting(self):
        ran: list[bool] = []

        def step() -> None:
            time.sleep(0.05)
            ran.append(True)
            runtime._closing.discard(thread)

        thread = threading.Thread(target=step)
        runtime._closing.add(thread)
        self.addCleanup(runtime._closing.discard, thread)
        threading.Timer(0.1, thread.start).start()
        runtime._join_closing(thread)
        self.assertEqual(ran, [True])

    def test_the_exit_gives_up_on_a_closing_thread_that_never_starts(self):
        thread = threading.Thread(target=lambda: None)
        runtime._closing.add(thread)
        self.addCleanup(runtime._closing.discard, thread)
        with mock.patch.object(runtime, "STOP_SECONDS", 0.05):
            started = time.monotonic()
            runtime._join_closing(thread)
        self.assertLess(time.monotonic() - started, 1.0)

    def test_the_loops_still_running_at_exit_are_wound_down(self):
        import subprocess
        import sys

        script = (
            "import asyncio, sys\n"
            "from tinyjobs.runtime import LoopRunner\n"
            "class Resident:\n"
            "    async def close(self):\n"
            "        open(sys.argv[1], 'w').write('closed')\n"
            "runner = LoopRunner()\n"
            "runner.keep(runner.own_loop(), Resident())\n"
            "runner.run(lambda: asyncio.sleep(0), name='tick')\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            marker = os.path.join(directory, "closed")
            finished = subprocess.run(
                [sys.executable, "-c", script, marker],
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(finished.returncode, 0, finished.stderr)
            self.assertEqual(finished.stderr, "")
            self.assertTrue(os.path.exists(marker))


class RestingLoopTests(unittest.TestCase):
    def test_a_loop_counts_as_served_while_its_thread_starts(self):
        runner = LoopRunner()
        self.addCleanup(runner.close)
        proceed = threading.Event()
        serve = runner._serve

        def slow_serve(loop, ready) -> None:
            proceed.wait(5)
            serve(loop, ready)

        runner._serve = slow_serve
        loop = runner.own_loop()
        self.assertFalse(loop.is_running())
        self.assertTrue(runner.serves(loop))
        self.assertFalse(runtime._gone(loop))
        proceed.set()
        self.assertEqual(runner.run(lambda: asyncio.sleep(0, "ran")), "ran")
        self.assertNotIn(loop, runtime._resting)
        self.assertTrue(runner.serves(loop))


class CollectedElsewhereTests(unittest.TestCase):
    def test_a_target_collected_in_another_process_runs_no_step(self):
        ran: list[str] = []
        before = threading.active_count()
        runtime._collected([None], os.getpid() + 1, ran.append, ("step",))
        self.assertEqual(ran, [])
        self.assertEqual(threading.active_count(), before)

    @unittest.skipUnless(hasattr(os, "fork"), "needs os.fork")
    def test_a_forked_child_starts_no_thread_for_the_parent_threads(self):
        import subprocess
        import sys

        script = (
            "import os, sys, threading\n"
            "from tinyjobs.backends.sqlite.connection import ConnectionPool\n"
            "pool = ConnectionPool(sys.argv[1])\n"
            "ready, release = threading.Barrier(6), threading.Event()\n"
            "def hold():\n"
            "    pool.connection.execute('SELECT 1')\n"
            "    ready.wait()\n"
            "    release.wait()\n"
            "threads = [threading.Thread(target=hold, daemon=True) for _ in range(5)]\n"
            "for thread in threads:\n"
            "    thread.start()\n"
            "ready.wait()\n"
            "spawned = []\n"
            "sys.addaudithook(lambda event, args: event == '_thread.start_new_thread' and spawned.append(os.getpid()))\n"
            "child = os.fork()\n"
            "if child == 0:\n"
            "    os._exit(spawned.count(os.getpid()))\n"
            "_, status = os.waitpid(child, 0)\n"
            "release.set()\n"
            "sys.exit(os.waitstatus_to_exitcode(status))\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            finished = subprocess.run(
                [sys.executable, "-W", "ignore", "-c", script, os.path.join(directory, "jobs.db")],
                capture_output=True,
                text=True,
                timeout=60,
            )
        self.assertEqual(finished.returncode, 0, finished.stderr)

    def test_nothing_is_watched_once_the_process_is_exiting(self):
        class Target:
            pass

        target = Target()
        with mock.patch.object(runtime, "_exiting", threading.Event()) as exiting:
            exiting.set()
            forget = runtime.when_collected(target, lambda: None)
            ref = next(ref for ref in runtime._watched if ref() is target)
            self.assertEqual(runtime._watched[ref], [])
        forget()

    def test_a_fork_retires_an_executor_once_it_is_idle(self):
        paused: list[str] = []

        class Owner:
            def step_aside_for_fork(self) -> None:
                paused.append("paused")

        owner = Owner()
        runtime.pause_threads_at_fork(owner)
        self.addCleanup(runtime._fork_pausers.discard, owner)
        with mock.patch.object(runtime, "LOOP_THREAD_PREFIX", "no-such-thread"):
            runtime._pause_idle_owners()
        self.assertEqual(paused, ["paused"])


class KeptTaskTests(unittest.TestCase):
    def test_a_hand_back_submitted_as_the_loop_closes_still_runs(self):
        runner = LoopRunner()
        loop = runner.own_loop()
        wait_running(loop)
        done: list[str] = []

        async def hand_back() -> None:
            await asyncio.sleep(0.05)
            done.append("handed back")

        outcome = runtime.submit(loop, hand_back(), "hand_back", returning=True)
        runner.close_here()
        outcome.result(5)
        self.assertEqual(done, ["handed back"])

    def test_other_work_submitted_as_the_loop_closes_is_refused(self):
        runner = LoopRunner()
        loop = runner.own_loop()
        wait_running(loop)

        async def work() -> None:
            await asyncio.sleep(0.05)

        outcome = runtime.submit(loop, work(), "work")
        runner.close_here()
        with self.assertRaises(Exception):
            outcome.result(5)

    def test_a_kept_task_started_while_the_loop_winds_down_is_waited_for(self):
        runner = LoopRunner()
        loop = runner.own_loop()
        wait_running(loop)
        done: list[str] = []

        async def late() -> None:
            await asyncio.sleep(0.05)
            done.append("late")

        started = threading.Event()

        async def first() -> None:
            started.set()
            await asyncio.sleep(0.02)
            runtime.submit(loop, late(), "late", returning=True)

        runtime.submit(loop, first(), "first", kept=True)
        self.assertTrue(started.wait(5))
        runner.close_here()
        self.assertEqual(done, ["late"])

    def test_a_hand_back_submitted_while_residents_close_still_runs(self):
        runner = LoopRunner()
        loop = runner.own_loop()
        wait_running(loop)
        closing = threading.Event()
        done: list[str] = []
        outcomes: list = []

        class Resident:
            async def close(self) -> None:
                closing.set()
                await asyncio.sleep(0.2)

        async def hand_back() -> str:
            await asyncio.sleep(0.3)
            done.append("handed back")
            return "handed back"

        def late() -> None:
            closing.wait(5)
            outcomes.append(
                runtime.submit(loop, hand_back(), "hand_back", returning=True)
            )

        runner.keep(loop, Resident())
        thread = threading.Thread(target=late)
        thread.start()
        runner.close_here()
        thread.join(5)
        self.assertEqual(outcomes[0].result(5), "handed back")
        self.assertEqual(done, ["handed back"])

    def test_a_task_that_ignores_cancellation_does_not_keep_the_loop_open(self):
        runner = LoopRunner()
        loop = runner.own_loop()
        wait_running(loop)
        started = threading.Event()
        release = threading.Event()

        async def stubborn() -> None:
            started.set()
            while not release.is_set():
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    pass

        abandoned: list[dict] = []
        loop.call_soon_threadsafe(
            loop.set_exception_handler, lambda _, context: abandoned.append(context)
        )
        runtime.submit(loop, stubborn(), "stubborn")
        self.assertTrue(started.wait(5))
        self.addCleanup(release.set)
        with (
            mock.patch.object(runtime, "STOP_SECONDS", 0.2),
            self.assertLogs("tinyjobs.runtime", "WARNING") as logs,
        ):
            began = time.monotonic()
            runner.close_here()
            while not loop.is_closed() and time.monotonic() - began < 5:
                time.sleep(0.01)
        self.assertLess(time.monotonic() - began, 3.0)
        self.assertTrue(loop.is_closed())
        self.assertIn("ignored cancellation", logs.output[0])
        gc.collect()
        self.assertEqual(
            [context["message"] for context in abandoned],
            ["Task was destroyed but it is pending!"],
        )


    def test_other_tasks_are_cancelled_while_a_kept_task_outlasts_the_close(self):
        runner = LoopRunner()
        loop = runner.own_loop()
        wait_running(loop)
        events: list[str] = []
        started = threading.Event()

        async def kept() -> None:
            await asyncio.sleep(1)

        async def ordinary() -> None:
            started.set()
            try:
                await asyncio.sleep(3600)
            finally:
                events.append("cleaned up")

        runtime.submit(loop, kept(), "kept", kept=True)
        runtime.submit(loop, ordinary(), "ordinary")
        self.assertTrue(started.wait(5))
        with mock.patch.object(runtime, "STOP_SECONDS", 0.3):
            began = time.monotonic()
            runner.close_here()
        self.assertLess(time.monotonic() - began, 3.0)
        self.assertEqual(events, ["cleaned up"])

    def test_the_tasks_a_kept_task_waits_on_are_kept_too(self):
        runner = LoopRunner()
        loop = runner.own_loop()
        wait_running(loop)
        started = threading.Event()

        async def step(label: str) -> str:
            started.set()
            await asyncio.sleep(0.2)
            return label

        async def kept() -> list:
            return [
                await asyncio.shield(step("shielded")),
                *(await asyncio.gather(step("gathered"))),
            ]

        outcome = runtime.submit(loop, kept(), "kept", kept=True)
        self.assertTrue(started.wait(5))
        runner.close_here()
        self.assertEqual(outcome.result(5), ["shielded", "gathered"])

class OsThreadTests(unittest.TestCase):
    def test_a_thread_counts_as_finished_only_once_it_has_ended(self):
        import types

        release = threading.Event()
        thread = threading.Thread(target=release.wait, daemon=True)
        thread.start()
        self.addCleanup(release.set)
        with mock.patch.object(thread, "is_alive", side_effect=AssertionError):
            self.assertFalse(runtime._finished(thread))
        release.set()
        thread.join(5)
        with mock.patch.object(thread, "is_alive", side_effect=AssertionError):
            self.assertTrue(runtime._finished(thread))
        self.assertTrue(runtime._finished(None))
        self.assertFalse(
            runtime._finished(types.SimpleNamespace(is_alive=lambda: True))
        )

    def threads(self, *others: threading.Thread, current=None):
        import types

        current = current or threading.current_thread()
        active = {current.ident: current}
        active.update({index: thread for index, thread in enumerate(others, 1)})
        return mock.patch.object(
            runtime, "threading", types.SimpleNamespace(_active=active, _limbo={})
        )

    def named(self, name: str) -> threading.Thread:
        return threading.Thread(target=lambda: None, name=name)

    def test_only_tinyjobs_threads_are_paused_for(self):
        owned = self.named(runtime.CONTROL_THREAD_NAME)
        with (
            self.threads(owned),
            mock.patch.object(runtime, "_os_thread_count", return_value=None),
        ):
            self.assertTrue(runtime._pause_wanted(threading.get_ident()))

    def test_threads_outside_python_skip_the_pause(self):
        owned = self.named(runtime.LOOP_THREAD_PREFIX)
        with (
            self.threads(owned),
            mock.patch.object(runtime, "_os_thread_count", return_value=3),
        ):
            self.assertFalse(runtime._pause_wanted(threading.get_ident()))

    def test_threads_of_the_application_skip_the_pause(self):
        with self.threads(self.named(runtime.LOOP_THREAD_PREFIX), self.named("app")):
            self.assertFalse(runtime._pause_wanted(threading.get_ident()))

    def test_a_process_without_tinyjobs_threads_skips_the_pause(self):
        with self.threads():
            self.assertFalse(runtime._pause_wanted(threading.get_ident()))

    def test_a_fork_from_a_tinyjobs_thread_skips_the_pause(self):
        loop_thread = self.named(runtime.LOOP_THREAD_PREFIX)
        loop_thread._ident = 99
        with self.threads(self.named(runtime.CONTROL_THREAD_NAME), current=loop_thread):
            self.assertFalse(runtime._pause_wanted(99))

    def pause_here(self) -> bool:
        ident = threading.get_ident()
        runtime._fork_marks[ident] = os.getpid()
        self.addCleanup(runtime._fork_marks.pop, ident, None)
        self.addCleanup(setattr, runtime, "_pausing", False)
        acked = threading.Lock()
        acked.acquire()
        resume = runtime._after_fork_actions[ident] = mock.Mock()
        self.addCleanup(runtime._after_fork_actions.pop, ident, None)
        return runtime._pause(ident, os.getpid(), acked, resume)

    def test_a_loop_still_running_leaves_the_backends_alone(self):
        owner = mock.Mock()
        loop_thread = self.named(runtime.LOOP_THREAD_PREFIX)
        with (
            mock.patch.object(runtime, "_runners", weakref_set()),
            mock.patch.object(runtime, "_fork_pausers", weakref_set(owner)),
            mock.patch.object(
                threading,
                "enumerate",
                return_value=[threading.current_thread(), loop_thread],
            ),
        ):
            self.assertTrue(self.pause_here())
        owner.pause_for_fork.assert_not_called()

    def test_the_backends_pause_once_no_loop_runs(self):
        owner = mock.Mock()
        with (
            mock.patch.object(runtime, "_runners", weakref_set()),
            mock.patch.object(runtime, "_fork_pausers", weakref_set(owner)),
            mock.patch.object(
                threading, "enumerate", return_value=[threading.current_thread()]
            ),
        ):
            self.assertTrue(self.pause_here())
        owner.pause_for_fork.assert_called_once_with()

    def test_the_fork_waits_for_the_threads_the_pause_ended(self):
        acked = threading.Lock()
        with (
            mock.patch.object(runtime, "_os_thread_count", return_value=1),
            mock.patch.object(runtime, "_wait_for_os_threads") as wait,
        ):
            runtime._wait_for_pause(acked)
        wait.assert_called_once_with(len(threading._active))

    def test_the_fork_stops_waiting_for_an_acknowledgement_that_never_comes(self):
        acked = threading.Lock()
        acked.acquire()
        with mock.patch.object(runtime, "FORK_ACK_SECONDS", 0.05):
            started = time.monotonic()
            runtime._wait_for_pause(acked)
        self.assertLess(time.monotonic() - started, runtime.THREAD_EXIT_SECONDS)

    def test_the_wait_ends_once_the_count_drops_to_what_is_expected(self):
        counts = iter([5, 5, 4, 3, 3])
        with mock.patch.object(
            runtime, "_os_thread_count", side_effect=lambda: next(counts)
        ):
            started = time.monotonic()
            runtime._wait_for_os_threads(3)
        self.assertLess(time.monotonic() - started, runtime.THREAD_EXIT_SECONDS / 2)
        self.assertEqual(list(counts), [3])

    def test_the_wait_gives_up_after_its_deadline(self):
        with mock.patch.object(runtime, "_os_thread_count", return_value=9):
            started = time.monotonic()
            runtime._wait_for_os_threads(3)
        self.assertGreaterEqual(
            time.monotonic() - started, runtime.THREAD_EXIT_SECONDS * 0.9
        )


def weakref_set(*items):
    import weakref

    return weakref.WeakSet(items)


if __name__ == "__main__":
    unittest.main()
