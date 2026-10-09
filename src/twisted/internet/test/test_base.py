# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Tests for L{twisted.internet.base}.
"""

import socket
from queue import Queue
from typing import Callable
from unittest import skipIf

from zope.interface import implementer

from typing_extensions import ParamSpec

from twisted.internet._resolver import FirstOneWins
from twisted.internet.base import DelayedCall, ReactorBase, ThreadedResolver
from twisted.internet.defer import Deferred
from twisted.internet.error import DNSLookupError
from twisted.internet import error
from twisted.internet.interfaces import IReactorThreads, IReactorTime, IResolverSimple
from twisted.internet.task import Clock
from twisted.python.threadpool import ThreadPool
from twisted.trial.unittest import SkipTest, TestCase

try:
    import signal as _signal
except ImportError:
    signal = None
else:
    signal = _signal

_P = ParamSpec("_P")


@implementer(IReactorTime, IReactorThreads)
class FakeReactor:
    """
    A fake reactor implementation which just supports enough reactor APIs for
    L{ThreadedResolver}.
    """

    def __init__(self):
        self._clock = Clock()
        self.callLater = self._clock.callLater

        self._threadpool = ThreadPool()
        self._threadpool.start()
        self.getThreadPool = lambda: self._threadpool

        self._threadCalls = Queue()

    def callFromThread(
        self, callable: Callable[_P, object], *args: _P.args, **kwargs: _P.kwargs
    ) -> None:
        self._threadCalls.put((callable, args, kwargs))

    def _runThreadCalls(self):
        f, args, kwargs = self._threadCalls.get()
        f(*args, **kwargs)

    def _stop(self):
        self._threadpool.stop()

    def getDelayedCalls(self):
        # IReactorTime.getDelayedCalls
        pass

    def seconds(self) -> float:  # type: ignore[empty-body]
        # IReactorTime.seconds
        pass

    def callInThread(
        self, callable: Callable[_P, object], *args: _P.args, **kwargs: _P.kwargs
    ) -> None:
        # IReactorInThreads.callInThread
        pass

    def suggestThreadPoolSize(self, size):
        # IReactorThreads.suggestThreadPoolSize
        pass


class ThreadedResolverTests(TestCase):
    """
    Tests for L{ThreadedResolver}.
    """

    def test_success(self):
        """
        L{ThreadedResolver.getHostByName} returns a L{Deferred} which fires
        with the value returned by the call to L{socket.gethostbyname} in the
        threadpool of the reactor passed to L{ThreadedResolver.__init__}.
        """
        ip = "10.0.0.17"
        name = "foo.bar.example.com"
        timeout = 30

        reactor = FakeReactor()
        self.addCleanup(reactor._stop)

        lookedUp = []
        resolvedTo = []

        def fakeGetHostByName(name):
            lookedUp.append(name)
            return ip

        self.patch(socket, "gethostbyname", fakeGetHostByName)

        resolver = ThreadedResolver(reactor)
        d = resolver.getHostByName(name, (timeout,))
        d.addCallback(resolvedTo.append)

        reactor._runThreadCalls()

        self.assertEqual(lookedUp, [name])
        self.assertEqual(resolvedTo, [ip])

        # Make sure that any timeout-related stuff gets cleaned up.
        reactor._clock.advance(timeout + 1)
        self.assertEqual(reactor._clock.calls, [])

    def test_failure(self):
        """
        L{ThreadedResolver.getHostByName} returns a L{Deferred} which fires a
        L{Failure} if the call to L{socket.gethostbyname} raises an exception.
        """
        timeout = 30

        reactor = FakeReactor()
        self.addCleanup(reactor._stop)

        def fakeGetHostByName(name):
            raise OSError("ENOBUFS (this is a funny joke)")

        self.patch(socket, "gethostbyname", fakeGetHostByName)

        failedWith = []
        resolver = ThreadedResolver(reactor)
        d = resolver.getHostByName("some.name", (timeout,))
        self.assertFailure(d, DNSLookupError)
        d.addCallback(failedWith.append)

        reactor._runThreadCalls()

        self.assertEqual(len(failedWith), 1)

        # Make sure that any timeout-related stuff gets cleaned up.
        reactor._clock.advance(timeout + 1)
        self.assertEqual(reactor._clock.calls, [])

    def test_timeout(self):
        """
        If L{socket.gethostbyname} does not complete before the specified
        timeout elapsed, the L{Deferred} returned by
        L{ThreadedResolver.getHostByName} fails with L{DNSLookupError}.
        """
        timeout = 10

        reactor = FakeReactor()
        self.addCleanup(reactor._stop)

        result = Queue()

        def fakeGetHostByName(name):
            raise result.get()

        self.patch(socket, "gethostbyname", fakeGetHostByName)

        failedWith = []
        resolver = ThreadedResolver(reactor)
        d = resolver.getHostByName("some.name", (timeout,))
        self.assertFailure(d, DNSLookupError)
        d.addCallback(failedWith.append)

        reactor._clock.advance(timeout - 1)
        self.assertEqual(failedWith, [])
        reactor._clock.advance(1)
        self.assertEqual(len(failedWith), 1)

        # Eventually the socket.gethostbyname does finish - in this case, with
        # an exception.  Nobody cares, though.
        result.put(IOError("The I/O was errorful"))

    def test_resolverGivenStr(self):
        """
        L{ThreadedResolver.getHostByName} is passed L{str}, encoded using IDNA
        if required.
        """
        calls = []

        @implementer(IResolverSimple)
        class FakeResolver:
            def getHostByName(self, name, timeouts=()):
                calls.append(name)
                return Deferred()

        class JustEnoughReactor(ReactorBase):
            def installWaker(self):
                pass

        fake = FakeResolver()
        reactor = JustEnoughReactor()
        reactor.installResolver(fake)
        rec = FirstOneWins(Deferred())
        reactor.nameResolver.resolveHostName(rec, "example.example")
        reactor.nameResolver.resolveHostName(rec, "example.example")
        reactor.nameResolver.resolveHostName(rec, "v\xe4\xe4ntynyt.example")
        reactor.nameResolver.resolveHostName(rec, "\u0440\u0444.example")
        reactor.nameResolver.resolveHostName(rec, "xn----7sbb4ac0ad0be6cf.xn--p1ai")

        self.assertEqual(len(calls), 5)
        self.assertEqual(list(map(type, calls)), [str] * 5)
        self.assertEqual("example.example", calls[0])
        self.assertEqual("example.example", calls[1])
        self.assertEqual("xn--vntynyt-5waa.example", calls[2])
        self.assertEqual("xn--p1ai.example", calls[3])
        self.assertEqual("xn----7sbb4ac0ad0be6cf.xn--p1ai", calls[4])


def nothing():
    """
    Function used by L{DelayedCallTests.test_str}.
    """


class DelayedCallMixin:
    """
    L{DelayedCall}
    """

    def _getDelayedCallAt(self, time):
        """
        Get a L{DelayedCall} instance at a given C{time}.

        @param time: The absolute time at which the returned L{DelayedCall}
            will be scheduled.
        """

        def noop(call):
            pass

        return DelayedCall(time, lambda: None, (), {}, noop, noop, None)

    def setUp(self):
        """
        Create two L{DelayedCall} instanced scheduled to run at different
        times.
        """
        self.zero = self._getDelayedCallAt(0)
        self.one = self._getDelayedCallAt(1)

    def test_str(self):
        """
        The string representation of a L{DelayedCall} instance, as returned by
        L{str}, includes the unsigned id of the instance, as well as its state,
        the function to be called, and the function arguments.
        """
        dc = DelayedCall(12, nothing, (3,), {"A": 5}, None, None, lambda: 1.5)
        self.assertEqual(
            str(dc),
            "<DelayedCall 0x%x [10.5s] called=0 cancelled=0 nothing(3, A=5)>"
            % (id(dc),),
        )

    def test_repr(self):
        """
        The string representation of a L{DelayedCall} instance, as returned by
        {repr}, is identical to that returned by L{str}.
        """
        dc = DelayedCall(13, nothing, (6,), {"A": 9}, None, None, lambda: 1.6)
        self.assertEqual(str(dc), repr(dc))

    def test_lt(self):
        """
        For two instances of L{DelayedCall} C{a} and C{b}, C{a < b} is true
        if and only if C{a} is scheduled to run before C{b}.
        """
        zero, one = self.zero, self.one
        self.assertTrue(zero < one)
        self.assertFalse(one < zero)
        self.assertFalse(zero < zero)
        self.assertFalse(one < one)

    def test_le(self):
        """
        For two instances of L{DelayedCall} C{a} and C{b}, C{a <= b} is true
        if and only if C{a} is scheduled to run before C{b} or at the same
        time as C{b}.
        """
        zero, one = self.zero, self.one
        self.assertTrue(zero <= one)
        self.assertFalse(one <= zero)
        self.assertTrue(zero <= zero)
        self.assertTrue(one <= one)

    def test_gt(self):
        """
        For two instances of L{DelayedCall} C{a} and C{b}, C{a > b} is true
        if and only if C{a} is scheduled to run after C{b}.
        """
        zero, one = self.zero, self.one
        self.assertTrue(one > zero)
        self.assertFalse(zero > one)
        self.assertFalse(zero > zero)
        self.assertFalse(one > one)

    def test_ge(self):
        """
        For two instances of L{DelayedCall} C{a} and C{b}, C{a > b} is true
        if and only if C{a} is scheduled to run after C{b} or at the same
        time as C{b}.
        """
        zero, one = self.zero, self.one
        self.assertTrue(one >= zero)
        self.assertFalse(zero >= one)
        self.assertTrue(zero >= zero)
        self.assertTrue(one >= one)

    def test_eq(self):
        """
        A L{DelayedCall} instance is only equal to itself.
        """
        # Explicitly use == here, instead of assertEqual, to be more
        # confident __eq__ is being tested.
        self.assertFalse(self.zero == self.one)
        self.assertTrue(self.zero == self.zero)
        self.assertTrue(self.one == self.one)

    def test_ne(self):
        """
        A L{DelayedCall} instance is not equal to any other object.
        """
        # Explicitly use != here, instead of assertEqual, to be more
        # confident __ne__ is being tested.
        self.assertTrue(self.zero != self.one)
        self.assertFalse(self.zero != self.zero)
        self.assertFalse(self.one != self.one)


class DelayedCallNoDebugTests(DelayedCallMixin, TestCase):
    """
    L{DelayedCall}
    """

    def setUp(self):
        """
        Turn debug off.
        """
        self.patch(DelayedCall, "debug", False)
        DelayedCallMixin.setUp(self)

    def test_str(self):
        """
        The string representation of a L{DelayedCall} instance, as returned by
        L{str}, includes the unsigned id of the instance, as well as its state,
        the function to be called, and the function arguments.
        """
        dc = DelayedCall(12, nothing, (3,), {"A": 5}, None, None, lambda: 1.5)
        expected = (
            "<DelayedCall 0x{:x} [10.5s] called=0 cancelled=0 "
            "nothing(3, A=5)>".format(id(dc))
        )
        self.assertEqual(str(dc), expected)

    def test_switchToDebug(self):
        """
        If L{DelayedCall.debug} changes from C{0} to C{1} between
        L{DelayeCall.__init__} and L{DelayedCall.__repr__} then
        L{DelayedCall.__repr__} returns a string that does not include the
        creator stack.
        """
        dc = DelayedCall(3, lambda: None, (), {}, nothing, nothing, lambda: 2)
        dc.debug = 1
        self.assertNotIn("traceback at creation", repr(dc))


class DelayedCallDebugTests(DelayedCallMixin, TestCase):
    """
    L{DelayedCall}
    """

    def setUp(self):
        """
        Turn debug on.
        """
        self.patch(DelayedCall, "debug", True)
        DelayedCallMixin.setUp(self)

    def test_str(self):
        """
        The string representation of a L{DelayedCall} instance, as returned by
        L{str}, includes the unsigned id of the instance, as well as its state,
        the function to be called, and the function arguments.
        """
        dc = DelayedCall(12, nothing, (3,), {"A": 5}, None, None, lambda: 1.5)
        expectedRegexp = (
            "<DelayedCall 0x{:x} \\[10.5s\\] called=0 cancelled=0 "
            "nothing\\(3, A=5\\)\n\n"
            "traceback at creation:".format(id(dc))
        )
        self.assertRegex(str(dc), expectedRegexp)

    def test_switchFromDebug(self):
        """
        If L{DelayedCall.debug} changes from C{1} to C{0} between
        L{DelayeCall.__init__} and L{DelayedCall.__repr__} then
        L{DelayedCall.__repr__} returns a string that includes the creator
        stack (we captured it, we might as well display it).
        """
        dc = DelayedCall(3, lambda: None, (), {}, nothing, nothing, lambda: 2)
        dc.debug = 0
        self.assertIn("traceback at creation", repr(dc))


class DelayedCallSameTimeOrderingTests(TestCase):
    """
    L{DelayedCall} instances scheduled for the same time are ordered by the
    sequence in which they were registered.
    """

    def setUp(self):
        self.first = DelayedCall(
            1.0, lambda: None, (), {}, nothing, nothing, lambda: 0.0
        )
        self.second = DelayedCall(
            1.0, lambda: None, (), {}, nothing, nothing, lambda: 0.0
        )

    def test_lt(self):
        """
        The earlier registration compares as less than the later one even
        though both are scheduled for the same time.
        """
        self.assertTrue(self.first < self.second)
        self.assertFalse(self.second < self.first)

    def test_le(self):
        """
        The earlier registration compares as less-than-or-equal to the later
        one; distinct registrations at the same time do not compare equal.
        """
        self.assertTrue(self.first <= self.second)
        self.assertFalse(self.second <= self.first)


class _DeterministicTimedCallReactor:
    """
    Build just enough of a L{ReactorBase} to deterministically drive its
    timed-call machinery (L{ReactorBase.callLater},
    L{ReactorBase.runUntilCurrent} and friends) without a real event loop.
    """

    def __init__(self):
        reactor = ReactorBase.__new__(ReactorBase)
        reactor._pendingTimedCalls = []
        reactor._newTimedCalls = []
        reactor._cancellations = 0
        reactor.threadCallQueue = []
        reactor._justStopped = False
        reactor._stopped = False
        reactor._eventTriggers = {}
        self._now = 0.0
        reactor.seconds = self.seconds
        self.reactor = reactor

    def seconds(self):
        return self._now

    def callLater(self, delay, f, *args, **kwargs):
        return self.reactor.callLater(delay, f, *args, **kwargs)

    def getDelayedCalls(self):
        return self.reactor.getDelayedCalls()

    def advance(self, now):
        """
        Move the clock to C{now} and run one reactor batch.
        """
        self._now = now
        self.reactor._insertNewDelayedCalls()
        self.reactor.runUntilCurrent()
        return self


class TimedCallBatchTests(TestCase):
    """
    Deterministic tests for the dispatch of multiple timed calls which
    become due at the same moment, including cancellation, rescheduling,
    clock jumps, errors and shutdown.
    """

    def test_sameTimeCallsRunInRegistrationOrder(self):
        """
        Many calls scheduled for the same moment are dispatched in their
        registration order.
        """
        harness = _DeterministicTimedCallReactor()
        events = []
        count = 12
        for i in range(count):
            harness.callLater(10.0, events.append, i)
        harness.advance(10.0)
        self.assertEqual(events, list(range(count)))

    def test_cancelledCallsDoNotRunAndDoNotReorderTheRest(self):
        """
        Cancelling some of the calls due at the same moment keeps the
        relative order of the remaining calls; cancelled calls are not part
        of the reported pending set and never run.
        """
        harness = _DeterministicTimedCallReactor()
        events = []
        calls = [harness.callLater(10.0, events.append, i) for i in range(12)]
        harness.reactor._insertNewDelayedCalls()
        calls[3].cancel()
        calls[6].cancel()
        harness.advance(10.0)
        self.assertEqual(events, [i for i in range(12) if i not in (3, 6)])
        self.assertEqual(harness.getDelayedCalls(), [])

    def test_resetLaterFromDueCallbackRunsOnceAtNewTime(self):
        """
        A due call which is reset to a later moment from within another due
        callback runs exactly once, at the new time, while the other due
        calls keep their registration order.
        """
        harness = _DeterministicTimedCallReactor()
        events = []
        holder = {}

        def reschedule():
            events.append("a")
            holder["b"].reset(5.0)

        harness.callLater(10.0, reschedule)
        holder["b"] = harness.callLater(10.0, events.append, "b")
        harness.callLater(10.0, events.append, "c")
        harness.callLater(10.0, events.append, "d")
        harness.advance(10.0)
        self.assertEqual(events, ["a", "c", "d"])
        harness.advance(15.0)
        self.assertEqual(events, ["a", "c", "d", "b"])

    def test_cancelFromDueCallback(self):
        """
        A due call cancelled from within another due callback does not run;
        the rest of the batch is unaffected.
        """
        harness = _DeterministicTimedCallReactor()
        events = []
        holder = {}

        def cancel():
            events.append("a")
            holder["b"].cancel()

        harness.callLater(10.0, cancel)
        holder["b"] = harness.callLater(10.0, events.append, "b")
        harness.callLater(10.0, events.append, "c")
        harness.advance(10.0)
        self.assertEqual(events, ["a", "c"])
        self.assertEqual(harness.getDelayedCalls(), [])

    def test_resetSoonerRunsOnceAtNewTime(self):
        """
        A call reset to an earlier time runs exactly once at the new time
        and never runs at the originally scheduled time.
        """
        harness = _DeterministicTimedCallReactor()
        events = []
        call = harness.callLater(20.0, events.append, "x")
        harness.reactor._insertNewDelayedCalls()
        harness.advance(5.0)
        call.reset(5.0)  # 5 seconds after the 5.0 reading -> time 10
        harness.advance(9.0)
        self.assertEqual(events, [])
        harness.advance(10.0)
        self.assertEqual(events, ["x"])
        harness.advance(20.0)
        self.assertEqual(events, ["x"])

    def test_clockJumpForwardRunsDueBatchOnly(self):
        """
        A forward jump which makes many calls due at once dispatches them
        once each in registration order, without running calls that are
        still in the future.
        """
        harness = _DeterministicTimedCallReactor()
        events = []
        for i in range(12):
            harness.callLater(10.0, events.append, i)
        for i in range(12, 20):
            harness.callLater(100.0, events.append, i)
        harness.advance(55.0)
        self.assertEqual(events, list(range(12)))
        harness.advance(100.0)
        self.assertEqual(events, list(range(20)))

    def test_clockJumpBackwardDoesNotRunOrRepeat(self):
        """
        A backward jump must not execute pending calls (nor repeat executed
        ones), and a reset afterwards is relative to the jumped-back clock.
        """
        harness = _DeterministicTimedCallReactor()
        events = []
        call = harness.callLater(100.0, events.append, "x")
        harness.reactor._insertNewDelayedCalls()
        harness._now = 120.0
        harness._now = 50.0
        harness.reactor.runUntilCurrent()
        self.assertEqual(events, [])
        call.reset(10.0)  # 50 + 10 -> time 60
        harness.advance(60.0)
        self.assertEqual(events, ["x"])
        harness.advance(120.0)
        self.assertEqual(events, ["x"])

    def test_exceptionInOneDueCallbackIsolatesTheOthers(self):
        """
        When a due callback raises, the other due callbacks still run once
        each and the failure is reported through logging.
        """
        harness = _DeterministicTimedCallReactor()
        events = []

        def boom():
            events.append("boom")
            raise RuntimeError("boom")

        harness.callLater(10.0, events.append, "a")
        harness.callLater(10.0, boom)
        harness.callLater(10.0, events.append, "c")
        harness.advance(10.0)
        self.assertEqual(events, ["a", "boom", "c"])
        failures = self.flushLoggedErrors(RuntimeError)
        self.assertEqual(len(failures), 1)

    def test_stopDuringBatchPurgesDueCallsAndFiresShutdownOnce(self):
        """
        When the reactor is stopped while a due batch is running, the due
        calls each run once; zero-delay calls registered during the batch
        (which would only be inserted on the next, now-never iteration) are
        discarded rather than left pending; future calls stay pending; the
        shutdown event fires once.
        """
        harness = _DeterministicTimedCallReactor()
        events = []
        shutdowns = []
        zeroDelayHolder = {}

        class _Shutdown:
            def fireEvent(self):
                shutdowns.append("shutdown")

        harness.reactor._eventTriggers["shutdown"] = _Shutdown()

        def stop():
            events.append("stop")
            harness.reactor.stop()

        for i in range(6):
            harness.callLater(10.0, events.append, f"before{i}")
        harness.callLater(10.0, stop)
        for i in range(6):
            harness.callLater(10.0, events.append, f"after{i}")
        future = harness.callLater(50.0, events.append, "future")

        def registerZeroDelay():
            events.append("register")
            zeroDelayHolder["call"] = harness.callLater(
                0.0, events.append, "never"
            )

        harness.callLater(10.0, registerZeroDelay)
        harness.advance(10.0)
        # The whole due batch ran once; the zero-delay call did not.
        self.assertEqual(
            events,
            [f"before{i}" for i in range(6)]
            + ["stop"]
            + [f"after{i}" for i in range(6)]
            + ["register"],
        )
        # Simulate mainLoop's timeout() insertion attempt after shutdown.
        harness.reactor.timeout()
        pending = harness.getDelayedCalls()
        self.assertEqual([c for c in pending if c.getTime() <= 10.0], [])
        self.assertIn(future, pending)
        self.assertEqual(shutdowns, ["shutdown"])
        # The due-but-never-dispatched call is gone and reports inactive,
        # with the same exception semantics as a cancellation.
        discarded = zeroDelayHolder["call"]
        self.assertNotIn(discarded, pending)
        self.assertFalse(discarded.active())
        self.assertRaises(error.AlreadyCancelled, discarded.cancel)

    def test_oneShotAndZeroDelayRunOnceAndReportActivity(self):
        """
        Ordinary one-shot calls (including zero-delay ones) run exactly
        once, report active beforehand/inactive afterwards, and keep their
        scheduled time.
        """
        harness = _DeterministicTimedCallReactor()
        events = []
        delayed = harness.callLater(3.0, events.append, "late")
        zero = harness.callLater(0.0, events.append, "now")
        self.assertTrue(delayed.active())
        self.assertTrue(zero.active())
        self.assertEqual(delayed.getTime(), 3.0)
        harness.advance(0.0)
        self.assertEqual(events, ["now"])
        self.assertFalse(zero.active())
        self.assertTrue(delayed.active())
        harness.advance(3.0)
        self.assertEqual(events, ["now", "late"])
        self.assertFalse(delayed.active())
        harness.advance(100.0)
        self.assertEqual(events, ["now", "late"])

    def test_repeatCancelOrResetKeepsExceptionSemantics(self):
        """
        Cancelling or resetting an already-called or already-cancelled call
        raises the established exceptions and never causes a repeat run.
        """
        harness = _DeterministicTimedCallReactor()
        events = []
        called = harness.callLater(10.0, events.append, "a")
        cancelled = harness.callLater(10.0, events.append, "b")
        cancelled.cancel()
        harness.advance(10.0)
        self.assertEqual(events, ["a"])
        self.assertRaises(error.AlreadyCalled, called.cancel)
        self.assertRaises(error.AlreadyCalled, called.reset, 1.0)
        self.assertRaises(error.AlreadyCancelled, cancelled.cancel)
        self.assertRaises(error.AlreadyCancelled, cancelled.reset, 1.0)
        harness.advance(100.0)
        self.assertEqual(events, ["a"])


class TestSpySignalCapturingReactor(ReactorBase):

    """
    Subclass of ReactorBase to capture signals delivered to the
    reactor for inspection.
    """

    def installWaker(self):
        """
        Required method, unused.
        """


@skipIf(not signal, "signal module not available")
class ReactorBaseSignalTests(TestCase):

    """
    Tests to exercise ReactorBase's signal exit reporting path.
    """

    def test_exitSignalDefaultsToNone(self):
        """
        The default value of the _exitSignal attribute is None.
        """
        reactor = TestSpySignalCapturingReactor()
        self.assertIs(None, reactor._exitSignal)

    def test_captureSIGINT(self):
        """
        ReactorBase's SIGINT handler saves the value of SIGINT to the
        _exitSignal attribute.
        """
        reactor = TestSpySignalCapturingReactor()
        reactor.sigInt(signal.SIGINT, None)
        self.assertEqual(signal.SIGINT, reactor._exitSignal)

    def test_captureSIGTERM(self):
        """
        ReactorBase's SIGTERM handler saves the value of SIGTERM to the
        _exitSignal attribute.
        """
        reactor = TestSpySignalCapturingReactor()
        reactor.sigTerm(signal.SIGTERM, None)
        self.assertEqual(signal.SIGTERM, reactor._exitSignal)

    def test_captureSIGBREAK(self):
        """
        ReactorBase's SIGBREAK handler saves the value of SIGBREAK to the
        _exitSignal attribute.
        """
        if not hasattr(signal, "SIGBREAK"):
            raise SkipTest("signal module does not have SIGBREAK")

        reactor = TestSpySignalCapturingReactor()
        reactor.sigBreak(signal.SIGBREAK, None)
        self.assertEqual(signal.SIGBREAK, reactor._exitSignal)
