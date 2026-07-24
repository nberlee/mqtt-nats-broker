#!/usr/bin/env python3
"""Driver for the Eclipse Paho MQTT 5.0 interoperability suite (client_test5.py).

The upstream suite is written to run via its own ``__main__`` (which parses CLI
flags into ``unittest.main`` and sets ``host``/``port`` as module globals). Its
``setData()`` resets the topic globals but never assigns ``host``/``port``, so
importing the module and running the ``Test`` class directly requires setting
those globals ourselves. See docs/CONFORMANCE.md.

Usage:
  paho_driver.py <interop_dir> <host> <port> --list        list test methods
  paho_driver.py <interop_dir> <host> <port> <test_name>   run one test method

Running one test per process lets the caller wrap each in a ``timeout``: several
suite tests (e.g. test_payload_format, test_user_properties) busy-wait for their
expected messages with no timeout and would otherwise hang the whole run.

Exit status: 0 if the single test passed, 1 otherwise (failure, error, or an
unknown test name). ``--list`` always exits 0.
"""
import sys
import unittest


def _load(interop_dir, host, port):
    sys.path.insert(0, interop_dir)
    import client_test5  # noqa: E402  (path set above)

    # setData() declares host/port global but does not assign them; set them here
    # so they survive into setUpClass. topic_prefix is likewise only assigned in
    # the suite's own __main__ (not setData), yet test_shared_subscriptions reads
    # it -- set it to the same value __main__ uses so that test can run.
    client_test5.host = host
    client_test5.port = port
    client_test5.topic_prefix = "client_test5/"
    return client_test5


def main(argv):
    if len(argv) < 4:
        print(__doc__, file=sys.stderr)
        return 2
    interop_dir, host, port, what = argv[0], argv[1], int(argv[2]), argv[3]
    mod = _load(interop_dir, host, port)

    names = sorted(
        n for n in dir(mod.Test) if n.startswith("test_") and callable(getattr(mod.Test, n))
    )
    if what == "--list":
        for n in names:
            print(n)
        return 0

    if what not in names:
        print("unknown test: %s" % what, file=sys.stderr)
        return 1

    suite = unittest.TestLoader().loadTestsFromNames([what], mod.Test)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
