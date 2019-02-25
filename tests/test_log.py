from replica_classes import Log, Record
from requests import Timestamp, ClientRequest
from enums import Method
import unittest


class InTest(unittest.TestCase):

    def setUp(self):

        self.record = Record("0", Timestamp(), ClientRequest(Method.READ, {}), Timestamp(), "id")

    def test_in_true(self):

        log = Log()

        log.add(self.record)

        self.assertTrue(self.record in log)

    def test_in_false(self):

        log = Log()

        self.assertFalse(self.record in log)


class MergeTest(unittest.TestCase):

    def setUp(self):

        self.record = Record("0", Timestamp(), ClientRequest(Method.READ, {}), Timestamp(), "id")
        self.replica_ts = Timestamp({"id": 1})

    def test_merge_true(self):

        r1 = Record("1", Timestamp({"id": 0}), ClientRequest(Method.READ, {}), Timestamp(), "id")
        r2 = Record("1", Timestamp({"id": 0}), ClientRequest(Method.READ, {}), Timestamp(), "id")

        log_1 = Log()
        log_2 = Log([r1])

        log_1.merge(log_2, self.replica_ts)

        self.assertTrue(r2 in log_1)

    def test_merge_false(self):

        r1 = Record("1", Timestamp({"id": 2}), ClientRequest(Method.READ, {}), Timestamp(), "id")

        log_1 = Log()
        log_2 = Log([r1])

        log_1.merge(log_2, self.replica_ts)

        self.assertFalse(r1 in log_1)


class StableTest(unittest.TestCase):

    # Stable updates are FINE.
    # It will be hard to invent some genuine data for this methinks.
    # Of course it always passes: the ids NEVER match

    def setUp(self):

        self.r1 = Record("1", Timestamp({"1": 1, "2": 0, "3": 0}), ClientRequest(Method.READ, {}), Timestamp(), "id_1")
        self.r2 = Record("2", Timestamp({"1": 0, "2": 0, "3": 0}), ClientRequest(Method.READ, {}), Timestamp(), "id_2")
        self.r3 = Record("3", Timestamp({"1": 0, "2": 0, "3": 1}), ClientRequest(Method.READ, {}), Timestamp(), "id_3")

        self.log = Log([self.r3, self.r2, self.r1])

    def test_stable_1(self):

        stable = self.log.stable(Timestamp({"1": 0}))

        self.assertFalse(self.r1 in stable)
        self.assertTrue(self.r2 in stable)

    def test_stable_2(self):

        stable = self.log.stable(Timestamp({"1": 1}))

        self.assertTrue(self.r1 in stable)

    def test_stable_3(self):

        stable = self.log.stable(Timestamp({"1": 1}))

        self.assertFalse(self.r2 in stable)

    def test_stable_4(self):

        stable = self.log.stable(Timestamp({"3": 4}))

        self.assertTrue(self.r1 in stable)
        self.assertTrue(self.r2 in stable)
        self.assertTrue(self.r3 in stable)

    def test_stable_5(self):

        stable = self.log.stable(Timestamp({"3": 4}))

        self.assertTrue(stable[0] is self.r1)
        self.assertTrue(stable[1] is self.r2)
        self.assertTrue(stable[2] is self.r3)


def run():

    test_cases = [InTest, MergeTest, StableTest]
    all_tests = unittest.TestSuite()

    for case in test_cases:
        all_tests.addTest(unittest.TestLoader().loadTestsFromTestCase(case))

    unittest.TextTestRunner(verbosity=2).run(all_tests)


run()
