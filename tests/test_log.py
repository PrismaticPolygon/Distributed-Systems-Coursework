import unittest

from enums import Operation
from replica_classes import Log, Record
from requests import Timestamp, ClientRequest


class InTest(unittest.TestCase):

    def setUp(self):

        self.record = Record("0", Timestamp(), ClientRequest(Operation.READ, {}), Timestamp(), "id")

    def test_in_true(self):

        log = Log()

        log += self.record

        # None type? Do I have to return the object?

        self.assertTrue("id" in log)

    def test_in_false(self):

        log = Log()

        self.assertFalse("id" in log)


class MergeTest(unittest.TestCase):

    def setUp(self):

        self.r1 = Record("1", Timestamp({"id": 0}), ClientRequest(Operation.READ, {}), Timestamp(), "id_1")
        self.r2 = Record("1", Timestamp({"id": 2}), ClientRequest(Operation.READ, {}), Timestamp(), "id_2")

        self.replica_ts = Timestamp({"id": 1})

    def test_merge_true(self):

        log_1 = Log()
        log_2 = Log([self.r1])

        log_1.merge(log_2, self.replica_ts)

        self.assertFalse("id_1" in log_1)

    def test_merge_false(self):

        log_1 = Log()
        log_2 = Log([self.r2])

        log_1.merge(log_2, self.replica_ts)

        self.assertTrue("id_2" in log_1)


class StableTest(unittest.TestCase):

    def setUp(self):

        self.r1 = Record("1", Timestamp({"1": 1, "2": 0, "3": 0}), ClientRequest(Operation.READ, {}), Timestamp(), "id_1")
        self.r2 = Record("2", Timestamp({"1": 0, "2": 0, "3": 0}), ClientRequest(Operation.READ, {}), Timestamp(), "id_2")
        self.r3 = Record("3", Timestamp({"1": 0, "2": 0, "3": 1}), ClientRequest(Operation.READ, {}), Timestamp(), "id_3")

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

        self.assertTrue(self.r2 in stable)

    def test_stable_4(self):

        stable = self.log.stable(Timestamp({"3": 4}))

        self.assertFalse(self.r1 in stable)
        self.assertTrue(self.r2 in stable)
        self.assertTrue(self.r3 in stable)

    def test_stable_5(self):

        stable = self.log.stable(Timestamp({"1": 2, "3": 4}))

        self.assertTrue(stable[0] is self.r2)
        self.assertTrue(stable[1] is self.r3)
        self.assertTrue(stable[2] is self.r1)


def run():

    test_cases = [InTest, MergeTest, StableTest]
    all_tests = unittest.TestSuite()

    for case in test_cases:
        all_tests.addTest(unittest.TestLoader().loadTestsFromTestCase(case))

    unittest.TextTestRunner(verbosity=2).run(all_tests)


run()
