import unittest

from timestamp import Timestamp


class InitTest(unittest.TestCase):
    def test_default_init(self):
        timestamp = Timestamp()

        self.assertEqual(timestamp.replicas, {})

    def test_replica_init(self):
        replicas = {"id": 0}

        timestamp = Timestamp(replicas)

        self.assertEqual(timestamp.replicas, replicas)


class LessThanEqualsTest(unittest.TestCase):
    def test_1(self):
        t1 = Timestamp()
        t2 = Timestamp()

        self.assertTrue(t1 <= t2)

    def test_2(self):
        t1 = Timestamp({"id": 0})
        t2 = Timestamp({"id": 0})

        self.assertTrue(t1 <= t2)

    def test_3(self):
        t1 = Timestamp({"id": 1})
        t2 = Timestamp({"id": 1})

        self.assertTrue(t1 <= t2)

    def test_4(self):
        t1 = Timestamp({"id": 0})
        t2 = Timestamp({"id": 1})

        self.assertTrue(t1 <= t2)

    def test_5(self):
        t1 = Timestamp({"id": 0})
        t2 = Timestamp()

        self.assertTrue(t1 <= t2)

    def test_6(self):
        t1 = Timestamp()
        t2 = Timestamp({"id": 0})

        self.assertTrue(t1 <= t2)

    def test_7(self):
        t1 = Timestamp()
        t2 = Timestamp({"id": 0})

        self.assertTrue(t1 <= t2)

    def test_8(self):
        t1 = Timestamp()
        t2 = Timestamp({"id": 2})

        self.assertTrue(t1 <= t2)


class MergeTest(unittest.TestCase):
    def test_1(self):
        t1 = Timestamp({"id": 0})
        t2 = Timestamp({"id": 1})

        t1.merge(t2)

        self.assertEqual(t1.replicas["id"], 1)

    def test_2(self):
        t1 = Timestamp()
        t2 = Timestamp({"id": 1})

        t1.merge(t2)

        self.assertEqual(t1.replicas["id"], 1)

    def test_3(self):
        t1 = Timestamp({"id": 0})
        t2 = Timestamp({"id": 1})

        t2.merge(t1)

        self.assertNotEqual(t2.replicas["id"], 0)


class CompareTest(unittest.TestCase):
    def test_1(self):
        t1 = Timestamp({"id": 0})
        t2 = Timestamp({"id": 1})

        l = list(t1.compare(t2))

        self.assertEqual(l[0], "id")


def run():
    test_cases = [InitTest, LessThanEqualsTest, MergeTest, CompareTest]
    all_tests = unittest.TestSuite()

    for case in test_cases:
        all_tests.addTest(unittest.TestLoader().loadTestsFromTestCase(case))

    unittest.TextTestRunner(verbosity=2).run(all_tests)
