from timestamp import Timestamp
import unittest


class InitTest(unittest.TestCase):

    def test_default_init(self):

        timestamp = Timestamp()

        self.assertEqual(timestamp.replicas, {})

    def test_replica_init(self):

        replicas = {"id": 0}

        timestamp = Timestamp(replicas)

        self.assertEqual(timestamp.replicas, replicas)


class LessThanTest(unittest.TestCase):

    def test_1(self):

        t1 = Timestamp({"id": 0})
        t2 = Timestamp({"id": 1})

        self.assertTrue(t1 < t2)

    def test_2(self):

        t1 = Timestamp({"id": 1})
        t2 = Timestamp({"id": 0})

        self.assertFalse(t1 < t2)

    def test_id_not_in_ts(self):

        t1 = Timestamp({"id": 1})
        t2 = Timestamp()

        self.assertFalse(t1 < t2)

    def test_id_not_in_ts_but_equal(self):

        t1 = Timestamp({"id": 0})
        t2 = Timestamp()

        self.assertTrue(t1 < t2)

    def test_id_in_other(self):

        t1 = Timestamp()
        t2 = Timestamp({"id": 0})

        self.assertTrue(t1 < t2)

    def test_id_in_other_but_greater(self):

        t1 = Timestamp()
        t2 = Timestamp({"id": 1})

        self.assertFalse(t1 < t2)


class EqualsTest(unittest.TestCase):

    def test_equals_true_1(self):

        t1 = Timestamp({"id": 0})
        t2 = Timestamp({"id": 0})

        self.assertTrue(t1 == t2)

    def test_equals_true_2(self):

        t1 = Timestamp({"id": 1})
        t2 = Timestamp({"id": 1})

        self.assertTrue(t1 == t2)

    def test_equals_true_3(self):

        t1 = Timestamp({"id": 0})
        t2 = Timestamp()

        self.assertTrue(t1 == t2)

    def test_equals_true_4(self):

        t1 = Timestamp()
        t2 = Timestamp({"id": 0})

        self.assertTrue(t1 == t2)

    def test_equals_false_1(self):

        t1 = Timestamp({"id": 0})
        t2 = Timestamp({"id": 1})

        self.assertFalse(t1 == t2)

    def test_equals_false_2(self):

        t1 = Timestamp({"id": 1})
        t2 = Timestamp({"id": 0})

        self.assertFalse(t1 == t2)

    def test_equals_false_3(self):

        t1 = Timestamp({"id": 1, "id_2": 0})
        t2 = Timestamp({"id": 0, "id_2": 1})

        self.assertFalse(t1 == t2)


class MergeTest(unittest.TestCase):

    def test_merge_true(self):

        t1 = Timestamp({"id": 0})
        t2 = Timestamp({"id": 1})

        t1.merge(t2)

        self.assertTrue(t1 == t2)

    def test_merge_false(self):

        t1 = Timestamp({"id": 0})
        t2 = Timestamp({"id": 1})

        t2.merge(t1)

        self.assertFalse(t1 == t2)


def run():

    test_cases = [InitTest, LessThanTest, EqualsTest, MergeTest]
    all_tests = unittest.TestSuite()

    for case in test_cases:

        all_tests.addTest(unittest.TestLoader().loadTestsFromTestCase(case))

    unittest.TextTestRunner(verbosity=2).run(all_tests)




