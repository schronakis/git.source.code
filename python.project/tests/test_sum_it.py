import unittest
from fractions import Fraction

from sum_it.sum_it import SumIt

class TestSum(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print("\nsetUpClass method: Runs before all tests...")

    def setUp(self):
        print("\nRunning setUp method...")
        self.test_list_integers = SumIt([1, 2, 3])
        self.test_list_fractions = SumIt([Fraction(1, 4), Fraction(1, 4), Fraction(2, 5)])

    def tearDown(self):
        print("Running tearDown method...")

    def test_list_int(self):
        """
        Test that it can sum a list of integers
        """
        result = self.test_list_integers.sum()
        self.assertEqual(result, 6)
        

    def test_list_fraction(self):
        """
        Test that it can sum a list of fractions
        """
        result = self.test_list_fractions.sum()
        self.assertEqual(result, 1)
        
    @classmethod
    def tearDownClass(cls):
    	print("\ntearDownClass method: Runs after all tests...")

if __name__ == '__main__':
    unittest.main()