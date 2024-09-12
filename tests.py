import unittest
from NiceTab import NiceTab, NiceVec
import pyarrow as pa

class TestNiceVec(unittest.TestCase):
    def setUp(self):
        self.tab = NiceTab(pa.table({
                'c1': pa.array([1, 2, 3, 4, 5]),
                'c2': pa.array([2, 4, 6, 8, 10]),
                'c3': pa.array(['a', 'b', 'a', 'b', 'e']),
                'w': pa.array([1, 1,1, 2, 3])
                }))

    def test_sum(self):
        c4 = self.tab.c1 + self.tab.c2
        self.assertIsInstance(c4, NiceVec)
        self.tab.c4 = c4
        self.assertEqual(self.tab.Q("c4", aggregation="sum")["c4"][0], 5*3*3)

        self.tab["c4"] = c4 #check both assignment syntaxes
        self.assertEqual(self.tab.Q("c4", aggregation="sum").loc[0,"c4"], 5*3*3)
        
        self.tab.c12 = 12 + self.tab.c1
        self.assertEqual(self.tab.Q("c12", aggregation="sum").loc[0,"c12"], (12+3)*5)
        
        self.tab["c12"] = 12 + self.tab["c1"] #check both calling syntaxes
        self.assertEqual(self.tab.Q("c12", aggregation="sum").loc[0,"c12"], (12+3)*5)

    def test_replace(self):
        smaller = self.tab.c2.replace_if(self.tab.c1<4, 0)
        self.assertEqual(smaller.sum(), 18)

    def test_clip(self):
        self.tab["c1"] = self.tab.c1.clip(upper=3)
        self.assertEqual(self.tab.c1.sum(), 12)
        self.tab.neg = pa.array([-1, 2, -3, -4, -5]) #with no arguments, trim negatives.
        self.assertEqual(self.tab.neg.clip().sum(), 2)

    def test_weights(self):
        s = self.tab.Q(["c1", "c2"], group_by="c3")
        self.assertEqual(s['c1']['a'], 4)
        self.assertEqual(s['c2']['e'], 10)

        ws = self.tab.Q(["c1", "c2"], group_by="c3", weight='w')
        self.assertEqual(ws['c1']['e'], 15./8)
        self.assertEqual(ws['c2']['b'], 20./8)

    def test_simple_select(self):
        s = self.tab.Q(["c1", "c2"], where=self.tab.c3=='a')
        self.assertEqual(s['c1'][1], 3)
        self.assertEqual(s['c2'][0], 2)

    def test_cp(self):
        """A reminder that selecting makes copies, not pointers. Check vector element-picking. """
        cp = self.tab.select('c1')
        self.tab.c1 = self.tab.c2 * 2
        self.assertEqual(self.tab.c1[2], 12)
        self.assertEqual(cp['c1'][0], 1)
