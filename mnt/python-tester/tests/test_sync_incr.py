import pytest
import pandas as pd
   

class TestDataTransfer:

    def test_result_equal(self):
        df = pd.DataFrame({1: [10], 2: [20]})
        exactly_equal = pd.DataFrame({1: [10], 2: [20]})
        assert True == df.equals(exactly_equal)
        
    def test_deu_ruim(self):
        p = 3
        print(p)
        assert 1 == 0