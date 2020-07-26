import unittest
from exercise import Processor
import os

src = os.path.dirname(__file__)
fin = os.path.join(src, 'input.csv')
fout = os.path.join(src, 'output.csv')
goldensrc = os.path.join(src, 'golden.csv')
p = Processor(fin, fout)

class TestsCreateEntry(unittest.TestCase):
    
    def test_keys(self):
        symbols = ['a', 'b', 'c']
        output = {i:None for i in symbols}
        p.data = {}
        for i in symbols:
            p.create_entry(i)
        assert ( output.keys() ^ p.data.keys() ) == set()

    def test_construction(self):
        symbol = 'test'
        p.data = {}
        p.create_entry(symbol)
        assert ( p.data[symbol].items() ^ p.entry.items() ) == set()


class TestParseRow(unittest.TestCase):
    
    def test_parse(self):
        with open(p.fin, 'rt', encoding='Latin-1') as f:
            for row in f:
                break
        r = row.split(',')
        output = {p.input[i]: p.types[i](r[i]) for i in range(p.width)}
        result = p.parse_row(row)
        assert ( output.items() ^ result.items() ) == set()

class TestGoldenSource(unittest.TestCase):

    def test_output(self):

        p.data = {}
        p.run()
        
        with open(goldensrc, 'rb') as f:
            golden = f.read()

        with open(fout, 'rb') as f:
            result = f.read()

        assert golden == result


if __name__ == '__main__':
    unittest.main()


