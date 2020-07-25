'''
Emphasis have been placed on readability over performance
Performance can be improved by limited hash lookups and replaced with indexed lists
If performance is critical then parts or all of the code can be moved to Cython
'''



class Processor(object):

    def __init__(self, fin_path=None, fout_path=None):

        self.fin = fin_path
        self.fout = fout_path
        self.header = ['MaxTimeGap','TotalVolumeTraded','MaxTradePrice','VWAP']
        self.input = ['time','symbol','size','price']
        self.types = [int, str, int, int]
        self.width = len(self.input)
        self.entry = {i:0 for i in self.header}
        self.entry['prev_time'] = None
        self.entry['turnover'] = 0
        self.data = {}

    def create_entry(self, symbol):
        self.data.update({symbol: {i:j for i, j in self.entry.items()}})

    def parse_row(self, row):
        r = row.split(',')
        assert len(r) == self.width
        return {self.input[i]: self.types[i](r[i]) for i in range(self.width)}

    def update_MaxTimeGap(self, symbol, time):
        if self.data[symbol]['prev_time'] is not None:
            delta = time - self.data[symbol]['prev_time']
            if delta > self.data[symbol]['MaxTimeGap']:
                self.data[symbol]['MaxTimeGap'] = delta
        self.data[symbol]['prev_time'] = time

    def calc_VWAP(self):
        for symb, entry in self.data.items():
            entry['VWAP'] = entry['turnover'] // entry['TotalVolumeTraded']

    def process_row(self, row):
        row = self.parse_row(row)
        symb = row['symbol']
        if symb not in self.data:
            self.create_entry(symb)
        self.update_MaxTimeGap(symb, row['time'])
        self.data[symb]['TotalVolumeTraded'] += row['size']
        self.data[symb]['turnover'] += row['size'] * row['price']
        if row['price'] > self.data[symb]['MaxTradePrice']:
            self.data[symb]['MaxTradePrice'] = row['price']

    def get_line(self, symbol):
        entry = self.data[symbol]
        output = [symbol]
        output += [str(entry[i]) for i in self.header]
        # output['VWAP'] = entry['turnover'] // entry['TotalVolumeTraded']
        return ','.join(output) + '\n'
    
    def run(self):
        
        with open(self.fin, 'rt', encoding='Latin-1') as f:
            for row in f:
                self.process_row(row)

        keys = sorted(self.data.keys())
        self.calc_VWAP()

        with open(self.fout, 'wt', encoding='Latin-1') as f:
            for k in keys:
                f.write( self.get_line(k) )
