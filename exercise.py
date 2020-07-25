'''
Code is written for Python 3
Emphasis have been placed on readability over performance
Performance can be improved by limiting hash lookups and replacing them with indexed lists
If performance is critical then parts or all of the code can be moved to Cython (less readable)

latin-1 encoding for read and writes is to improve performance
Python3 treats all text I/O as unicode by default, which leads to performance
overhead.  latin-1 cuts the overhead in half and is sufficient given the
inputs are only alphanumeric with no special charecters requiring unicode
'''

class Processor(object):

    def __init__(self, fin_path=None, fout_path=None):

        # file paths for input and output files
        self.fin = fin_path
        self.fout = fout_path
        # structured input
        # sets the number of inputs, their order and their labels
        # types are explicitly cast for safety i.e. ValueError on int fields that are not int (e.g. decimal values)
        # width sets the number of fields to expect; an assertion error happens if a row has too few or too many
        self.input = ['time','symbol','size','price']
        self.types = [int, str, int, int]
        self.width = len(self.input)
        # header determines the entries to be written to the output file and the order
        self.header = ['MaxTimeGap','TotalVolumeTraded','MaxTradePrice','VWAP']
        # entry holds output values and intermidiary values for computation
        # can be easily extended by adding additional entries
        # class can be easily extended with additional funcs for computation (see update_MaxTimeGap, calc_VWAP)
        self.entry = {i:0 for i in self.header}
        self.entry['prev_time'] = None
        self.entry['turnover'] = 0
        # init of data container
        self.data = {}

    def create_entry(self, symbol):
        # creates a new entry, by symbol, in the data container
        self.data.update({symbol: {i:j for i, j in self.entry.items()}})

    def parse_row(self, row):
        # parses a row from input data
        # checks that the row conforms to the structured input format and casts types
        r = row.split(',')
        assert len(r) == self.width
        return {self.input[i]: self.types[i](r[i]) for i in range(self.width)}

    def update_MaxTimeGap(self, symbol, time):
        # helper func to compute max time gap during file processing
        if self.data[symbol]['prev_time'] is not None:
            delta = time - self.data[symbol]['prev_time']
            if delta > self.data[symbol]['MaxTimeGap']:
                self.data[symbol]['MaxTimeGap'] = delta
        self.data[symbol]['prev_time'] = time

    def calc_VWAP(self):
        # helper func to compute VWAP after processing file
        for symb, entry in self.data.items():
            entry['VWAP'] = entry['turnover'] // entry['TotalVolumeTraded']

    def process_row(self, row):
        # CORE piece - processes each row by:
        # parsing row -> create entry if necessary -> update and compute inters and output 
        row = self.parse_row(row)
        symb = row['symbol']
        if symb not in self.data:
            self.create_entry(symb)
        # calls helper for computation; addition (extensions) helpers can be added here
        self.update_MaxTimeGap(symb, row['time'])
        # computes inters and output on a single pass basis
        # additional (extensions) computations should be added here
        self.data[symb]['TotalVolumeTraded'] += row['size']
        self.data[symb]['turnover'] += row['size'] * row['price']
        if row['price'] > self.data[symb]['MaxTradePrice']:
            self.data[symb]['MaxTradePrice'] = row['price']

    def get_line(self, symbol):
        # creates a structured output line for a given symbol
        entry = self.data[symbol]
        output = [symbol]
        output += [str(entry[i]) for i in self.header]
        return ','.join(output) + '\n'
    
    # executor
    def run(self):
        
        # open input file and conduct single pass processing of file
        with open(self.fin, 'rt', encoding='Latin-1') as f:
            for row in f:
                self.process_row(row)
        
        # calls all helpers to operate on the data container post read
        # all outputs that can only be calculated after reading the full input file should go here
        self.calc_VWAP()
        
        # keys sets the order of the output file by symbol
        keys = sorted(self.data.keys())
        
        # write structured output file
        with open(self.fout, 'wt', encoding='Latin-1') as f:
            for k in keys:
                # write structured output line to file
                f.write( self.get_line(k) )
