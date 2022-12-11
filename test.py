import unittest
import pandas as pd
from parameterized import parameterized
import nose2


colum_names = ['Destination.IP', 'Timestamp', 'Flow.Duration', 'Flow.Bytes.s', 'Average.Packet.Size', 'ProtocolName']

def bytes_transfered(flow_duration, flow_bytes):
    bytes_transfered = flow_duration * flow_bytes
    return bytes_transfered

def number_packets(bytes_transfered, packet_size):
    number_packets = bytes_transfered /  packet_size
    return number_packets

class DfTests(unittest.TestCase):
    def setUp(self):
        try:
            data = pd.read_csv("Dataset-Unicauca-Version2-87Atts.csv", 
            usecols=['Destination.IP', 'Timestamp', 'Flow.Duration', 'Flow.Bytes.s', 'Average.Packet.Size', 'ProtocolName'])
            self.fixture = data
        except IOError as e:
            print(e)

    # testing colum names    
    def test_colnames(self):
        self.assertListEqual(list(self.fixture.columns), colum_names)

    # testing timestamp format
    def test_timestamp_format(self):
        ts = self.fixture["Timestamp"]
        [self.assertRegex(i, "\d{2}/\d{2}/\d{6}:\d{2}:\d{2}") for i in ts]


# test cases for bytes_transfered function
def test_cases_1():
    """ Create test cases in the format of a dataframe. """
    df = pd.DataFrame.from_dict({
         'test_1': ['negative_int_test' , -2    , 2     , -4                 , None              ], 
         'test_2': ['positive_int_test' , 2     , 2     , 4                 , None              ],
         'test_3': ['decimal_test'      , .5    , .4    , 0.2               , None              ],
         'test_4': ['none_type_test'    , None  , 2     , None              , TypeError         ],
         'test_5': ['string_type_test'  , '10'  , 1     , None              , TypeError         ],
         },
        orient='index'
    )

    df.columns = ['name','a','b','expected_output', 'expected_error']
    # return dataframe as a list of tuples.
    return list(df.itertuples(index=False, name=None)) 

# test cases for number_packets function
def test_cases_2():
    """ Create test cases in the format of a dataframe. """
    df = pd.DataFrame.from_dict({
         'test_1': ['negative_int_test' , -2    , 2     , -1                 , None              ], 
         'test_2': ['positive_int_test' , 2     , 2     , 1                 , None              ],
         'test_3': ['decimal_test'      , .5    , .4    , 1.25               , None              ],
         'test_4': ['none_type_test'    , None  , 2     , None              , TypeError         ],
         'test_5': ['string_type_test'  , '10'  , 1     , None              , TypeError         ],
         'test_6': ['zero_division_test', 2     , 0     , None              , ZeroDivisionError ]
         },
        orient='index'
    )

    df.columns = ['name','a','b','expected_output', 'expected_error']

    # return dataframe as a list of tuples.
    return list(df.itertuples(index=False, name=None)) 



class TestSuite(unittest.TestCase):
    @parameterized.expand(
    test_cases_1()
    )
    # testing bytes_transfered function
    def test_bytes_transfered(self, name, a, b, expected_output, expected_error=None):
        if expected_error is None:
            assert bytes_transfered(a, b) == expected_output
        
        else:
            with self.assertRaises(expected_error):
                bytes_transfered(a, b)
    
    @parameterized.expand(
        test_cases_2()
     )
    # testing number_packets function
    def test_number_packets(self, name, a, b, expected_output, expected_error=None):
        if expected_error is None:
            assert number_packets(a, b) == expected_output
        
        else:
            with self.assertRaises(expected_error):
                number_packets(a, b)
        

if __name__ == '__main__':
    nose2.main()