import unittest
import csv
import pandas as pd
import numpy as np
from customer_billing import extract_data_from_csv, transform_data, load_data_to_csv
#customer billing test class
class TestDataPipeline(unittest.TestCase):
    #set up test data
    def setUp(self):
        self.input_file_1 = "/home/fundi/moringaschool/week11/billing_data1.csv"
        self.input_file_2 = "/home/fundi/moringaschool/week11/billing_data2.csv"
        self.input_file_3 = "/home/fundi/moringaschool/week11/billing_data3.csv"
        self.output_file = "/home/fundi/moringaschool/week11/billing_data_output1.csv"

        #scenario1: valid input
        self.test_data1 = [{'customer_id': 1, 'billing_amount': '$100', 'tax_amount': 10}, 
                          {'customer_id': 2, 'billing_amount': '$200', 'tax_amount': 20}
        ]                 
        #scenario2: missing values
        self.test_data2 = [{'customer_id': None, 'billing_amount': '$100', 'tax_amount': 10}, 
                          {'customer_id': 2, 'billing_amount': None, 'tax_amount': 20},
                          {'customer_id': 3, 'billing_amount': "$300", 'tax_amount': 30}
        ]
        #scenatio2: duplicates
        self.test_data3 = [{'customer_id': 1, 'billing_amount': '$100', 'tax_amount': 10}, 
                          {'customer_id': 1, 'billing_amount': '$100', 'tax_amount': 10},
                          {'customer_id': 2, 'billing_amount': '$200', 'tax_amount': 20}
                          ]
        #transformed data to load to csv
        self.test_data_output = pd.DataFrame({'customer_id':[1,2], 'billing_amount':[100.0, 200.0], 
                                      'tax_amount':[10.0, 20.0], 'total_charge':[110.0, 220.0]})
    
    def _write_test_data_to_csv(self, file_path, data):
        field_names = ['customer_id', 'billing_amount', 'tax_amount']
        with open(file_path, 'w', newline='' ) as file:
            writer = csv.DictWriter(file, field_names)
            writer.writeheader()
            writer.writerows(data)

    def test_extracting_valid_data(self):
        #test extracting data from csv file with valid data
        #prepare test data
        self._write_test_data_to_csv(self.input_file_1, self.test_data1)
        expected_data = pd.DataFrame(self.test_data1)
        #test extracting function
        extracted_data = extract_data_from_csv(self.input_file_1)
        #assert extracted data
        self.assertTrue(extracted_data.equals(expected_data)) 
        
    def test_extracting_data_with_missing_value(self):
        #test extracting data from csv file
        #prepare test data
        self._write_test_data_to_csv(self.input_file_2, self.test_data2)
        #expected data, csv.dictreader read missing values as empty string
        expected_data = pd.DataFrame(self.test_data2)
        #test extracting function
        extracted_data = extract_data_from_csv(self.input_file_2)
        #assert extracted data
        self.assertTrue(extracted_data.equals(expected_data)) 
    
    def test_extracting_duplicated_data(self):
        #prepare the test data
        self._write_test_data_to_csv(self.input_file_3, self.test_data3)
        expected_data = pd.DataFrame(self.test_data3)
        #read the data
        extracted_data = extract_data_from_csv(self.input_file_3)        
        self.assertTrue(extracted_data.equals(expected_data))

    def test_transforming_valid_data(self):
        #test transforming data with no missing value, duplicates
        #test data
        test_data = pd.DataFrame(self.test_data1)
        expected_data = pd.DataFrame({'customer_id':[1,2], 'billing_amount':[100.0, 200.0], 
                                      'tax_amount':[10.0,20.0], 'total_charge':[110.0, 220.0]})
        transformed_data = transform_data(test_data)
        self.assertTrue(transformed_data.equals(expected_data))

    def test_transforming_data_with_missing_values(self):
        #test transforming data with missing value, 
        test_data = pd.DataFrame(self.test_data2)
        expected_data = pd.DataFrame({'customer_id':[3], 'billing_amount':[300.0], 
                                      'tax_amount':[30.0], 'total_charge':[330.0]})
        transformed_data = transform_data(test_data)
        self.assertTrue(transformed_data.equals(expected_data))
    
    def test_transforming_data_with_duplicates(self):
        #test for handling duplicates values
        test_data = pd.DataFrame(self.test_data3)
        expected_data = pd.DataFrame({'customer_id':[1,2], 'billing_amount':[100.0, 200.0], 
                                      'tax_amount':[10.0, 20.0], 'total_charge':[110.0, 220.0]})
        transformed_data = transform_data(test_data)
        self.assertTrue(transformed_data.equals(expected_data))        
    
    def test_loading_data_to_csv(self):
        #load the tranformed data to csv
        #test data
        expected_data = self.test_data_output
        #load data to csv
        load_data_to_csv(self.test_data_output, self.output_file)
        #loaded data
        loaded_data = pd.read_csv(self.output_file)
        self.assertTrue(loaded_data.equals(expected_data))

        
        

if __name__ == "__main__":
    unittest.main()