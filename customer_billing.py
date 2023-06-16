import pandas as pd

import csv

input_file = '/home/fundi/moringaschool/week11/billing_data2.csv'
output_file = '/home/fundi/moringaschool/week11/billing_data_output.csv'

def extract_data_from_csv(input_file):
    #load the csv file
    data = pd.read_csv(input_file)
    return data

def transform_data(data:pd.DataFrame):
    #transform:drop rows with missing value, drop duplicates, remove $ from amount and calc total charge
    #remove missing values
    data = data.dropna(axis=0)
    #drop duplicates
    data = data.drop_duplicates()
    #convert customer_id to int type and tax amount as float type
    data['customer_id'] = data['customer_id'].astype(int)
    data['tax_amount'] = data['tax_amount'].astype(float)
    #remove the dollar sign $ from billing amount
    data['billing_amount'] = data['billing_amount'].apply(lambda x:float(x.replace("$", "")))
    #calculate total charge
    data['total_charge'] = data['billing_amount'] + data['tax_amount']
    #reset the index
    data = data.reset_index(drop=True)
    
    return data

def load_data_to_csv(data:pd.DataFrame, output_file):
    #save the data to a csv file
    #convert the data frame to csv
    csv_data = data.to_csv(output_file, index=False)
    

# t = extract_data_from_csv(input_file)
# print('input data',t, "t, types", t.dtypes)
# td = transform_data(t)
# print('after transformation', td, 'tranformed dtypes', td.dtypes)
# load_data_to_csv(t)

    
