import csv
import os 
import json 

data_path = '../data/raw_data.csv'

file = open(data_path)

csvreader = csv.reader(file, delimiter=';')
header = []
header = next(csvreader) #to pass the header and start from the values

def generate_message() -> dict:
    header = next(csvreader)

    # print(header)
    current_message = header[4]
    current_message = json.loads(current_message)

    return current_message

# #Testing 
# if __name__ == '__main__':
#     print(generate_message())

