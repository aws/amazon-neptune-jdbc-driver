import csv
import argparse
import sys
import json
import collections
from collections import OrderedDict
import codecs
from dateutil.parser import parse
import getopt, sys

def is_date(string):
    try:
        parse(string, fuzzy=False)
        return True
    except ValueError:
        return False

def isfloat(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def print_help():
    print ("Usage:")
    print ("\tpython3 ./tdvt_csv_to_neptune.py -i <input_tdvt_file_path> -o <output_json_file_path>")
    print ("\tpython3 ./tdvt_csv_to_neptune.py --input <input_tdvt_file_path> --output <output_json_file_path>")

def csv_to_json(csvFilePath, jsonFilePath):
    with open(csvFilePath) as csvf, open(jsonFilePath, 'w') as jsonf:
            rowCount = 0
            csvReader = csv.DictReader(csvf)
            for row in csvReader: 
                rowCount += 1
                for key in row.keys():
                    column = row[key]
                    if column is None:
                        row[key] = None
                    elif column == "":
                        row[key] = None
                    elif key.startswith("bool"):
                        row[key] = True if column == "1" else False
                    elif column.isdigit():
                        row[key] = int(column)
                    elif isfloat(column):
                        row[key] = float(column)
                    elif is_date(column):
                        row[key] = parse(column, fuzzy=False).isoformat()
                    else:
                        row[key] = row[key]
                jsonf.write(json.dumps(row) + "\n")
    print("Row count: " + str(rowCount))

def main():
    argumentList = sys.argv[1:]
    options = "h:i:o:"
    long_options = ["help", "input", "output"]
    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
        input_file = None
        output_file = None
        print("arguments: " + str(arguments))
        for currentArgument, currentValue in arguments:
            if currentArgument in ("-h", "--Help"):
                print_help()
            elif currentArgument in ("-i", "--input"):
                print ("Input file: " + str(currentValue))
                input_file = currentValue
            elif currentArgument in ("-o", "--output"):
                print ("Output file: " + str(currentValue))
                output_file = currentValue
        if input_file == None or output_file == None:
            print("Error, input file or output file was None. Please refer to usage below.")
            print_help()
            return
    except getopt.error as err:
        # output error, and return with an error code
        print(str(err))
        print_help()
        return
    csv_to_json(input_file, output_file)

if __name__ == "__main__":
    print("IF running calcs, make sure datetime1 values are converted from \"\" to \"1900-01-01 00:00:00\".")
    main()