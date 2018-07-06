#This script's purpose is to mine the CBM-CFS3 report.fil for it's "Disturbance Reconciliation" output
#It creates a delimited text file (default delimiter is comma)
import re #regular expressions
import sys

def filterRow(row, column):
    #check if the row is a harvest default disturbance type
    if row[column['Default Disturbance Type']] in [ '4', '185', '195', '196' ] :
        return True
        
def run(inputFile, outputFile, delimiter=','):
    fileContents = inputFile.read() #read entire file into memory... it's the only way to use regex this way apparently
    # match on the blocks of disturbance reconciliation... please see http://docs.python.org/library/re.html
    matches = re.findall('Disturbance Reconciliation.+?Records Changed:[ \t0-9]+', fileContents, re.DOTALL) 
    # the 'Disturbance Reconciliation' part matches the first bit of text in each block
    # '.+?' matches any character in a non greedy fashion
    # 'Records Changed:[ \t0-9]+' matches the end of each block

    #variable to hold the column names and indexes
    columns = {}

    for match in matches: #loop 1, create the collection of names in the Disturbance Reconciliation blocks
        for innerMatch in re.split('[\r\n]', match):
            if 'Reconciliation' in innerMatch : continue
            kvp = re.split('    |[:]',innerMatch)
            kvp = filter(None, kvp) #remove the empty entries in the split
            key = kvp[0].strip() #remove leading and trailing whitespace
            if not key in columns :
                outputFile.write(key + delimiter) #write the column names
                columns[key] = len(columns)

    outputFile.write('\n')

    for match in matches: #loop 2, write out all of the values with columns enumerated in loop 1
        row = []
        for col in columns :
            row.append('')
        for innerMatch in re.split('[\r\n]', match):
            if 'Reconciliation' in innerMatch : continue
            kvp = re.split('    |[:]',innerMatch)
            kvp = filter(None, kvp)
            key = kvp[0].strip()
            value = kvp[1].strip()
            index = columns[key]
            row[index] = value
        if filterRow(row, columns) :
            for item in row :
                outputFile.write(item + delimiter)
            outputFile.write('\n')

if __name__ == "__main__":

    with open(sys.argv[1], 'r') as inputFile:
        with open(sys.argv[2], 'w') as outputFile:
            run(inputFile, outputFile)
    