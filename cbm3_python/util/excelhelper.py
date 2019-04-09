# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import xlrd

def load_dict_from_worksheet(sheet):
    # read header values into the list
    keys = [sheet.cell(0, col_index).value for col_index in xrange(sheet.ncols)]

    dict_list = []
    for row_index in xrange(1, sheet.nrows):
        d = {keys[col_index]: sheet.cell(row_index, col_index).value
             for col_index in xrange(sheet.ncols)}
        dict_list.append(d)
    return dict_list

def get_worksheet_as_dict(excelPath, worksheetIndex):
    wb = xlrd.open_workbook(excelPath)
    ws = wb.sheet_by_index(worksheetIndex)
    return load_dict_from_worksheet(ws)