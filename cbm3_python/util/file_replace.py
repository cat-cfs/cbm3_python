from tempfile import mkstemp
from shutil import move, copymode
from os import fdopen, remove

def replace(file_path, replace_func):
    """For every line in the specified file path the return
    value of replace_func(line_num, line) will be written to 
    a new file that will replace the original file.
    
    Modified version of answer from thomas-watnedal posted here:
    https://stackoverflow.com/questions/39086/search-and-replace-a-line-in-a-file-in-python
    
    Args:
        file_path (str): path to an existing writable file.
        replace_func (func): a function of (int, str) -> str called
            for each line in the specified file.
    """
    #Create temp file
    fh, abs_path = mkstemp()
    with fdopen(fh,'w') as new_file:
        with open(file_path) as old_file:
            for i, line in enumerate(old_file):
                new_file.write(replace_func(i, line))
    #Copy the file permissions from the old file to the new file
    copymode(file_path, abs_path)
    #Remove original file
    remove(file_path)
    #Move new file
    move(abs_path, file_path)