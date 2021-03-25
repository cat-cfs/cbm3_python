import os
import glob


def iterate_svl_files(dir):
    for path in glob.glob(os.path.join(os.path.abspath(dir), "svl*")):
        yield path


def iterate_svl_lines(svl_file_path):

    dat_file = False
    if os.path.splitext(svl_file_path)[1].lower() == ".dat":
        dat_file = True

    with open(svl_file_path, 'r') as fp:
        if dat_file:
            for i_line, line in enumerate(fp):
                if i_line == 0:  # first line is not part of the data
                    continue
                else:
                    tokens = line.split()
                    # insert a null-value token to compensate for a field
                    # "years_since_land_use_change"
                    # found in ini format but not dat format
                    tokens.insert(5, "")
                    yield tokens
        else:
            line_tokens = []
            for i_line, line in enumerate(fp):
                if (i_line % 6) == 0:
                    if not i_line == 0:
                        yield line_tokens
                        line_tokens = []
                elif (i_line % 6) == 5:
                    tokens = line.split()
                    num_classifiers = len(tokens) - 6
                    needed_classifiers = 10 - num_classifiers
                    # need to insert null classifier values (-99)
                    # which appear in .dat but not in .ini format
                    line_tokens.extend(tokens[:num_classifiers])
                    line_tokens.extend([-99] * needed_classifiers)
                    line_tokens.extend(tokens[num_classifiers:])
                else:
                    line_tokens.extend(line.split())
            # dat file type
