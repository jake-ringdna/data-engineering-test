#!/usr/bin/env python3

import csv

input_filename = "data/data.tsv"
output_filename = "data/cleansed.tsv"


# validate row
def is_valid_row(test_row):
    # check # of columns
    if len(test_row) != 5:
        return False
    # sanity check last column (email) contains '@'
    if '@' not in test_row[4]:
        return False
    # ensure first column (id) is an int
    try:
        int(test_row[0])
        return True
    except ValueError:
        return False


# attempt to cleanse bad rows
def fix_bad_row(bad_row):
    # strip spaces and remove blank columns
    candidate_row = [bad_row[0]] + \
        [x.strip() for x in bad_row[1:] if x.strip() != '']
    if is_valid_row(candidate_row):
        return candidate_row

    # check for account_number not starting with a digit
    # if not, assume 3 name fields
    if not candidate_row[3][0].isdigit():

        # 2nd and 3rd names the same: delete duplicate
        if candidate_row[2] == candidate_row[3]:
            del candidate_row[3]

        # otherwise join 2nd and 3rd names together
        else:
            candidate_row[2] = " ".join(candidate_row[2:4])
            del candidate_row[3]

        # check for validity now
        if is_valid_row(candidate_row):
            return candidate_row
        # unaccounted for case
        else:
            return ['-1'] + bad_row

    # unaccounted for case
    else:
        return ['-1'] + bad_row


with open(input_filename, encoding='utf-16-le', newline='') as inputf, \
        open(output_filename, 'w', encoding='utf-8', newline='') as outputf:
    reader = csv.reader(inputf, delimiter='\t', strict=True)
    writer = csv.writer(outputf, delimiter='\t', quotechar='"',
                        lineterminator='\n')

    # get and write out the header row
    header_row = next(reader)
    writer.writerow(header_row)

    prev_row = []
    for row in reader:
        if is_valid_row(row):
            # write out id as is, with spaces stripped from other columns
            writer.writerow([row[0]] + [x.strip() for x in row[1:]])

        # process invalid row
        else:
            # if prev_row isn't empty, combine with current row
            if prev_row != []:
                combined = prev_row + row
                if is_valid_row(combined):
                    writer.writerow([row[0]] + [x.strip() for x in row[1:]])
                    prev_row = []
                else:
                    # if > 5 items and last item in list contains '@'
                    # then this is a "complete" bad row to be cleansed
                    if len(combined) > 5 and '@' in combined[-1:][0]:
                        fixed_row = fix_bad_row(combined)
                        if fixed_row[0] == '-1':
                            print(f"discarding row that couldn't be cleansed: "
                                  f"{combined}")
                        else:
                            writer.writerow(fixed_row)
                        prev_row = []

                    # otherwise continue to next row with current combined
                    # as previous row value
                    else:
                        prev_row = combined

            # otherwise, start of a bad row
            else:
                prev_row = row

    print(f"output written to: {output_filename}")
