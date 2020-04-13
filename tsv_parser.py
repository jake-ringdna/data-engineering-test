#! /usr/bin python3

def parse_int(i):
    """
    Return True if i can parse to int else Return False
    """
    try:
        int(i)
        return True
    except ValueError:
        return False


def format_tsv(input_filepath, output_filepath):
    """
    Read Lines from TSV and parse them for formatting.
    Output to TSV format.
    """
    broken_line = []
    output = open(output_filepath, 'w', encoding="utf-8")
    for row in open(input_filepath, 'r', encoding='utf-16-le'):
        line = row.split('\t')
        if len(line) == 5:
            broken_line = []
            output.write("\t".join(line))
        else:
            broken_line.extend(line)
            if '' in broken_line: broken_line.remove('')
            if '\n' in broken_line: broken_line.remove('\n')
            if '\t' in broken_line: broken_line.remove('\t')
            if len(broken_line) > 3 and not parse_int(broken_line[3]): broken_line.pop(3)
            broken_line = [elem.replace('\n', '').strip() for elem in broken_line]
            if len(broken_line) == 5:
                output.write("\t".join(broken_line))

if __name__ == "__main__":
    format_tsv('data/data.tsv', 'data/clean_data.tsv')
