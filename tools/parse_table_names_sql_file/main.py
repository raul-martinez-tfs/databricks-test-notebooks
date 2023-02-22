"""
source: 
https://judecapachietti.medium.com/writing-a-python-script-to-read-sql-table-names-from-a-sql-file-afa49216ef7a

sample cmd: 
    python main.py -i ./f_invntry_bal_dly_hist_e1lsg.sql
sample output: 
    ['d_curncy_mth_rt', 'd_date', 'edp_lkup', 'f0005', 'f0006', 'f0010', 'f4101_adt', 'F41021', 'f4102_adt', 'f4105']
"""

import argparse

def process_sql_file(file_name):
    file, string = open(file_name, "r"), ''

    for line in file:
        line = line.rstrip()
        line = line.split('//')[0]
        line = line.split('--')[0]
        line = line.split('#')[0]
        line = line.replace('(', ' ( ')
        line = line.replace(')', ' ) ')
        string += ' ' + line
    file.close()

    # remove multi-line comments:
    while string.find('/*') > -1 and string.find('*/') > -1:
        l_multi_line = string.find('/*')
        r_multi_line = string.find('*/')
        string = string[:l_multi_line] + string[r_multi_line + 2:]

    # remove extra whitespaces and make list
    words = string.split()

    return words

def find_table_names(words):
    table_names = set()
    previous_word = None

    for word in words:
        if previous_word == 'from' or previous_word == 'join':
            if word != '(':
                table_names.add(word)
        previous_word = word

    return sorted(list(table_names))

def find_table_names_from_sql_file(file_name):
    words = process_sql_file(file_name)
    return sorted(find_table_names(words), key=lambda s: s.lower())


if __name__ == '__main__':
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-i", "--input", help="input sql file")
    args = argParser.parse_args()

    print(find_table_names_from_sql_file(args.input))

