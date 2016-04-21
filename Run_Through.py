import csv


class Columns(object):
    """this class takes in a series of columns and can contian a lambda expression to have a logical 'where' for the columns"""
    def __init__(self, predicate=''):
        self.columns = []
        self.predicate = predicate
        self.set_lambda(self.predicate)

    def add_columns(self, column):
        self.columns.append(column)

    def set_lambda(self, predicate):
        if callable(predicate):
            self.predicate = predicate
        else:
            self.predicate = ''

    def __iter__(self):
        return iter(self.columns)



class ColumnObject(object):
    """class contains information about a column, like where it sits within tbl arr and its name.
     This object stores an option predicate -- that acts like a where clause"""
    def __init__(self, name, lookup_name, file_name, predicate=''):
        self.name = name
        self.lookup_name = lookup_name
        self.file_name = file_name
        self.predicate = predicate
        self.set_lambda(self.predicate)
        self.lookup_index = self.get_index()

    def set_lambda(self, predicate):
        if callable(predicate):
            self.predicate = predicate
        else:
            self.predicate = ''

    def get_index(self):
        """ gets the index of a column in a single column csv file or text file
        :rtype: int
        """
        index_num = 0
        with open(self.file_name, 'r') as lookup_file:
            file_reader = csv.reader(lookup_file)
            for line in file_reader:
                if line[0] == self.lookup_name:
                    return index_num
                else:
                    index_num += 1
        return -1


class DataManipulation(object):
    """this class manipulates data passed to it and outputs a result"""
    def __init__(self, columns, file_in, file_out, delimiter='|'):
        print('this may take a bit')
        self.columns = columns
        self.file_in = file_in
        self.file_out = file_out
        self.delimiter = delimiter
        self.rec_count = self.filter_data()

    def filter_data(self):
        counter = 0
        with open(self.file_in, encoding='ISO-8859-1') as tsvfile, open(self.file_out, 'w') as csvout:

            tsv_reader = csv.reader(tsvfile, delimiter='\t')
            csv_writer = csv.writer(csvout)

            for line in tsv_reader:
                column_ok = True
                try:
                    result_str = ''
                    for clm in self.columns:
                        if column_ok:
                            result_str = result_str + (line[clm.lookup_index]) + self.delimiter
                            if clm.predicate != '':
                                column_ok = clm.predicate(line[clm.lookup_index])
                                rec = result_str
                        else:
                            break
                    if column_ok:
                        csv_writer.writerow([rec])  # brackets wrap the string as a list object
                        counter += 1

                except Exception as e:
                    print(e)

        return counter

    def count_records(self, file_in):
        cnt = 0
        with open(file_in, encoding="ISO-8859-1") as filein:
            reader = csv.reader(filein)
            for line in reader:
                cnt += 1
        return cnt

lookup_fil = 'columns.csv'

c = Columns()

c.add_columns(ColumnObject('pagename', 'pagename', lookup_fil, predicate=lambda n: n == 'fbn:root:front:channel'))
c.add_columns(ColumnObject('page_event', 'post_page_event', lookup_fil, predicate=lambda n: n == '0'))
c.add_columns(ColumnObject('date_time', 'date_time', lookup_fil))
c.add_columns(ColumnObject('referrer', 'referrer', lookup_fil))
c.add_columns(ColumnObject('first_hit_page', 'first_hit_pagename', lookup_fil))
c.add_columns(ColumnObject('article', 'post_prop12', lookup_fil))
c.add_columns(ColumnObject('events', 'event_list', lookup_fil))
c.add_columns(ColumnObject('ref_domain', 'ref_domain', lookup_fil))
c.add_columns(ColumnObject('ref_type', 'ref_type', lookup_fil))
c.add_columns(ColumnObject('unique_id1', 'post_visid_high', lookup_fil))
c.add_columns(ColumnObject('unique_id2', 'post_visid_low', lookup_fil))


# myColumns = [column1, column2, column3, column4, column5, column6, column7, column8, column9, column10, column11]
print(str(DataManipulation(c, "hit_data.tsv", "12-21.csv")))
print(str(DataManipulation(c, "hit_data2.tsv", "3-7.csv")))
#filter_data(myColumns, "hit_data.tsv", "12-21.csv")))
#filter_data(myColumns, "hit_data2.tsv", "3-7.csv")))
