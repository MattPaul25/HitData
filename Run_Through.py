import csv


def get_index(column_name, file_name):
    """ gets the index of a column in a single column csv file or text file
    :rtype: int
    """
    index_num = 0
    with open(file_name, 'r') as lookup_file:
        file_reader = csv.reader(lookup_file)
        for line in file_reader:
            if line[0] == column_name:
                return index_num
            else:
                index_num += 1
    return -1


def filter_data(file_in, file_out):
    with open(file_in, encoding="ISO-8859-1") as tsvfile, open(file_out, 'w') as csvout:

        lookup_fil = 'columns.csv'
        pagename_ind = get_index('evar10', lookup_fil)
        page_event_ind = get_index('post_page_event', lookup_fil)
        date_time_ind = get_index('date_time', lookup_fil)
        referrer_ind = get_index('referrer', lookup_fil)
        username_ind = get_index('username', lookup_fil)
        unique_id_ind = [get_index('post_visid_high', lookup_fil), get_index('post_visid_low', lookup_fil)]

        tsv_reader = csv.reader(tsvfile, delimiter="\t")
        csv_writer = csv.writer(csvout)


        for line in tsv_reader:
            try:
                pagename = line[pagename_ind]
                page_event = line[page_event_ind]  # post page event
                date_time = line[date_time_ind]
                unique_id = line[unique_id_ind[0]] + '-' + line[unique_id_ind[1]]
                referrer = line[referrer_ind]
                user = line[username_ind]
                if pagename == 'fbn:root:front:channel' and page_event == '0':
                    rec = pagename + '|' + page_event + '|' + date_time + '|' + unique_id + '|' + referrer + '|' + user
                    csv_writer.writerow([rec])  # brackets wrap the string as a list object

            except Exception as e:
                print(e)

def count_records(file_in):
    cnt = 0
    with open(file_in, encoding="ISO-8859-1") as filein:
        reader = csv.reader(filein)
        for line in reader:
            cnt += 1
    return cnt


print(str(count_records("hit_data.tsv")))
filter_data("hit_data.tsv", "out.csv")
