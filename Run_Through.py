import csv

with open("hit_data.tsv", encoding="ISO-8859-1") as tsvfile:
    tsvreader = csv.reader(tsvfile, delimiter="\t")
    status_variable = ""
    dupCount = 0
    recCount = 0
    for line in tsvreader:
        try:
            time_stamp = line[26]
            page_name = line[226]
            unique_id = line[493] + '-' + line[494]
            page_event = line[350] # post page event
            some_var = len(page_name)
            if some_var > 0:
                if page_event == '0':
                    recCount += 1
                    if time_stamp + page_name == status_variable:
                        dupCount += 1
                        print("duplicates: " + str(dupCount) + " and regular count: " + str(recCount))
            status_variable = time_stamp + page_name
        except Exception as e:
            print(e)