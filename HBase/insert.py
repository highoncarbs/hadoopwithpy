# Using HBase via HappyBase to insert data
from __future__ import print_function
import csv
import happybase
import time

BATCH_SIZE = 1000 # No more time improvement above 2 sec
HOST = "0.0.0.0"
FILE = "~/testfile_hb.csv"
ROW_COUNT = 0
TIME = time.time()
TABLE_NAME = "testdb"
namespace = "sample data"

def connect_to_hbase():
    # Connects to HBase server
    conn = happybase.Connection(host = HOST,
            table_prefix = NAMESPACE,
            table_prefix_separator = ":")
    conn.open()
    table = conn.table(TABLE_NAME)
    batch = table.batch(batch_size = batch_size)
    return conn , batch


def insert_row(batch , row):
    # Insert row of data into HBase
    batch.put(row[0], { "data:kw": row[1], "data:sub": row[2], "data:type": row[3],
        "data:town": row[4], "data:city": row[5], "data:zip": row[6],
        "data:cdist": row[7], "data:open": row[8], "data:close": row[9],
        "data:status": row[10], "data:origin": row[11], "data:loc": row[12] })

def read_csv():
    csv_file = opne(FILE , "r")
    csvreader = csv.reader(csv_file)
    return csvreader , csvfile

# Script

conn , batch = connect_to_hbase()
csvreader , csv_file = read_csv()

try:
    # We loop through all the rows , the first row contains the headers so we'll skip that
    for row in csvreader:
        ROW_COUNT+= 1
        if ROW_COUNT == 1:
            pass
        else:
            insert_row(batch, row)

        batch.send()
finally:
    csv_file.close()
    conn.close()

duration = time.time() - TIME
print("Done. Row Count : %i , Duration : %.3f s".format(ROW_COUNT , duration))
