import os
#
# db_utils
#
import aws_s3_utils

DB_LOC = "s3://hubseq-db/"

def db_insert(tbl, rows):
    response = aws_s3_utils.add_to_json_object(os.path.join(DB_LOC,tbl+".json"), rows)
    return response

def db_fetch(tbl):
    response = aws_s3_utils.get_json_object(os.path.join(DB_LOC,tbl+".json"))
    return response[0]

