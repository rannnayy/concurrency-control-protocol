from myparser import *
from simple_locking import * 
from OCC import *
from transaction import transaction
import sys
import os

if __name__ == "__main__":
    if (len(sys.argv) != 3):
        print("Usage: python3 main.py <file_name> <method>")
        print("<method> is one of: \n\tsimple-locking\n\tsimple-optimistic-cc\n\tmultiversion-ts-ord-cc")
        exit()
    cwd = os.getcwd()
    file_name = cwd + '\\test\\' + sys.argv[1]
    # print(file_name)
    if (not os.path.isfile(file_name)):
        print("File does not exist")
        exit()
    method = sys.argv[2]
    if method not in ["simple-locking", "simple-optimistic-cc", "multiversion-ts-ord-cc"]:
        print("<method> is one of: \n\tsimple-locking\n\tsimple-optimistic-cc\n\tmultiversion-ts-ord-cc")
        exit()
    num_trans, objs, transactions = parse_input(file_name)

    if method == "simple-locking":
        simple_locking = simple_locking(num_trans, objs, transactions)
        simple_locking.start()
        simple_locking.print_results()
    elif method == "simple-optimistic-cc":
        print("Simple Optimistic Concurrency Control")
        occ = OCC(num_trans, objs, transactions)
        occ.start()
    elif method == "multiversion-ts-ord-cc":
        print("Multiversion Timestamp Ordered Concurrency Control")