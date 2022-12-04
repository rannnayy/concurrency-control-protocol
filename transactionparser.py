from transaction import transaction

def read_file(file_name):
    file = open(file_name, 'r')
    lines = file.readlines()
    file.close()
    num_T = int(lines[0])
    objs = lines[1].split()
    trans = lines[2].split('; ')
    
    return num_T, objs, trans

def parse_input(file_name):
    num_trans, objs, trans = read_file(file_name)
    transactions = []
    for t in trans:
        transactions.append(transaction(t))
    return num_trans, objs, transactions

# parse_input('test.txt')