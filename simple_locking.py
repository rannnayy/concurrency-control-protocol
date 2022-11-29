from transaction import transaction

class simple_locking():
    def __init__(self, num_trans, objs, transactions):
        self.num_trans = num_trans
        self.objs = objs
        self.transactions = transactions
        self.lock_table = {}
        self.queue = []
        self.results = []
        self.init_lock_table()

    
    def init_lock_table(self):
        for obj in self.objs:
            self.lock_table[obj] = (False, None)

    def lock(self, transaction):
        # print(self.lock_table[transaction.get_obj()][0])
        if not self.lock_table[transaction.get_obj()][0]:
            self.lock_table[transaction.get_obj()] = (True, transaction.get_ts())
            self.results.append(f"XL{transaction.get_ts()}({transaction.get_obj()})")
            self.results.append(transaction.__str__())
        else:
            self.append_transaction(transaction)

    def append_transaction(self, transaction):
        self.queue.append(transaction)

    def unlock(self, transaction):
        for lock in self.lock_table:
            if self.lock_table[lock][1] == transaction.get_ts():
                self.lock_table[lock] = (False, None)
                self.results.append(transaction.__str__())
                self.results.append(f"UL{transaction.get_ts()}({lock})")
    
    def check_trans_finished(self, transaction):
        for t in self.queue:
            if t.get_ts() == transaction.get_ts():
                return False
        return True


    def start(self):
        for t in self.transactions:
            self.append_transaction(t)
        while len(self.queue) > 0:
            t = self.queue.pop(0)
            if t.get_type() == "R" or t.get_type() == "W":
                self.lock(t)
            elif t.get_type() == "C":
                if self.check_trans_finished(t):
                    self.unlock(t)
                else:
                    self.append_transaction(t)


    def print_results(self):
        for r in self.results:
            print(r, end="; ")
