from transaction import transaction
import re

class simple_locking():
    def __init__(self, num_trans, objs, transactions):
        self.num_trans = num_trans
        self.objs = objs
        self.transactions = transactions
        self.lock_table = {}
        self.waiting_table = {}
        self.queue = []
        self.each_transaction = {}
        self.results = []
        self.init_lock_table()

    
    def init_lock_table(self):
        for obj in self.objs:
            self.lock_table[obj] = None

    def append_transaction(self, transaction):
        self.queue.append(transaction)

    def append_each_transaction(self, transaction):
        if transaction.get_ts() in self.each_transaction:
            self.each_transaction[transaction.get_ts()].append(transaction)
        else:
            self.each_transaction[transaction.get_ts()] = [transaction]
    
    def check_have_lock(self, transaction):
        if self.lock_table[transaction.get_obj()] == transaction.get_ts():
            return True
        return False
    
    def check_trans_finished(self, transaction):
        for t in self.queue:
            if t.get_ts() == transaction.get_ts():
                return False
        return True

    def try_get_lock(self, transaction):
        if not self.lock_table[transaction.get_obj()]:
            self.lock_table[transaction.get_obj()] = transaction.get_ts()
            self.results.append(f"XL{transaction.get_ts()}({transaction.get_obj()})")
            return True
        else:
            return False

    def wait(self, transaction):
        if (transaction.get_ts() in self.waiting_table):
            if (not self.waiting_table[transaction.get_ts()][0]):
                self.waiting_table[transaction.get_ts()][0] = transaction.get_obj()
            if (transaction not in self.waiting_table[transaction.get_ts()][1]):
                self.waiting_table[transaction.get_ts()][1].append(transaction)
        else:
            self.waiting_table[(transaction.get_ts())] = [transaction.get_obj(), [transaction]]
        

    def check_if_waiting(self, transaction):
        if transaction.get_ts() in self.waiting_table:
            if not self.waiting_table[transaction.get_ts()][0]:
                return False
            return True
        return False

    def remove_from_wt(self, transaction):
        if transaction.get_ts() in self.waiting_table:
            if transaction in self.waiting_table[transaction.get_ts()][1]:
                self.waiting_table[transaction.get_ts()][1].remove(transaction)

    def execute(self, transaction):
        if not self.check_if_waiting(transaction):
            if self.check_have_lock(transaction):
                self.results.append(transaction.__str__())
                self.remove_from_wt(transaction)
                return
            else:
                if (self.try_get_lock(transaction)):
                    self.results.append(transaction.__str__())
                    self.remove_from_wt(transaction)
                    return
        self.wait(transaction)

    def commit(self, transaction):
        if not self.check_if_waiting(transaction):
            self.results.append(transaction.__str__())

            lock_released = []
            for lock in self.lock_table:
                if self.lock_table[lock] == transaction.get_ts():
                    self.lock_table[lock] = None
                    lock_released.append(lock)
                    self.results.append(f"UL{transaction.get_ts()}({lock})")
                    
            for ts in self.waiting_table:
                if self.waiting_table[ts][0] in lock_released:
                    self.waiting_table[ts][0] = None
                    waiting_table = self.waiting_table[ts][1].copy()
                    for t in waiting_table:
                        if t.get_type() == "R" or t.get_type() == "W":
                            self.execute(t)
                        elif t.get_type() == "C":
                            self.commit(t)
        else:
            self.wait(transaction)


    def abort(self, ts):
        self.results.append(f"A{ts}")
        for item in self.each_transaction[ts]:
            self.append_transaction(item)

    def DeadlockHandler(self):
        stack = []
        for ts in self.waiting_table:
            stack.insert(0,ts)
        while len(stack) > 0:
            ts = stack.pop(0)
            if self.waiting_table[ts][0]:
                self.abort(ts)
        self.waiting_table = {}
        self.lock_table = {}
        self.init_lock_table()
        self.each_transaction = {}

    def start(self):
        for t in self.transactions:
            self.append_transaction(t)
        finished = False
        while (not finished):
            while len(self.queue) > 0:
                t = self.queue.pop(0)
                self.append_each_transaction(t)
                if t.get_type() == "R" or t.get_type() == "W":
                    self.execute(t)
                elif t.get_type() == "C":
                    self.commit(t)
            if len(self.results) == 0:
                print("No transactions")
                finished = True
            # finished = True
            else:
                self.DeadlockHandler()
                if len(self.queue) == 0:
                    finished = True



    def print_results(self):
        for r in self.results:
            print(r, end="; ")
