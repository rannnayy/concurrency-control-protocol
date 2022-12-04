from transaction import transaction
import re

class simple_locking():
    def __init__(self, num_trans, objs, transactions):
        self.num_trans = num_trans
        self.objs = objs
        self.transactions = transactions
        self.lock_table = {}
        self.waiting_table = {}
        self.each_transaction = {}
        self.timestamp = {}
        self.queue = []
        self.waiting_queue = []
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
            self.waiting_table[(transaction.get_ts())] = None 
            self.timestamp[(transaction.get_ts())] = transaction.get_ts()
    
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

    

    def check_deadlock(self,transaction):
        def rec_deadlock(owner_lock, check_ts):
            if owner_lock in self.waiting_table and self.waiting_table[owner_lock]:
                if self.lock_table[self.waiting_table[owner_lock]] == check_ts:
                    return True
                else:
                    return rec_deadlock(self.lock_table[self.waiting_table[owner_lock]], check_ts)
            else: 
                return False

        if self.waiting_table[transaction.get_ts()]:
            lock_owner = self.lock_table[self.waiting_table[transaction.get_ts()]]
            if lock_owner in self.waiting_table and self.waiting_table[lock_owner]:
                if rec_deadlock(lock_owner, transaction.get_ts()):
                    self.results.append("Deadlock detected!")

    def wound_wait(self,transaction):
        if self.waiting_table[transaction.get_ts()]:
            lock_owner = self.lock_table[self.waiting_table[transaction.get_ts()]]
            if self.timestamp[lock_owner] < self.timestamp[transaction.get_ts()]:
                self.wait(transaction)
            else:
                for item in self.queue:
                    if item.get_ts() == lock_owner:
                        self.abort(item)
                        break

    def wait(self, transaction):
        if (not self.waiting_table[transaction.get_ts()]):
            self.waiting_table[transaction.get_ts()] = transaction.get_obj()
        if (transaction not in self.waiting_queue):
            self.waiting_queue.append(transaction)      
        

    def check_if_waiting(self, transaction):
        if transaction.get_ts() in self.waiting_table:
            if not self.waiting_table[transaction.get_ts()]:
                return False
            return True
        return False

    def remove_from_wt(self, transaction):
        if transaction.get_ts() in self.waiting_table:
            if transaction in self.waiting_queue:
                self.waiting_queue.remove(transaction)

    def execute(self, transaction):
        if not self.check_if_waiting(transaction):
            if self.check_have_lock(transaction):
                self.results.append(transaction)
                self.remove_from_wt(transaction)
                return
            else:
                if (self.try_get_lock(transaction)):
                    self.results.append(transaction)
                    self.remove_from_wt(transaction)
                    return
                else:
                    self.wait(transaction)
                    if (self.is_deadlock_prevention.upper() == "Y"):
                        self.wound_wait(transaction)
                    else:
                        self.check_deadlock(transaction)
                    return
        self.wait(transaction)

    def unlock(self, ts):
        lock_released = []
        for lock in self.lock_table:
            if self.lock_table[lock] == ts:
                self.lock_table[lock] = None
                lock_released.append(lock)
                self.results.append(f"UL{ts}({lock})")
                
        for ts in self.waiting_table:
            if self.waiting_table[ts] in lock_released:
                self.waiting_table[ts] = None
                waiting_queue = self.waiting_queue.copy()
                for t in waiting_queue:
                    if t.get_type() == "R" or t.get_type() == "W":
                        self.execute(t)
                    elif t.get_type() == "C":
                        self.commit(t)


    def commit(self, transaction):
        if not self.check_if_waiting(transaction):
            self.results.append(transaction)
            self.unlock(transaction.get_ts())
            self.remove_from_wt(transaction)
        else:
            self.wait(transaction)


    def abort(self, transaction):
        self.results.append(f"A{transaction.get_ts()}")
        self.unlock(transaction.get_ts())
        max = 0
        for key in self.timestamp:
            if key > max:
                max = key
        self.timestamp[transaction.get_ts()] = max + 1
        for item in self.each_transaction[transaction.get_ts()]:
            if item == transaction:
                break
            self.queue.append(item)


    def start(self):
        self.is_deadlock_prevention = input("Activate deadlock prevention (wound-wait)? (y/n): ")
        for t in self.transactions:
            self.append_transaction(t)
        while len(self.queue) > 0:
            t = self.queue.pop(0)
            self.append_each_transaction(t)
            if t.get_type() == "R" or t.get_type() == "W":
                self.execute(t)
            elif t.get_type() == "C":
                self.commit(t)
        if len(self.results) == 0:
            print("No transactions")


    def print_results(self):
        print("Schedule result: ")
        for r in self.results:
            print(r, end="; ")
        print()
        for t in self.waiting_queue:
                if t.get_type() == "C":
                    print("Transaction " + str(t.get_ts()) + " caught in deadlock")
        

