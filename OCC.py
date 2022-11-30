from transaction import transaction

class TransactionQ():
    def __init__(self):
        self.WTS = []
        self.RTS = []
        self.waiting_queue = []
        self.commit = False
        self.start_time = 0
        self.finish_time = 0
    
    def finishes(self, ts):
        self.finish_time = ts
        self.commit = True
    
    def isCommitted(self):
        return self.commit, self.start_time, self.finish_time
    
    def isFirst(self):
        return len(self.WTS) == 0 and len(self.RTS) == 0
    
    def setCommit(self, commit):
        self.commit = commit
    
    def doRead(self, ts, obj):
        if (self.isFirst):
            self.start_time = ts
        self.RTS.append([ts, obj])
    
    def doWrite(self, ts, obj):
        if (self.isFirst):
            self.start_time = ts
        self.WTS.append([ts, obj])
    
    def doAbort(self, ts):
        # Abort transactions with timestamp < ts
        for r in self.RTS:
            if r[0] < ts:
                self.waiting_queue.append(["R", r.ts, r.obj])
        for w in self.WTS:
            if w[0] < ts:
                self.waiting_queue.append(["W", w.ts, w.obj])
        self.waiting_queue.sort(key = lambda x: x[1])
        print("Aborted.")
        print("Queue: ", self.waiting_queue)
        return self.waiting_queue

class OCC():
    def __init__(self, num_trans, objs, transactions):
        self.num_trans = num_trans
        self.objs = objs
        self.transactions = transactions
        self.ts = 0
        self.T = []
        for i in range(self.num_trans):
            self.T.append(TransactionQ())
    
    def start(self):
        for trans in self.transactions:
            # Just do read or write in local variable in read phase
            self.ts += 1
            if trans.transaction_type == "R":
                self.T[trans.transactions_ts].doRead(self.ts, trans.transaction_obj)
            elif trans.transaction_type == "W":
                self.T[trans.transaction_ts].doWrite(self.ts, trans.transaction_obj)
            elif trans.transaction_type == "C":
                # commit time
                # do validation against smaller transaction ts
                # commit only if all read and write are valid
                # if not valid, abort
                self.T[trans.transaction_ts].setCommit = self.validation_phase(trans.transaction_ts)
                if (self.T[trans.transaction_ts].commit == True):
                    self.T[trans.transaction_ts].finishes(self.ts)
                else:
                    self.wait_q = self.T[trans.transaction_ts].doAbort(self.ts)
                    for q in self.wait_q:
                        self.transactions.append(transaction(q[0]+str(trans.transaction_ts)+"("+q[2]+")"))
            elif trans.transaction_type == "A":
                self.T[trans.transaction_ts].doAbort(self.ts)

    def validation_phase(self, trans_ts):
        # Check if others are committed
        for i in self.num_trans:
            if i != trans_ts and self.T[i].isCommitted == True:
                # check current transaction in respect to this committed transaction
                WSTi = self.T[i].WTS
                RSTk = self.T[trans_ts].RTS
                overlap = self.checkOverlap(WSTi, RSTk)
                if self.T[i].finish_time < self.T[trans_ts].start_time or not overlap:
                    # validation success
                    return True
                else:
                    return False
    
    def checkOverlap(self, TS1, TS2):
        # Compare each value in TS1 to all TS2
        for i in TS1:
            for j in TS2:
                if i[1] == j[1]:
                    return True
        return False

    # def print_results(self):
    #     for r in self.results:
    #         print(r, end="; ")