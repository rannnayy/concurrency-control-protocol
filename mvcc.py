from transaction import transaction

class Q():
    def __init__(self, var, k):
        self.var = var
        self.k = k
        self.WTS = 0
        self.RTS = 0
    
    def __str__(self):
        return f"Q({self.var}) TS({self.var}_{self.k})=({self.RTS}, {self.WTS})"
    
    def readOp(self, T_ts):
        self.RTS = T_ts
    
    def writeOp(self, T_ts):
        self.WTS = T_ts

class T():
    def __init__(self, ts):
        self.trans = []
        self.ts = ts
        self.res = []

    def __str__(self):
        return f"T{self.ts} having transactions {self.trans}"

    def addT(self, t):
        self.trans.append(t)
    
    def setTS(self, ts):
        # Because of rollback may get new TS
        self.ts = ts

class mvcc():
    def __init__(self):
        self.Q = []
        self.T = []
        self.current_ts = 0
    
    def __str__(self):
        return f"Q={self.Q} T={self.T}"
    
    def start(self, num_T, vars, transactions):
        self.num_T = num_T
        self.vars = vars
        self.transactions = transactions
        self.current_ts = num_T

        for i in range(self.num_T):
            self.T.append(T(i+1))
        
        for v in vars:
            self.Q.append(Q(v, 0))

        count = 0
        while True:
            if count >= len(self.transactions):
                break
            type = self.transactions[count].get_type()
            no = self.transactions[count].get_ts()
            var = self.transactions[count].get_obj()

            if type == "R":
                # Search for suitable version to be read
                for i in range (len(self.Q)-1, -1, -1):
                    if self.Q[i].var == var:
                        if self.Q[i].WTS <= self.T[no-1].ts:
                            if self.Q[i].RTS <= self.T[no-1].ts:
                                self.Q[i].readOp(self.T[no-1].ts)
                                self.T[no-1].addT(self.transactions[count])
                                self.T[no-1].res.append(self.Q[i].k)
                                print(self.transactions[count], "Reads", self.Q[i])
                            else:
                                self.T[no-1].addT(self.transactions[count])
                                self.T[no-1].res.append(self.Q[i].k)
                                print(self.transactions[count], "Reads", self.Q[i])
                            break
            elif type == "W":
                # Search for the latest version of var
                for i in range (len(self.Q)-1, -1, -1):
                    if self.Q[i].var == var:
                        if self.T[no-1].ts < self.Q[i].RTS:
                            # Rollback
                            # Rollback all transactions of T[no-1]
                            prev_ts = self.T[no-1].ts
                            self.current_ts += 1
                            self.T[no-1].setTS(self.current_ts)
                            self.transactions.insert(count + 1, transaction("A" + str(no)))
                            self.T[no-1].addT(self.transactions[count])
                            print(self.transactions[count], "Rollback.")
                            break
                        elif self.T[no-1].ts == self.Q[i].WTS:
                            self.T[no-1].addT(self.transactions[count])
                            print(self.transactions[count], "Rewrites", self.Q[i])
                            break
                        else:
                            self.Q.append(Q(var, self.T[no-1].ts))
                            self.Q[-1].writeOp(self.T[no-1].ts)
                            self.Q[-1].readOp(self.T[no-1].ts)
                            self.T[no-1].addT(self.transactions[count])
                            print(self.transactions[count], "New version.", self.Q[-1])
                            break
            elif type == "A":
                temp_start_T = self.search_c(self.T[no-1].trans)
                temp_len_T = len(self.T[no-1].trans)
                # print(temp_start_T, temp_len_T)
                temp_cascade = []
                for j in range(temp_start_T, temp_len_T):
                    self.transactions.insert(count + 1 + j - temp_start_T, self.T[no-1].trans[j])
                    # Check cascading rollback
                    # Search for every Q which has k = prev_ts (means that version was written by prev_ts)
                    for t in self.T:
                        for tres in t.res:
                            if tres == prev_ts:
                                temp_len_t_res = len(t.res)
                                for i in range(t.res.index(tres), temp_len_t_res):
                                    temp_cascade.append(t.trans[i])
                                break
                cnt_plus = 0
                # insert cascading transactions
                for c in temp_cascade:
                    self.transactions.insert(count + 1 + (temp_len_T - temp_start_T) + cnt_plus, c)
                    cnt_plus += 1
                print(self.transactions[count])
                self.T[no-1].addT(self.transactions[count])
            elif type == "C":
                print(self.transactions[count])
                self.T[no-1].addT(self.transactions[count])
            count += 1
    
    def search_c(self, trans):
        idx = len(trans) - 1
        # print("awal", idx)
        while idx >= 0:
            # print(idx)
            if trans[idx].get_type() == "C":
                return idx + 1
            idx -= 1
        return idx + 1