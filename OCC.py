from transaction import transaction

class T():
    def __init__(self, no):
        self.no = no
        self.transactions = []
        self.start = -1
        self.validation = -1
        self.ts = -1
    
    def __str__(self) -> str:
        return f"T{self.no}: startTS => {self.start}, validationTS => {self.validation}, ts => {self.ts}, transactions => {self.transactions}"
    
    def addTransaction(self, transaction):
        self.transactions.append(transaction)
    
    def setStart(self, start):
        self.start = start

    def setValidation(self, validation):
        self.validation = validation

    def setTS(self, ts):
        self.ts = ts

    def doAbort(self):
        # Abort transactions until found Commit
        while (self.transactions[-1].get_type() != "C" and len(self.transactions) > 0):
            self.transactions.pop()

    
class occ():
    def __init__(self):
        self.T = []
        self.all_transactions = []
        self.num_T = 0
        self.vars = []
        self.latest_ts = 0
    
    def start(self, num_T, vars, all_transactions):
        self.num_T = num_T
        self.vars = vars
        self.all_transactions = all_transactions
        for i in range(self.num_T):
            self.T.append(T(i+1))
        print("Read and Execution Phase")
        self.read()
        print(self.T)
        # Validation includes write operation
        print("Validation and Write Phase")
        self.validate()
    
    def read(self):
        count = 1
        for t in self.all_transactions:
            type = t.get_type()
            no = t.get_ts()

            self.T[no-1].addTransaction(t)
            if type == "C":
                # The last transaction is commit
                self.T[no-1].setValidation(count)
            elif type in ["R", "W"]:
                # The first transaction, set start time
                if self.T[no-1].start == -1:
                    self.T[no-1].setStart(count)
            
            count += 1

        # Check if validation unset
        # ex. if no C written
        for i in range(self.num_T):
            if self.T[i].validation == -1:
                self.T[i].setValidation(count)
                count += 1
        
        self.latest_ts = count
    
    def first_commit(self):
        min = 999999
        index = -1
        for i in range(self.num_T):
            if self.T[i].ts == -1 and self.T[i].validation < min:
                min = self.T[i].validation
                index = i
        return index, min

    def commit_earlier(self):
        earlier = []
        for i in range(self.num_T):
            # Search for T that has committed
            # Committed T has ts != -1
            if self.T[i].ts != -1:
                earlier.append(i)
        # print("EAAAAAAARRRRRRRRRRLLLLLLLLLIEEEEEEE")
        # print(earlier)
        return earlier
    
    def remove_abort(self, trans):
        # Check from the last transaction to the first if there's abort transaction
        # If there's abort transaction, remove it and all until found C
        found_A = False
        temp_trans = []
        for i in range (len(trans)-1, -1, -1):
            if trans[i].get_type() == "A":
                found_A = True
                continue
            if found_A and trans[i].get_type() != "C":
                continue
            if found_A and trans[i].get_type() == "C":
                found_A = False
            if not found_A:
                temp_trans.insert(0, trans[i])
        return temp_trans
    
    def print_result(self, trans):
        for t in trans:
            print(t)
    
    def validate(self):
        cnt = 0
        while cnt < self.num_T:
            # Find the T that commit first
            index, min_ts = self.first_commit()
            # Towards that T, perform validation to every T that has committed earlier
            valid = self.validation_test(index, min_ts)
            if (valid):
                self.T[index].setTS(min_ts)
                cnt += 1
                print(f"T{self.T[index].no} validated and written at {min_ts}")
                temp_trans = self.remove_abort(self.T[index].transactions)
                self.print_result(temp_trans)
            else:
                self.T[index].setValidate(self.latest_ts + 1)
    
    def validation_test(self, i, ts_i):
        # i should be in 0..num_T-1
        is_first = True
        for j in range(self.num_T):
            # Check for transaction other than i
            if j != i:
                # Check for past committed transaction
                if self.T[j].validation < ts_i:
                    is_first = False
                    if (self.T[j].validation < self.T[i].start) or (self.T[j].validation < self.T[i].validation and self.checkDisjoint(self.T[i].transactions, self.T[j].transactions)):
                        return True
        if is_first:
            return True
        return False
    
    def checkDisjoint(self, RTSi, WTSj):
        # Compare each value in TS1 to all TS2
        for i in RTSi:
            if (i.get_type() == "R"):
                for j in WTSj:
                    if (j.get_type() == "W"):
                        if i.get_obj() == j.get_obj():
                            return False
        return True