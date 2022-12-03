class transaction:
    def __init__(self, transaction):
        self.transaction_type = transaction[0]
        if (self.transaction_type == "R" or self.transaction_type == "W"):
            self.transaction_ts = int(transaction[1:transaction.index("(")])
            self.transaction_obj = transaction[transaction.index("(")+1:transaction.index(")")]
        else:
            self.transaction_ts = int(transaction[1:])
            self.transaction_obj = ""

    def __str__(self):
        if self.transaction_type == "R" or self.transaction_type == "W":
            return f"{self.transaction_type}{self.transaction_ts}({self.transaction_obj})"
        else:
            return f"{self.transaction_type}{self.transaction_ts}"

    def get_type(self):
        return self.transaction_type
    
    def get_ts(self):
        return self.transaction_ts

    def get_obj(self):
        return self.transaction_obj