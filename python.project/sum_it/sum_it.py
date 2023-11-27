class SumIt:
    
    def __init__(self, arg):
        self.arg = arg
 
    def sum(self):
        total = 0
        for val in self.arg:
            total += val
        return total
