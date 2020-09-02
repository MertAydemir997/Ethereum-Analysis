from mrjob.job import MRJob
import time

class timeanalysis(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if len(fields)==9:
                monthYear = time.strftime("%Y-%m", time.gmtime(int(fields[7])))
                transactions = int(fields[8])
                yield(monthYear, transactions)
        except:
            pass

    def reducer(self, monthYear, count):
        transactionSum = sum(count)
        yield(monthYear, transactionSum)

if __name__ == '__main__':
    timeanalysis.run()

#job ID
#http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1575381276332_2427/
