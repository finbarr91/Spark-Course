# Finding the Total amount spent by a customer.
from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("TotalSumCalculation")
sc = SparkContext(conf = conf)

def parseline(line):
    field = line.split(",")
    return (int(field[0]),float(field[2]))

lines = sc.textFile(r"C:\Users\chukw\PycharmProjects\Spark-Course\customer-orders.csv")
rdd = lines.map(parseline)
total_amount = rdd.reduceByKey(lambda x,y : x+y)
result1 = total_amount.collect()
for result in result1:
    print("This is result1==>",result)
flipped = total_amount.map(lambda x: (x[1], x[0]))
result2 = flipped.collect()
for result in result2:
    print("This is result2==>", result)
total_amount_sorted = flipped.sortByKey()
results = total_amount_sorted.collect()
for result in results:
    # print(f"{result[1]} : ${round(result[0],2)}")
    print(f"$ {round(result[0],2)} : {result[1]}")




