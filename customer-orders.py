from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Customer-orders")
sc = SparkContext(conf = conf)

def parseLine(lines):
    fields=lines.split(',')
    customer=int(fields[0])
    purchase=float(fields[2])
    return customer, purchase
    
lines=sc.textFile(r'C:\SparkCourse\customer-orders.csv')

rdd=lines.map(parseLine)
totalPurchase=rdd.reduceByKey(lambda x,y: x+y)
totalPurchaseSwap=totalPurchase.map(lambda x: (x[1], x[0]))
totalPurchaseSorted=totalPurchaseSwap.sortByKey()
moneySorted=totalPurchaseSorted.map(lambda x: (x[1], x[0]))
results=moneySorted.collect()

for result in results:
    print(result[0], "\t{:.2f}".format(result[1]))
    #
#for id, order in results:
#    print("customer {}: money spent {:.2f}".format(id,order))