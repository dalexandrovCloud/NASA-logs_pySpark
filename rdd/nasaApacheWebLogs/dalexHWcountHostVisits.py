from pyspark import SparkContext, SparkConf
import itertools
import collections
from operator import add

conf = SparkConf().setAppName('NASA_count_host_visits').setMaster('local[*]')
sc = SparkContext(conf = conf)
def getHostCol(line):
	line_col = line.split('\t')
	return ','.join([line_col[0]])



julyLogs = sc.textFile('../../in/nasa_19950701.tsv')
augustLogs = sc.textFile('../../in/nasa_19950801.tsv')

nasaLogs = julyLogs.union(augustLogs).filter(lambda line: not line.startswith('host'))
nasaLogs = (nasaLogs.map(lambda line: getHostCol(line)).countByValue().items())
nasaLogs = ('\n'.join([str(i) for i in itertools.chain(*nasaLogs)])).split('\n')


sc.parallelize(nasaLogs).saveAsTextFile('../../out/NASA_count_host_visits.tsv')

