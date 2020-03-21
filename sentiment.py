import sys
from datetime import datetime

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext

from textblob import TextBlob
import re


#URL = '127.0.0.1:9200'
URL = 'https://65bdede3fecb43999407e6a85f22bf6e.us-west-1.aws.found.io:9243'
INDEX = 'twitter'
TYPE = 'tweets'
RESOURCE = '%s/%s' % (INDEX, TYPE)
TOPIC = None
COUNTER = 0
# Creating Spark Configuration
conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("TwitterStreamApp")
conf.set('es.nodes', URL)
#conf.set('es.index.auto.create', 'true')

spc = None
spsc = None


def aggr_tags_count(new_values, total_sum):
    '''
    The function 'aggr_tags_count' aggregates and sum up the hashtag counts
    collected for each category
    '''
    return sum(new_values) + (total_sum or 0)

def get_sqlcontext_instance(spark_context):
    '''
    Create sql context object globally (singleton object)
    '''
    
    sqlContextSingletonInstance = 'sqlContextSingletonInstance' + str(COUNTER)
    if sqlContextSingletonInstance not in globals():
        globals()[sqlContextSingletonInstance] = SQLContext(spark_context)
    return globals()[sqlContextSingletonInstance]

def send_dataframe_to_elasticsearch(df):
    '''
    Send data frames to elesticsearch
    '''
    RESOURCE = '%s/%s' % (INDEX, TYPE)
    df.write.format('org.elasticsearch.spark.sql').mode('overwrite').save(RESOURCE)

def send_dataframe_to_elasticsearch2(df):
    '''
    Send data frames to elesticsearch
    '''
    
    #df.write.format('org.elasticsearch.spark.sql').option('es.index.auto.create', 'true').option('es.resource', 'index_name-{ts_col:YYYY.mm.dd}/type_name').option('es.mapping.id', 'es_id').save()
    df.write.mode('append').format('org.elasticsearch.spark.sql').option('es.nodes.wan.only','true').option('es.net.http.auth.user','elastic').option('es.net.http.auth.pass','Bu1FSLp8O5dw8jxn02nKVvaJ').option('es.nodes', URL).option('es.index.auto.create', 'true').option('es.resource', RESOURCE).save()

def rdd_process2(time, rdd):
        print("~~~~~~~~~~~~~~ %s ~~~~~~~~~~~~~~" % str(time))
    #try:
        # Get spark sql singleton context from the current context
        sql_context = get_sqlcontext_instance(rdd.context)
        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(
            topic= TOPIC, sentence=w[0], polarity=w[1], timestamp=datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
        if row_rdd.isEmpty():
            print('RDD is empty')
        else:
            # create a DF from the Row RDD
            hashtags_df = sql_context.createDataFrame(row_rdd)
            # Register the dataframe as table
            hashtags_df.registerTempTable("sentiment")
            # get the top 10 hashtags from the table using SQL and print them
            hashtag_counts_dataf = sql_context.sql("select topic, sentence, polarity, timestamp  from sentiment")
            hashtag_counts_dataf.show()
            send_dataframe_to_elasticsearch2(hashtags_df)
    # except Exception as e:
    #     f = sys.exc_info()
    #     print("Error: %s" % f[0])

def start_spark():
    # Create Spark instance with the above spark configuration   
    
    spc = SparkContext(conf=conf)
    spc.setLogLevel("ERROR")
    global spsc
    # Create the Streaming Context from the spark context created above with window size 2 seconds
    spsc = StreamingContext(spc, 2)
    global COUNTER
    COUNTER = COUNTER + 1
    # Setting a checkpoint to allow RDD recovery in case of node unavailability
    spsc.checkpoint("checkpoint_TwitterApp")

    # Read data from port 9009 on localhost
    dataStream = spsc.socketTextStream("localhost", 9009)
    sentences = dataStream.flatMap(lambda line: line.strip().split("||")).map(lambda x: ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", x).split()))
    filter_Sentence = sentences.map(lambda x: (x, TextBlob(x).sentiment.polarity))
    filter_Sentence.foreachRDD(rdd_process2)
    # start the streaming computation
    spsc.start()
    # wait for the streaming to finish
    spsc.awaitTermination()

def abort_spark():
    global spsc
    spsc.stop(stopSparkContext=True, stopGraceFully=False)
