from flask import Flask, render_template, request
import twitterfetch
import sentiment
from threading import Thread


twf = twitterfetch.TwitterFetch()
thread_fetch = None
thread_spark = None

app = Flask(__name__)

@app.route("/")
def main():
    return render_template("index.html")
 
@app.route("/run", methods=['GET','POST'])
def run():
    global flag_isStreaming
    flag_isStreaming = True
    topic_name = request.form['topic_name']
    twf.flag_fetch = True
    twf.topic_name = topic_name
    sentiment.TOPIC = topic_name
    thread_fetch = Thread(target = twf.fetch)
    thread_fetch.start()    
    thread_spark = Thread(target = sentiment.start_spark)  
    thread_spark.start()  
    twf.test_var = "Run"
    return twf.test_var

@app.route("/stop")
def stop():
    global flag_isStreaming
    flag_isStreaming = False
    thread = Thread(target = sentiment.abort_spark)
    thread.start()
    twf.flag_fetch = False
    twf.test_var = "Stop"
    twf.abort()        
    return twf.test_var

flag_isStreaming = False
if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=3000)
