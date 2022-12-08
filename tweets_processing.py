# Importing libraries (Lazy importing)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# If you are saving locally
import os
caminho = os.getcwd()

#Initilizing Spark Session
spark = SparkSession.builder.appName('AppName').getOrCreate()


# Setting up Schema
schema = StructType([StructField('id', LongType(), True),
                     StructField('tweet_text', StringType(), True),
                     StructField('tweet_date', TimestampType(), False)])
                        
df_XPTO = spark.read.schema(schema).json('YOUR/PATH/TO/JSON/FILES/*/*')

# Transform emoticons find in tweet text to emoticons that are being analyzed (Didn't know how to read emoticons
emoticons = {"😀": ":D", "😃":"=D", "😄":":D", "🤣":":'D", "😂": ":'D", "😉":";)", "😊":":)", "😍":":D",
            "😔" : ":c", '🤢':":x", "🤮":":x", "😕":":/", "😟":":c", "🙁":":c", "☹️":":(", 
            "😧":":o", "😨":":'o", "😰":":'o", "😥":":'(", "😢":":'(", "😭": ":'c", 
            "😱":":'o", "😞":":c", "😩":":c", "😡":":@", "🤬":":@"
            }
            
for emote in emoticons.keys():
  df_XPTO = df_XPTO.withColumn('tweet_text', regexp_replace('tweet_text', emote, emoticons[emote]))

# Dics classifying neither emoticons and words/classifications
emotions = {"positive" : 
                        [":D",":)",":]", ": )", ":-)", "=D", ":'D", ";)"],
            "negative" :
                        [":(",":[",":{",": (", ":'o", ":@", ":x", ":c", ":'c", ":'(", ":o"]
            }
            
classifications = {"positive" : 
                        ["bom","ótimo","perfeito", "maravilhoso", "excelente"],
                   "negative" :
                        ["péssimo", "ridículo", "horrível", "ruim", "horroroso", "luladrão", "biroliro", "bozo", "9 dedos"]
                  }


# Create arrays to all emotions 
all_words = []
all_emotions = []
[all_emotions.extend(i) for i in emotions.values()]
[all_words.extend(i) for i in classifications.values()]

# Create a new column "emotion" based on selected emotion on list all_emotion, then locate it on tweet text and save position
for emotion in all_emotions:
    df_XPTO = df_XPTO.withColumn(emotion, instr(df_XPTO.tweet_text, emotion))


# Create a new columns based on checking least position from columns assigned to emotions ignoring null and 0 values, then drop all columns assigined to emotes
df_symbols = df_XPTO.withColumn("Symbol", least(*[
    when(col(c).isNull() | (col(c) == 0), None).otherwise(c)
        for c in df_XPTO.columns[3:]])).drop(*all_emotions)

# Create a new column "word" based on selected word on list all_words, then locate it on tweet text and save position
for word in all_words:
    df_symbols = df_symbols.withColumn(word, instr(df_symbols.tweet_text, word))

# Create a new columns based on checking least position from columns assigned to symbols ignoring null and 0 values, then drop all columns assigined to words
df_Words = df_symbols.withColumn("Words", least(*[
    when(col(c).isNull() | (col(c) == 0), None).otherwise(c)
        for c in df_symbols.columns[4:]])).drop(*all_words)


# Substitute all null values to "None" don't know how to set up a better way to do this
df_Words = df_Words.fillna("None", subset=None)
df_Words = df_Words.withColumn("Words", when(col("Symbol").contains("None"), df_Words.Words).otherwise("None"))

# Create a new dataframe classifying if a tweet is positive or negative based on columns Symbol and Words, otherwise classifies Neutral 
df_fullTable = df_Words.withColumn("Emotion", when(df_Words.Symbol.isin(emotions['positive']) | df_Words.Words.isin(classifications['positive']), "Positive")\
                                  .when(df_Words.Symbol.isin(emotions['negative']) | df_Words.Words.isin(classifications['negative']), "Negative")\
                                  .otherwise("Neutral"))

# Create a collumn to refer based on year, month and day for each tweet
df_final = df_fullTable.withColumn("year", date_format(col("tweet_date"), "yyyy"))\
                  .withColumn("month", date_format(col("tweet_date"), "MM"))\
                  .withColumn("day", date_format(col("tweet_date"), "dd"))


# Create files particioned by year,month and day based on AWS recommendations
 df_final.write.partitionBy("year", "month", "day")\
    .parquet("s3://xpto-ref-davi-sena/twitter/sa-east-1/TWITTER_ELEICOES/", mode="append")
