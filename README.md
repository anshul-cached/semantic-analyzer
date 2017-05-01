# semantic-analyzer

Spark Semantic Analyzer Example
===============================

A few lines of code to demo how similarity of sentences can be checked base on their semantics works with Spark, in particular using [Word Mover's Distance] (http://jmlr.org/proceedings/papers/v37/kusnerb15.pdf) algorithm by Kusner.


To submit the job to an existing Spark installation you can package the job with the following command:

    sbt package

and then submit it with the following command:

    $SPARK_HOME/bin/spark-submit \
      --master $SPARK_MASTER \
      --jars $DEPENDENCIES \
      --class sentenceAnalyzer.SentenceSimilariyCheck \
      target/scala-2.10/sentence-similarity-analyzer-using-wmd_2.10-1.0.0.jar input.txt output.txt stopwords.txt googlew2v.tsv

<h4>Note</h4>I have converted the [google-news-word2vec-bin] (https://code.google.com/archive/p/word2vec/) file to tsv using genism package available for python.
      
After running the `sbt package` command you'll find the required JARs in your local Ivy cache (`$HOME/.ivy2/cache/`)

You can understand the code in detail on [learningfrombigdata] (http://learningfrombigdata.com)