package sentenceAnalyzer


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
object SentenceSimilariyCheck {
	def main(args:Array[String])={
		
	val sparkConfiguration = new SparkConf().setAppName("sentence-similarity-analyzer").setMaster(sys.env.get("spark.master").getOrElse("local[*]"))
	val sc = new SparkContext(sparkConfiguration)
	val sqlContext=new SQLContext(sc)
	import sqlContext.implicits._
	val Array(inputFile,outputFile)=Array(args(0),args(1))
 	val stopwords = sc.textFile("/stopwords.txt").collect.toSet
    
    val bStopwords = sc.broadcast(stopwords)
    
    val phraseTupple= sc.textFile(inputFile).map(line => {
            val Array(q1, q2, _) = line.split('\t')
            (q1, q2)
        }).zipWithIndex
    phraseTupple.cache
    phraseTupple.count
    
	import breeze.linalg._

 	def getWordPairs(id: Long, p1: String, p2: String, stopwords: Set[String]) = {
    	val w1p = p1.toLowerCase.replaceAll("\\p{Punct}", "").split(" ").filter(w => !stopwords.contains(w))
    	val w2p = p2.toLowerCase.replaceAll("\\p{Punct}", "").split(" ").filter(w => !stopwords.contains(w))
    	val wTupple = for (w1 <- w1p; w2 <- w2p) yield (id, (w1, w2))
    	wTupple.toList
	}

    val wordPairs = phraseTupple.flatMap(x=>getWordPairs(x._2, x._1._1, x._1._2, bStopwords.value))
    wordPairs.count()
    
    val w2v = sc.textFile("/GoogleNews-vectors-negative300.tsv")map(line => {
            val Array(word, vector) = line.split('\t')
            (word, vector)
        })
    
     
    w2v.cache
    w2v.count
    
    def distance(lvec: String, rvec: String): Double = {
    	val l = DenseVector(lvec.split(',').map(_.toDouble))
    	val r = DenseVector(rvec.split(',').map(_.toDouble))
    	math.sqrt(sum((l - r) :* (l - r)))
	}

    val wordVectors = wordPairs.map({case (idx, (lword, rword)) => (rword, (idx, lword))})
        .join(w2v)   
        .map({case (rword, ((idx, lword), rvec)) => (lword, (idx, rvec))})
        .join(w2v)   
        .map({case (lword, ((idx, rvec), lvec)) => ((idx, lword), (lvec, rvec))})
        .map({case ((idx, lword), (lvec, rvec)) => ((idx, lword), List(distance(lvec, rvec)))}) 
    
    
    val bestWMDs = wordVectors.reduceByKey((a, b) => a ++ b)
        .mapValues(distances => distances.sortWith(_ < _).head)  
        .map({case ((idx, lword), wmd) => (idx, wmd)})
        .reduceByKey((a, b) => a + b)                    
    
    // case class PhrasePair(q1: String, q2: String, wmd: Double)
    val results = phraseTupple.map(_.swap).join(bestWMDs).map({case (id, ((s1, s2), wmd)) => (s1, s2, wmd)}).toDF("q1","q2","wmd")
    // val resultsDF = sqlContext.createDataFrame(results)
    results.write.format("csv").save(outputFile)


}
   


}