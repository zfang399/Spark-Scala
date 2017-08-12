import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._

def createNLPPipeline(): StanfordCoreNLP={
	val props = new Properties()
	props.put("annotators","tokenize, ssplit, pos, lemma")
	new StanfordCoreNLP(props)
}

def isOnlyLetters(str: String): Boolean={
	str.forall(c => Character.isLetter(c))
}

def plainTexttoLemmas(text: String, stopWords: Set[String], pipleline:StanfordCoreNLP): Seq[String]={
	val doc=new Annotation(text)
	pipeline.annotate(doc)

	val lemmas=new ArrayBuffer[String]()
	val sentences=doc.get(classOf[SentencesAnnotation])
	for(sentence<-sentences;
		token<-sentence.get(classOf[SentencesAnnotation])){
		val lemma=token.get(classOf[LemmaAnnotation])
		if (lemma.length>2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)){
			lemmas+=lemma.toLowerCase
		}
	}
	lemmas
}

val stopWords=sc.broadcast(
	scala.io.Source.fromFIle("stopwords.txt").getLines().toSet).value
val lemmatized:RDD[Seq[String]]=plainText.mapPartitions(it=>{
	val pipeline=createNLPPipeline()
	it.map{
		case(title,contents)=>
		plainTexttoLemmas(contents,stopWords,pipeline)
	}
})