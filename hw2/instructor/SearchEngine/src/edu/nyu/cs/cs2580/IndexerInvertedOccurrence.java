package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Vector;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.nyu.cs.cs2580.SkipPointer.*;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 7649255732151541345L;

  private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
  private HashMap<String,List<Integer>> index = new HashMap<String,List<Integer>>();
  private HashMap<String,Integer> corpusDocFrequency = new HashMap<String, Integer>();
  private HashMap<String,Integer> corpusTermFrequency = new HashMap<String, Integer>();
  private HashMap<String,SkipPointer> skipPointerMap;
  private int skipSteps;
  public long totalWordsInCorpus = 0;
  public IndexerInvertedOccurrence(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
    skipPointerMap = new HashMap<String,SkipPointer>();
  }

  public void parse() throws Exception {
	  
    String corpusDirectoryString = _options._corpusPrefix;
    System.out.println("Construct index from: " + corpusDirectoryString);
    final File corpusDirectory = new File(corpusDirectoryString);
    HashMap<String,Integer> skipNumberList = new HashMap<String,Integer>();
    HashMap<String,Integer> posInPostingList = new HashMap<String,Integer>();
    for (final File fileEntry : corpusDirectory.listFiles()) {
      if (!fileEntry.isDirectory()) {
        String docTitle = "";
        StringBuilder sb = new StringBuilder();
        org.jsoup.nodes.Document doc = Jsoup.parse(fileEntry, "UTF-8");

        Element head = doc.select("h1[id=firstHeading]").first();
        if(head != null && head.text() != null) {
          //System.out.println(head.text().trim());
          docTitle = head.text().trim();
          sb.append(docTitle.toLowerCase());
        }
        Elements content_text = doc.select("div[id=mw-content-text]");
        for (Element elem : content_text) {
          Elements paras = elem.getElementsByTag("p");
          Elements h2s = elem.getElementsByTag("h2");

          for (Element h2 : h2s) {
            //System.out.println(h2.getElementsByClass("mw-headline").first());
            Element headLine = h2.getElementsByClass("mw-headline").first();
            if(headLine != null) {
              sb.append(headLine.text().toLowerCase());
            }
          }
          for (Element para : paras) {
            //System.out.println(para.text().trim());
            if(para.text() != null) {
              sb.append(para.text().trim().toLowerCase());
            }
          }
        }
        sb.toString();
        processDocument(docTitle,sb.toString(),posInPostingList,skipNumberList);        
      }
    }    
  }

  public void noParse() throws IOException
  {
	  String corpusFile = _options._corpusPrefix + "/corpus.tsv";
	    System.out.println("Construct index from: " + corpusFile);
	    HashMap<String,Integer> skipNumberList = new HashMap<String,Integer>();
	    HashMap<String,Integer> posInPostingList = new HashMap<String,Integer>();
	    BufferedReader reader = new BufferedReader(new FileReader(corpusFile));
	    try {
	      String line = null;
	      while ((line = reader.readLine()) != null) {
	        processDocument(line,posInPostingList,skipNumberList);
	      }
	    } 
	    catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	    finally {
	      reader.close();
	    }
  }
  
  @Override
  public void constructIndex() throws IOException {
    this.skipSteps = _options.skips;
	if(_options._corpus.equals("parse"))
    {
    	try {
	      parse();
	    }
	    catch(Exception e) {
	      e.printStackTrace();
	    }
    }
    else
    {
    	try {
  	      noParse();
  	    }
  	    catch(Exception e) {
  	      e.printStackTrace();
  	    }
    }
    
    System.out.println(
        "Indexed " + Integer.toString(_numDocs) + " docs with " +
            Long.toString(_totalTermFrequency) + " terms.");

    String indexFile = _options._indexPrefix + "/" + _options._index_file;
    System.out.println("Store index to: " + indexFile);
    ObjectOutputStream writer =
        new ObjectOutputStream(new FileOutputStream(indexFile));
    writer.writeObject(this);
    writer.close();
    System.out.println("Index File Created!");
  }

  private void processDocument(String title, String content, HashMap<String,Integer> posInPostingList, HashMap<String,Integer> skipNumberList) 
  {
    HashMap<String, List<Integer>> tokens = new HashMap<String, List<Integer>>();
    double normfactor = 0; 
    int totalwords = readTermVector(title + " " + content, tokens);
    int docid = _documents.size();
    updateIndex(tokens,docid,posInPostingList,skipNumberList);   

    for(String token: tokens.keySet()) {
      int x = tokens.get(token).get(0);
      normfactor += x*x;
    }

    DocumentIndexed doc = new DocumentIndexed(_documents.size());
    doc.setTitle(title);
    doc._normfactor = Math.sqrt(normfactor);
    doc._numwords = totalwords;
    _documents.add(doc);
    //totalWordsInCorpus += totalwords;
    ++_numDocs;
  }

  private void processDocument(String content, HashMap<String,Integer> posInPostingList, HashMap<String,Integer> skipNumberList) 
  {
    @SuppressWarnings("resource")
	Scanner s = new Scanner(content).useDelimiter("\t");
    
    String title = s.next();
    HashMap<String, List<Integer>> tokens = new HashMap<String, List<Integer>>();
    double normfactor = 0; 
    int totalwords = readTermVector(title + " " + s.next(), tokens);
    int docid = _documents.size();
    //readTermVector(s.next(),tokens);
    updateIndex(tokens,docid,posInPostingList,skipNumberList);   

    int numViews = Integer.parseInt(s.next());
    s.close();

    for(String token: tokens.keySet()) {
      int x = tokens.get(token).get(0);
      if(corpusDocFrequency.containsKey(token))
      {
    	  corpusDocFrequency.put(token, corpusDocFrequency.get(token) + 1);
      }
      else    	  
      {
    	  corpusDocFrequency.put(token, 1);
      }
      normfactor += x*x;
    }

    DocumentIndexed doc = new DocumentIndexed(_documents.size());
    doc.setTitle(title);
    doc._normfactor = Math.sqrt(normfactor);
    doc.setNumViews(numViews);
    doc._numwords = totalwords;
    _documents.add(doc);
    //totalWordsInCorpus += totalwords;
    ++_numDocs;
  }

  private int readTermVector(String content, HashMap<String, List<Integer>> tokens) {
    @SuppressWarnings("resource")
	Scanner s = new Scanner(content);  // Uses white space by default.
    int wordcount = 1;
    
    while (s.hasNext()) {
      String word = s.next();
      totalWordsInCorpus ++;
      if(corpusTermFrequency.containsKey(word))
      {
    	  corpusTermFrequency.put(word, corpusTermFrequency.get(word) + 1);
      }
      else
      {
    	  corpusTermFrequency.put(word, 1);
      }
      if(tokens.containsKey(word)) {
        List<Integer> listOfCountAndPositions = tokens.get(word);
        listOfCountAndPositions.set(0,listOfCountAndPositions.get(0)+1);
        listOfCountAndPositions.add(wordcount);
        tokens.put(word, listOfCountAndPositions);
      }
      else {
        List<Integer> listOfCountAndPositions = new ArrayList<Integer>();
        listOfCountAndPositions.add(1);
        listOfCountAndPositions.add(wordcount);
        tokens.put(word, listOfCountAndPositions);
      }
      wordcount++;
    }
    
    return wordcount-1;
    
  }

  private void updateIndex(
      HashMap<String,List<Integer>> tokens, int did,
      HashMap<String,Integer> posInPostingList,
      HashMap<String,Integer> skipNumberList) {
    
	  for(String word:tokens.keySet()) {
      List<Integer> postingList = tokens.get(word);
      List<Integer> indexPostingList = null;
      if(index.containsKey(word)) {
        indexPostingList = index.get(word);
      }
      else {
        indexPostingList = new ArrayList<Integer>();
      }
      indexPostingList.add(did);
      indexPostingList.addAll(postingList);
      posInPostingList.put(word, indexPostingList.size());
      index.put(word,indexPostingList);
     
      if(skipNumberList.containsKey(word)) {
          int lastUpdated = skipNumberList.get(word);
          lastUpdated++;
          if(lastUpdated == skipSteps) {
            skipNumberList.put(word, 0);
            SkipPointer skipPointer = null;
            if(skipPointerMap.containsKey(word)) {
              skipPointer = skipPointerMap.get(word);
            }
            else {
              skipPointer = new SkipPointer();
              skipPointerMap.put(word, skipPointer);
            }
            skipPointer.addPointer(did, posInPostingList.get(word));
            skipPointerMap.put(word, skipPointer);
          }
          else {
            skipNumberList.put(word, lastUpdated);
          }
      }
      else {
          skipNumberList.put(word, 0);
      }
    }
    return;
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
	long x = (System.currentTimeMillis());
	String indexFile = _options._indexPrefix + "/" + _options._index_file;
    System.out.println("Load index from: " + indexFile);
    this.skipSteps = _options.skips;
    ObjectInputStream reader =
        new ObjectInputStream(new FileInputStream(indexFile));
    IndexerInvertedOccurrence loaded = (IndexerInvertedOccurrence) reader.readObject();

    this._documents = loaded._documents;
    // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
    this._numDocs = _documents.size();
    this.index = loaded.index;
    this._totalTermFrequency = loaded._totalTermFrequency;
    this.skipPointerMap = loaded.skipPointerMap;
   
    this.totalWordsInCorpus = loaded.totalWordsInCorpus;
    this.corpusDocFrequency = loaded.corpusDocFrequency;
    this.corpusTermFrequency = loaded.corpusTermFrequency;
    reader.close();

    System.out.println(Integer.toString(_numDocs) + " documents loaded " +
        "with " + Long.toString(_totalTermFrequency) + " terms!");
    x = System.currentTimeMillis() - x;
    System.out.println((x/1000/60.0)+ " mins to load...");
  }

  @Override
  public Document getDoc(int docid) {
    return _documents.get(docid);
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}
   */
  
  @Override
  public Document nextDoc(Query query, int docid) {
	  DocumentIndexed [] docs = new DocumentIndexed[query._tokens.size()];
	    
	    int i = 0;
	    boolean flag = true;
	    int maxDocId = -1;
	    List<List<Integer>> postingLists = new ArrayList<List<Integer>>();
	    for(String term:query._tokens) {
	      postingLists.add(index.get(term));
	      docs[i] = nextPosition(term,docid);
	      if(docs[i] == null) {
	        return null;
	      }
	      if(i != 0) {
	        if(docs[i]._docid != docs[i-1]._docid)
	        {
	          flag = false;
	        }
	      }
	      if(maxDocId < docs[i]._docid) {
	        maxDocId = docs[i]._docid;
	      }
	      i++;
	    }
	    if(flag) {
	      //this doc contains all the terms!!
	      //all the query terms map to the same doc id
	      DocumentIndexed d1 = (DocumentIndexed) _documents.get(maxDocId);
	      DocumentIndexed d = new DocumentIndexed(d1._docid);
	      d.setTitle(d1.getTitle());
	      d.setUrl(d1.getTitle());
	      d.setPageRank(d1.getPageRank());
	      d.setNumViews(d1.getNumViews());
	      List<Integer> details = new ArrayList<Integer>();
	      for(int j = 0; j < docs.length; j++)
	      {
	        details.addAll(docs[j].getDocumentDetails());
	      }
	      d.setDocumentDetails(details);
	      d._normfactor = d1._normfactor;
	      d._numwords = d1._numwords;
	      return d;
	    }
	    return nextDoc(query, maxDocId-1);
  }

  @Override
  public Document nextDocument(Query query, int docid) {
    DocumentIndexed [] docs = new DocumentIndexed[query._tokens.size()];
    
    int i = 0;
    boolean flag = true;
    int maxDocId = -1;
    List<List<Integer>> postingLists = new ArrayList<List<Integer>>();
    for(String term:query._tokens) {
      postingLists.add(index.get(term));
      docs[i] = nextPosition(term,docid);
      if(docs[i] == null) {
        return null;
      }
      if(i != 0) {
        if(docs[i]._docid != docs[i-1]._docid)
        {
          flag = false;
        }
      }
      if(maxDocId < docs[i]._docid) {
        maxDocId = docs[i]._docid;
      }
      i++;
    }
    if(flag) {
      //this doc contains all the terms!!
      //all the query terms map to the same doc id
      DocumentIndexed d1 = (DocumentIndexed) _documents.get(maxDocId);
      DocumentIndexed d = new DocumentIndexed(d1._docid);
      d.setTitle(d1.getTitle());
      d.setUrl(d1.getTitle());
      d.setPageRank(d1.getPageRank());
      d.setNumViews(d1.getNumViews());
      List<Integer> details = new ArrayList<Integer>();
      for(int j = 0; j < docs.length; j++)
      {
        details.addAll(docs[j].getDocumentDetails());
      }
      d.setDocumentDetails(details);
      d._normfactor = d1._normfactor;
      d._numwords = d1._numwords;
      return d;
    }
    return nextDocument(query, maxDocId-1);
  }

  private List<List<Integer>> getDocumentDetails(String tokens[], int docpos[]) {
    List<List<Integer>> docDetails = new ArrayList<List<Integer>>();
    for(int j = 0 ; j < tokens.length; j++)
    {
      List<Integer> postingList = index.get(tokens[j]);
      int afterNextP = getNextDocPos(postingList, docpos[j]);
      List<Integer> pl = new ArrayList<Integer>();
      docDetails.add(pl);
      for(int i=docpos[j]+2; i<afterNextP; i++) {
        pl.add(postingList.get(i));
      }
    }
    return docDetails;
  }

  private int getNextDocPos(List<Integer> postingList,int pos) {
    if(pos >= postingList.size()-1) {
      return -1;
    }
    return (pos+2+postingList.get(pos+1));
  }

  public DocumentIndexed nextPhrasePos(String term, int docid)
  {
    boolean flag = true;

    String terms[] = term.split(" ");
    while(flag)
    {
      boolean flag2 = true;
      int max = -1;
      int docidpos[] = new int[terms.length];
      int docids[] = new int[terms.length];
      for(int i = 0 ; i < terms.length; i++)
      {
        String t = terms[i];

        docidpos[i] = nextPos(t, docid);
        if(docidpos[i] == -1)
          return null;

        docids[i] = index.get(t).get(docidpos[i]);
        if(i != 0)
        {
          if(docids[i] != docids[i-1])
          {
            flag2 = false;
          }
        }
        if(max < docids[i])
        {
          max = docids[i];
        }
      }
      if(flag2)
      {
        List<List<Integer>> l = getDocumentDetails(terms, docidpos);
        int pharasecount = 0;
        List<Integer> phrasedetails = new LinkedList<Integer>();
        //phrasedetails.add(0);
        List<Integer> firstterm = l.get(0);
        for(int i = 0; i < firstterm.size(); i++)
        {
          int firstpos = firstterm.get(i);
          boolean flag3 = true;
          for(int j = 1; j < l.size(); j++)
          {
            if(!l.get(j).contains(firstpos + j))
            {
              flag3 = false;
              break;
            }
          }
          if(flag3)
          {
            phrasedetails.add(firstpos);
            pharasecount++;
          }
        }
        if(pharasecount > 0)
        {
          flag = false;
          phrasedetails.add(0, pharasecount);
          DocumentIndexed d = new DocumentIndexed(max);
          d.setDocumentDetails(phrasedetails);
          return d;
        }
        else
        {
          docid = max;
        }
      }
      else
      {
        docid = max - 1;
      }

    }
    return null;
  }

  /*
   * Returns position of doc id location in the posting list
   * which is greater than the docid passed
   */
  private int nextPos(String term,int docid) {

    if(term.contains(" ")) {
      return 0;
    }
    else {
      List<Integer> postingList = index.get(term);
      if(postingList == null) {
        return -1;
      }
      int pos = 0;//(int) skipPointerMap.get(term).search(docid);
      if(postingList.get(pos) > docid) {
        return pos;
      }
      while(((pos = getNextDocPos(postingList, pos)) != -1) 
          && pos < postingList.size()
          && (postingList.get(pos) <= docid)) {
        ;
      }
      if(pos == postingList.size()) {
        return -1;
      }
      return pos;
    }
  }

  private DocumentIndexed nextPosition(String term,int docid) {

    if(term.contains(" ")) {
      return nextPhrasePos(term, docid);
    }
    else {
      List<Integer> postingList = index.get(term);
      if(postingList == null) {
        return null;
      }
      int pos = 0;
      int currdocid = postingList.get(pos);
      if(skipPointerMap.containsKey(term))
      {
    	  SkipPointer.Pair p = skipPointerMap.get(term).search(docid);
    	  pos = (int) p.getPos();
    	  currdocid = p.getDocid();
    	  if(currdocid > docid || docid == -1)
    	  {
    		  pos = 0;
    	      currdocid = postingList.get(pos);
    	  }
      }
      
      if(currdocid > docid) {
        DocumentIndexed d = new DocumentIndexed(currdocid);
        _documents.get(d._docid);
        List<Integer> docDetails = new ArrayList<Integer>();
        int nextP = getNextDocPos(postingList, pos);
        for(int i = pos + 1 ; i < nextP ; i++) {
          docDetails.add(postingList.get(i));
        }
        d.setDocumentDetails(docDetails);
        return d;
      }
      
      while(currdocid <= docid)
      {
    	  pos = getNextDocPos(postingList, pos);
    	  if(pos == -1 || pos >= postingList.size())
    		  return null;
    	  else
    		  currdocid = postingList.get(pos);
      }
      
      DocumentIndexed d = new DocumentIndexed(currdocid);
      _documents.get(d._docid);
      List<Integer> docDetails = new ArrayList<Integer>();
      int nextP = getNextDocPos(postingList, pos);
      for(int i = pos+1 ; i < nextP ; i ++) {
        docDetails.add(postingList.get(i));
      }
      d.setDocumentDetails(docDetails);
      return d;
    }
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    return corpusDocFrequency.get(term);
    
  }

  public int corpusTermFrequency() {
    return index.keySet().size();
  }

  public int corpusPhraseFrequency(String term)
  {
    Document doc = null;
    int totalphrases=0;
    String quotedterm = "\"" + term + "\"";
    Query query = new QueryPhrase(quotedterm);
    query.processQuery();
    int docid = -1;
    while ((doc = nextDocument(query, docid)) != null) {
      DocumentIndexed temp = (DocumentIndexed)doc;
      List<Integer> postingList= temp.getDocumentDetails();
      totalphrases += postingList.get(0);
      docid = doc._docid;
    }
    return totalphrases;
  }

  @Override
  public int corpusTermFrequency(String term) {
    if(term.contains(" "))
      return corpusPhraseFrequency(term);
//    List<Integer> postingList = index.get(term);
//    if(postingList == null || (postingList.size() == 0)) {
//      return 0;
//    }
//    int pos = 0;
//    int count = postingList.get(1);
//    while((pos = getNextDocPos(postingList, pos)) != -1) {
//      count += postingList.get(pos+1);
//    }
//    
//    return count;
	  return corpusTermFrequency.get(term);
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    SearchEngine.Check(false, "Not implemented!");
    return 0;
  }

  public long getTotalPhrasesCorpus(int tokenwordcount)
  {
    long numtokenscorpus=0;
    for(int i=0 ; i<_documents.size() ; i++)
    {
      Document d = _documents.get(i);
      if(d._numwords >= tokenwordcount)
        numtokenscorpus += d._numwords - (tokenwordcount - 1);
    }
    return numtokenscorpus;
  }

  @Override
  public long getTotalWordsInCorpus() {
	// TODO Auto-generated method stub
	return totalWordsInCorpus;
}

}
