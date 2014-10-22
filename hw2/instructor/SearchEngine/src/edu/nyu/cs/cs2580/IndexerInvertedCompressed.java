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
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Vector;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.nyu.cs.cs2580.Compress.Compression;
import edu.nyu.cs.cs2580.Compress.DeltaCompression;
import edu.nyu.cs.cs2580.Compress.GammaCompression;
import edu.nyu.cs.cs2580.SkipPointer.SkipPointer;
import edu.nyu.cs.cs2580.SearchEngine.Options;


@SuppressWarnings("unused")
public class IndexerInvertedCompressed extends Indexer implements Serializable{
  
  /**
	 * 
	 */
  private static final long serialVersionUID = 2382040574800063070L;
  
  public class PostingList implements Serializable
  {
    /**
	 * 
	 */
	private static final long serialVersionUID = -3373610604746592865L;
	/**
     * 
     */
    
    private int count;
    private BitSet bits;
    private Compression compress;
    private int corpusDocFrequency;
    private int corpusTermFrequency;
    
    public BitSet getBits() {
		return bits;
	}

	public void setBits(BitSet bits) {
		this.bits = bits;
	}

    public PostingList()
    {
      count = 0;
      bits = new BitSet();
      compress = new GammaCompression();
    }

    public int getCount() {
      return count;
    }
    
    public void setCount(int count) {
      if(count < this.count)
    	  System.out.println(count);
      
      this.count = count;
    }   
    
    public void add(int x)
    {
    	setCount(compress.compress(x, getBits(), count));
    }
    
    public void add(List<Integer> list)
    {
    	for(Integer i: list)
    		setCount(compress.compress(i, getBits(), count));
    }
    
    public int[] get(int pos)
    {
    	try {
    		return compress.deCompress(getBits(), getCount(), pos);
		} catch (Exception e) {
			// TODO: handle exception
			return new int[]{-1,-1};
		}    	
    }
	
    public int getCorpusDocFrequency() {
		return corpusDocFrequency;
	}
	
    public void setCorpusDocFrequency(int corpusDocFrequency) {
		this.corpusDocFrequency = corpusDocFrequency;
	}
	
    public int getCorpusTermFrequency() {
		return corpusTermFrequency;
	}
	
    public void setCorpusTermFrequency(int corpusTermFrequency) {
		this.corpusTermFrequency = corpusTermFrequency;
	}
	
	public void increaseCorpusTermFreqency()
	{
		this.corpusTermFrequency++;
	}
	
	public void increaseCorpusDocFreqency()
	{
		this.corpusDocFrequency++;
	}
  
  }

  private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
  private HashMap<String,PostingList> index = new HashMap<String,PostingList>();
  private HashMap<String,SkipPointer> skipPointerMap;
  private long totalWordsInCorpus = 0;
  private int skipSteps = 100;
  private HashMap<String, Integer> urlToDocId = new HashMap<String, Integer>();
  public IndexerInvertedCompressed(Options options) {
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
	    HashMap<String,Integer> lastDocInserted = new HashMap<String,Integer>();
	    for (final File fileEntry : corpusDirectory.listFiles()) {
	      if (!fileEntry.isDirectory()) {
	    	String url = fileEntry.getAbsolutePath();
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
	        processDocument(docTitle , sb.toString(), url,posInPostingList,skipNumberList,lastDocInserted);        
	      }
	    }    
  }
  
  public void noParse() throws IOException
  {
	  String corpusFile = _options._corpusPrefix + "/corpus.tsv";
	    System.out.println("Construct index from: " + corpusFile);
	    HashMap<String,Integer> skipNumberList = new HashMap<String,Integer>();
	    HashMap<String,Integer> posInPostingList = new HashMap<String,Integer>();
	    HashMap<String,Integer> lastDocInserted = new HashMap<String,Integer>();
	    BufferedReader reader = new BufferedReader(new FileReader(corpusFile));
	    try {
	      String line = null;
	      while ((line = reader.readLine()) != null) {
	        processDocument(line,posInPostingList,skipNumberList, lastDocInserted);
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
  
  private void processDocument(String title,
		  String content, String url,
			HashMap<String,Integer> posInPostingList, 
			HashMap<String,Integer> skipNumberList , 
			HashMap<String,Integer> lastDocInserted) 
  {
    HashMap<String, List<Integer>> tokens = new HashMap<String, List<Integer>>();
    double normfactor = 0; 
    int totalwords = readTermVector(title + " " + content, tokens);
    int docid = _documents.size();
    updateIndex(tokens,docid,posInPostingList,skipNumberList,lastDocInserted);   

    for(String token: tokens.keySet()) {
      int x = tokens.get(token).get(0);
      normfactor += x*x;
      index.get(token).increaseCorpusDocFreqency();
    }
    
    DocumentIndexed doc = new DocumentIndexed(_documents.size());
    doc.setTitle(title);
    doc._normfactor = Math.sqrt(normfactor);
    doc._numwords = totalwords;
    doc.setUrl(url);
    urlToDocId.put(url, _documents.size());
    _documents.add(doc);
    ++_numDocs;
  }
  
  private void processDocument(String content, 
      HashMap<String,Integer> posInPostingList, 
      HashMap<String,Integer> skipNumberList,
      HashMap<String,Integer> lastDocInserted) 
  {
    @SuppressWarnings("resource")
	Scanner s = new Scanner(content.toLowerCase()).useDelimiter("\t");

    String title = s.next();
    HashMap<String, List<Integer>> tokens = new HashMap<String, List<Integer>>();
    
    double normfactor = 0; 
    int totalwords = readTermVector(title + "  " + s.next(), tokens);
    int docid = _documents.size();
    updateIndex(tokens,docid,posInPostingList,skipNumberList,lastDocInserted);   

    int numViews = Integer.parseInt(s.next());
    s.close();
    
    for(String token: tokens.keySet()) {
      int x = tokens.get(token).get(0);
      normfactor += x*x;
      index.get(token).increaseCorpusDocFreqency();     
    }
    
    DocumentIndexed doc = new DocumentIndexed(_documents.size());
    doc.setTitle(title);
    doc._normfactor = Math.sqrt(normfactor);
    doc._numwords = totalwords;
    _documents.add(doc);
    ++_numDocs;    
  }
  
  private int readTermVector(String content, HashMap<String, List<Integer>> tokens) 
  {
    @SuppressWarnings("resource")
	Scanner s = new Scanner(content);  // Uses white space by default.
    int wordcount = 1;
    HashMap<String, Integer> lastOccurenceofWordinDoc = new HashMap<String, Integer>();
    while (s.hasNext()) {
      totalWordsInCorpus++;
      String word = s.next();
      if(index.containsKey(word))
    	  index.get(word).increaseCorpusTermFreqency();
      else
      {
    	  PostingList p = new PostingList();
    	  index.put(word, p);
    	  p.increaseCorpusTermFreqency();
      }
      if(tokens.containsKey(word)) {
        List<Integer> listOfCountAndPositions = tokens.get(word);
        listOfCountAndPositions.set(0,listOfCountAndPositions.get(0)+1);
        listOfCountAndPositions.add(wordcount - lastOccurenceofWordinDoc.get(word));
        lastOccurenceofWordinDoc.put(word, wordcount);
        tokens.put(word, listOfCountAndPositions);
      }
      else {
        List<Integer> listOfCountAndPositions = new ArrayList<Integer>();
        listOfCountAndPositions.add(1);
        listOfCountAndPositions.add(wordcount);
        lastOccurenceofWordinDoc.put(word, wordcount);
        
        tokens.put(word, listOfCountAndPositions);
      }
      wordcount++;
    }
    return wordcount-1;
  }
  
  private void updateIndex(
    HashMap<String,List<Integer>> tokens, int did,
    HashMap<String,Integer> posInPostingList,
    HashMap<String,Integer> skipNumberList,
    HashMap<String,Integer> lastDocInserted) {
    
    
    for(String word:tokens.keySet()) {
      
      List<Integer> postingList = tokens.get(word);
      PostingList p = null;
      if(index.containsKey(word)) {
        p = index.get(word);
      }
      else
      {
        p = new PostingList();
        index.put(word, p);
      }
      
      int lastDocId = 0;
      if(lastDocInserted.containsKey(word)) {
        lastDocId = lastDocInserted.get(word);
      }
      
      p.add(did - lastDocId);
      p.add(postingList);
      lastDocInserted.put(word, did);
   
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
          }
          skipPointer.addPointer(did, p.getCount());
        }
        else {
          skipNumberList.put(word, lastUpdated);
        }
      }
      else {
        skipNumberList.put(word, 0);
      }            
      posInPostingList.put(word, p.getCount());      
    }
    return;
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
	long x = (System.currentTimeMillis());
	String indexFile = _options._indexPrefix + "/" + _options._index_file;    
    System.out.println("Load index from: " + indexFile);

    ObjectInputStream reader =
        new ObjectInputStream(new FileInputStream(indexFile));
    IndexerInvertedCompressed loaded = (IndexerInvertedCompressed) reader.readObject();

    this._documents = loaded._documents;
    // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
    this._numDocs = _documents.size();
    this._totalTermFrequency = loaded._totalTermFrequency;
    this.skipPointerMap = loaded.skipPointerMap;
    this.skipSteps = loaded.skipSteps;
    this.totalWordsInCorpus = loaded.totalWordsInCorpus;
    this.index = loaded.index;
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
  public Document nextDocument(Query query, int docid) {
    DocumentIndexed [] docs = new DocumentIndexed[query._tokens.size()];
    int i = 0;
    boolean flag = true;
    int maxDocId = -1;
   
    for(String term:query._tokens) {
    	
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
  
  public DocumentIndexed nextPhrasePos(String term, int docid)
  {
    boolean flag = true;
    
    String terms[] = term.split(" ");
    while(flag)
    {
      boolean flag2 = true;
      int max = -1;

      DocumentIndexed docs[] = new DocumentIndexed[terms.length];
      for(int i = 0 ; i < terms.length; i++)
      {
        String t = terms[i];
        docs[i] = nextPosition(t, docid);
        if(docs[i] == null)
          return null;
        
        if(i != 0)
        {
          if(docs[i]._docid != docs[i-1]._docid)
          {
            flag2 = false;
          }
        }
        if(max < docs[i]._docid)
        {
          max = docs[i]._docid;
        }
      }
      if(flag2)
      {
        List<List<Integer>> l = new LinkedList<List<Integer>>();
        for(int i = 0; i < docs.length; i++)
        {
          l.add(docs[i].getDocumentDetails());
        }
        int pharasecount = 0;
        List<Integer> phrasedetails = new LinkedList<Integer>();
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
 
  private int getDocumentDetail(PostingList p, int pos, List<Integer> details) {
	  	if(pos == -1)
	  		return -2;
	  	int [] temp = p.get(pos);
	    int docid = temp[0];
	    temp = p.get(temp[1]);
	    int count = temp[0];
	    
	    details.add(docid);
	    details.add(count);
	    
	    int totval = 0;
	    for(int i=0;i<count;i++) {
	      temp = p.get(temp[1]);
	      totval += temp[0];
	      details.add(totval);
	    }
	    return temp[1];
  }
  
  private DocumentIndexed nextPosition(String term,int docid) {
    
    if(term.contains(" ")) {
      return nextPhrasePos(term, docid);
    }
    else {
      PostingList p = index.get(term);
      if(p == null)
      {
        return null;
      }
      
      
      int j = 0;
      List<Integer> details = new ArrayList<Integer>(); 
      j = getDocumentDetail(p,j,details);
      int offset = details.get(0);
      
      if(skipPointerMap.containsKey(term))
      {	SkipPointer.Pair pair = skipPointerMap.get(term).search(docid);
      	if(pair != null)
      	{
      		j = (int)pair.getPos();
      		offset = pair.getDocid();
      		if(offset > docid)
      		{
      			j = getDocumentDetail(p,j,details);
      			offset = details.get(0);
      		}
      	}
      }
      int currdocid = offset ;
      
      if(currdocid > docid) {
        DocumentIndexed d = new DocumentIndexed(currdocid);
        d.setDocumentDetails(details.subList(1, details.size()));
        return d;
      }

      while(j < p.getCount() && j != -2) {
        details.clear();
        j = getDocumentDetail(p, j, details);
        if(j == -2)
        	break;
        currdocid += details.get(0);
        if(currdocid > docid)
            break;
      }
      
      if(j >= p.getCount() || j == -2) {
        return null;
      }
      if(currdocid >= _documents.size())
      {
        return null;
      }
      DocumentIndexed d = new DocumentIndexed(currdocid);
      d.setDocumentDetails(details.subList(1, details.size()));
      return d;
    }
  }
  
 
  @Override
  public int corpusDocFrequencyByTerm(String term) {
	if(index.containsKey(term))
		return index.get(term).getCorpusDocFrequency();
	else
		return 0;
  }
  
  @Override
  public int corpusTermFrequency(String term) {
	if(index.containsKey(term))
		return index.get(term).getCorpusTermFrequency();
	else
		return 0;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    //SearchEngine.Check(false, "Not implemented!");
    int docid = urlToDocId.get(url);
    PostingList p = index.get(term);
    int currdocid = 0;
    int j = 0;
    if(skipPointerMap.containsKey(term))
    {
    	SkipPointer.Pair pair = skipPointerMap.get(term).search(docid);
    	currdocid = pair.getDocid();
    	j = (int )pair.getPos();
    	if(currdocid > docid)
    	{
    		int temp[] = p.get(0);
    		currdocid = temp[0];
    		j = temp[1];
    	}
    	
    }
    else
    {
    	return 0;
    }
    
    if(currdocid == docid)
    {	
    	if(j == -1)
    		return 0;
    	
    	int [] temp = p.get(j);
    	
    	if(temp == null)
    		return 0;
    	
    	if(temp[0] == -1)
    		return 0;
    	
    	return temp[0];
    }
    
    int count = 0;
    while(currdocid < docid)
  	{
    	if(j == -1)
    		return 0;
    	
    	int [] temp = p.get(j);
    	
    	if(temp == null)
    		return 0;
    	
    	if(temp[0] == -1)
    		return 0;
    	
    	count = temp[0];
    	
    	for(int i = 0; i < count; i++)
    	{
    		if(temp[1] == -1)
        		return 0;
    		temp = p.get(temp[1]);
    	}
    	
    	if(temp[1] == -1)
    		return 0;
		
    	temp = p.get(temp[1]);
    	
    	currdocid += temp[0];
    	j = temp[1];
    }
    
    if(currdocid == docid)
    { 	
    	if(j != -1)
    	{
    		int temp[] = p.get(j);
    		return temp[0];
    	}
    }
    return 0;
  }
  
  @Override
  public long getTotalWordsInCorpus() {
		// TODO Auto-generated method stub
		return totalWordsInCorpus;
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
  public Document nextDoc(Query query, int docid) {
    DocumentIndexed [] docs = new DocumentIndexed[query._tokens.size()];
    int i = 0;
    boolean flag = true;
    int maxDocId = -1;
   
    for(String term:query._tokens) {
    	
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
}
