package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Vector;
import java.util.Set;
import edu.nyu.cs.cs2580.SkipPointer.*;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer {
  
  private Vector<DocumentIndexed> _documents = new Vector<DocumentIndexed>();
  private HashMap<String,List<Integer>> index = new HashMap<String,List<Integer>>();
  private SkipPointer skipPointer;
  private int skipSteps = 5;

  public IndexerInvertedOccurrence(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
    skipPointer = new SkipPointer();
  }

  @Override
  public void constructIndex() throws IOException {
    String corpusFile = _options._corpusPrefix + "/corpus.tsv";
    System.out.println("Construct index from: " + corpusFile);
    HashMap<String,Integer> posInPostingList = new HashMap<String,Integer>();
    BufferedReader reader = new BufferedReader(new FileReader(corpusFile));
    try {
      String line = null;
      while ((line = reader.readLine()) != null) {
        processDocument(line,posInPostingList);
      }
    } finally {
      reader.close();
    }
    System.out.println(
        "Indexed " + Integer.toString(_numDocs) + " docs with " +
        Long.toString(_totalTermFrequency) + " terms.");

    String indexFile = _options._indexPrefix + "/corpus.idx";
    System.out.println("Store index to: " + indexFile);
    ObjectOutputStream writer =
        new ObjectOutputStream(new FileOutputStream(indexFile));
    writer.writeObject(this);
    writer.close();
  }
  
  private void processDocument(String content,HashMap<String,Integer> posInPostingList) {
    Scanner s = new Scanner(content).useDelimiter("\t");

    String title = s.next();
    HashMap<String, List<Integer>> tokens = new HashMap<String, List<Integer>>();
    readTermVector(title, tokens);
    int docid = _documents.size();
    readTermVector(s.next(),tokens);
    updateIndex(tokens,docid,posInPostingList);   

    int numViews = Integer.parseInt(s.next());
    s.close();

    DocumentIndexed doc = new DocumentIndexed(_documents.size());
    doc.setTitle(title);
    doc.setNumViews(numViews);
    _documents.add(doc);
    ++_numDocs;

    
  }
  
  private void readTermVector(String content, HashMap<String, List<Integer>> tokens) {
    Scanner s = new Scanner(content);  // Uses white space by default.
    int wordcount = 1;
    while (s.hasNext()) {
      String word = s.next();
      if(tokens.containsKey(word)) {
        List<Integer> listOfCountAndPositions = tokens.get(word);
        listOfCountAndPositions.set(0,listOfCountAndPositions.get(0)+1);
        //add delta for compression
        //for occurrence we don't need to use this
        //listOfCountAndPositions.add(
        //    wordcount - listOfCountAndPositions.get(listOfCountAndPositions.size()-1));
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
    return;
  }
  
  private void updateIndex(HashMap<String,List<Integer>> tokens, int did,HashMap<String,Integer> posInPostingList) {
    for(String word:tokens.keySet()) {
      if(posInPostingList.containsKey(word)) {
        int lastUpdated = posInPostingList.get(word);
        lastUpdated++;
        if(lastUpdated == 5) {
          posInPostingList.put(word, 0);
          skipPointer.addPointer(did, pos);
        }
        else {
          posInPostingList.put(word, lastUpdated);
        }
      }
      List<Integer> postingList = tokens.get(word);
      for(int a:tokens.get(word)) {
        postingList.add(did,0);
        index.put(word, postingList);
      }
    }
    return;
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    String indexFile = _options._indexPrefix + "/corpus.idx";
    System.out.println("Load index from: " + indexFile);

    ObjectInputStream reader =
        new ObjectInputStream(new FileInputStream(indexFile));
    IndexerInvertedOccurrence loaded = (IndexerInvertedOccurrence) reader.readObject();

    this._documents = loaded._documents;
    // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
    this._numDocs = _documents.size();
    this.index = loaded.index;
    reader.close();

    System.out.println(Integer.toString(_numDocs) + " documents loaded " +
        "with " + Long.toString(_totalTermFrequency) + " terms!");
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
    int [] docids = new int[query._tokens.size()];
    int i = 0;
    boolean flag = true;
    int maxDocId = -1;
    for(String term:query._tokens) {
      docids[i] = next(term,docid);
      if(docids[i] == -1) {
        return null;
      }
      if(i != 0) {
        if(docids[i-1] != docids[i]) {
          flag = false;
        }
      }
      if(maxDocId < docids[i]) {
        maxDocId = docids[i];
      }
      i++;
    }
    if(flag) {
      //this doc contains all the terms!!
      //all the query terms map to the same doc id
      DocumentIndexed d = _documents.get(docids[0]);
      d.setDocumentDetails(getDocumentDetails(query,docid));
      return d;
    }
    return nextDoc(query, maxDocId-1);
  }
  
  @Override
  public Document nextDocument(Query query, int docid) {
    int [] docIdPositions = new int[query._tokens.size()];
    int i = 0;
    boolean flag = true;
    int maxDocId = -1;
    List<List<Integer>> postingLists = new ArrayList<List<Integer>>();
    for(String term:query._tokens) {
      postingLists.add(index.get(term));
      docIdPositions[i] = nextPos(term,docid);
      if(docIdPositions[i] == -1) {
        return null;
      }
      if(i != 0) {
        if(postingLists.get(i-1).get(docIdPositions[i-1]) != 
            postingLists.get(i).get(docIdPositions[i])) {
          flag = false;
        }
      }
      if(maxDocId < postingLists.get(i).get(docIdPositions[i])) {
        maxDocId = postingLists.get(i).get(docIdPositions[i]);
      }
      i++;
    }
    if(flag) {
      //this doc contains all the terms!!
      //all the query terms map to the same doc id
      DocumentIndexed d = (DocumentIndexed) _documents.get(postingLists.get(0).get(docIdPositions[0]));
      d.setDocumentDetails(getDocumentDetails(query,docIdPositions));
      return d;
    }
    return nextDoc(query, maxDocId-1);
  }
  
  private List<Integer> getDocumentDetails(Query query, int docid) {
    List<Integer> docDetails = new ArrayList<Integer>();
    for(String term:query._tokens) {
      List<Integer> postingList = index.get(term);
      int nextP = nextPos(term, docid);
      int afterNextP = getNextDocPos(postingList, nextP);
      for(int i=nextP+1;i<afterNextP;i++) {
        docDetails.add(postingList.get(i));
      }
    }
    return docDetails;
  }
  
  private List<Integer> getDocumentDetails(Query query, int[] docIdPositions) {
    List<Integer> docDetails = new ArrayList<Integer>();
    int j=0;
    for(String term:query._tokens) {
      List<Integer> postingList = index.get(term);
      int afterNextP = getNextDocPos(postingList, docIdPositions[j]);
      for(int i=docIdPositions[j]+1;i<afterNextP;i++) {
        docDetails.add(postingList.get(i));
      }
      j++;
    }
    return docDetails;
  }
  
  private int getNextDocPos(List<Integer> postingList,int pos) {
    if(pos >= postingList.size()-1) {
      return -1;
    }
    return (pos+1+postingList.get(pos+1));
  }
  
  private int nextPos(String term,int docid) {
    List<Integer> postingList = index.get(term);
    if(postingList == null) {
      return -1;
    }
    int pos = 0;
    if(postingList.get(pos) > docid) {
      return pos;
    }
    while(((pos = getNextDocPos(postingList, pos)) != -1) 
        && (postingList.get(pos) <= docid)) {
      if(postingList.get(pos) == docid) {
        return getNextDocPos(postingList, pos);
      }
    }
    if(pos != -1) {
      return pos;
    }
    else {
      return -1;
    }
  }
  
  //This method does a linear search.
  //Binary search with skip pointers to be implemented.
  private int next(String term,int docid) {
    List<Integer> postingList = index.get(term);
    int nextP = nextPos(term, docid);
    if(nextP == -1) {
      return -1;
    }
    else {
      return postingList.get(nextP);
    }
  }
  
  private int binarySearch(String term, int low, int high, int current) {
    List<Integer> postingList = index.get(term);
    int mid = 0;
    while(high - low > 0) {
      mid = (low + high) / 2;
      if(postingList.get(mid) <= current) {
        low = mid;
      }
      else {
        high = mid;
      }
    }
    return high;
  }
  
  @Override
  public int corpusDocFrequencyByTerm(String term) {
    List<Integer> postingList = index.get(term);
    if(postingList == null || (postingList.size() == 0)) {
      return 0;
    }
    int pos = 0;
    int count = 1;
    while((pos = getNextDocPos(postingList, pos)) != -1) {
      count += 1;
    }
    return count;
  }

  public int corpusTermFrequency() {
    return index.keySet().size();
  }
  
  @Override
  public int corpusTermFrequency(String term) {
    List<Integer> postingList = index.get(term);
    if(postingList == null || (postingList.size() == 0)) {
      return 0;
    }
    int pos = 0;
    int count = postingList.get(1);
    while((pos = getNextDocPos(postingList, pos)) != -1) {
      count += postingList.get(pos+1);
    }
    return count;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    SearchEngine.Check(false, "Not implemented!");
    return 0;
  }
}
