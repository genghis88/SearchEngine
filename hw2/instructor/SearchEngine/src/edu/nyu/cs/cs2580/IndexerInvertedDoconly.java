package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer {
  
  private Vector<Document> _documents = new Vector<Document>();
  private HashMap<String,Vector<Integer>> index = new HashMap<String,Vector<Integer>>();

  public IndexerInvertedDoconly(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  @Override
  public void constructIndex() throws IOException {
    String corpusFile = _options._corpusPrefix + "/corpus.tsv";
    System.out.println("Construct index from: " + corpusFile);

    BufferedReader reader = new BufferedReader(new FileReader(corpusFile));
    try {
      String line = null;
      while ((line = reader.readLine()) != null) {
        processDocument(line);
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
  
  private void processDocument(String content) {
    Scanner s = new Scanner(content).useDelimiter("\t");

    String title = s.next();
    HashMap<String, Integer> tokens = new HashMap<String, Integer>();
    readTermVector(title, tokens);
    int docid = _documents.size();
    readTermVector(s.next(),tokens);
    updateIndex(tokens,docid);   

    int numViews = Integer.parseInt(s.next());
    s.close();

    Document doc = new DocumentIndexed(_documents.size());
    doc.setTitle(title);
    doc.setNumViews(numViews);
    _documents.add(doc);
    ++_numDocs;

    
  }
  
  private void readTermVector(String content, HashMap<String, Integer> tokens) {
    Scanner s = new Scanner(content);  // Uses white space by default.
    while (s.hasNext()) {
      String word = s.next();
      if(tokens.containsKey(word)) {
        //tokens.put(token, tokens.get(token) + 1);
      }
      else {
         tokens.put(word, 1);
      }
    }
    return;
  }
  
  private void updateIndex(HashMap<String, Integer> tokens ,int did) {
    List<String> wordsInDoc = (List<String>)tokens.keySet();
    for(String word:wordsInDoc) {
      //int count = tokens.get(tokenKey);
      if(index.containsKey(word)) {
        index.get(word).add(did);
      }
      else {
        Vector<Integer> postingList = new Vector<Integer>();
        postingList.add(did);
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
    IndexerInvertedDoconly loaded = (IndexerInvertedDoconly) reader.readObject();

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
    int prev = -1;
    int maxDocId = -1;
    for(String term:query._tokens) {
      docids[i] = next(term,docid);
      if(docids[i] == -1) {
        return null;
      }
      if(prev != -1) {
        if(docids[prev] != docids[i]) {
          flag = false;
        }
      }
      if(maxDocId < docids[i]) {
        maxDocId = docids[i];
      }
      prev++;
      i++;
    }
    if(flag) {
      //this doc contains all the terms!!
      //all the query terms map to the same doc id
      _documents.get(docids[0]);
    }
    return nextDoc(query, maxDocId);
  }
  
  private int next(String term,int docid) {
    Vector<Integer> postingList = index.get(term);
    if(postingList == null || postingList.size() == 0 || postingList.size() <= docid) {
      return -1;
    }
    if(postingList.get(0) > docid) {
      return postingList.get(0);
    }
    return postingList.get(binarySearch(term,0,postingList.size()-1,docid));
  }
  
  private int binarySearch(String term, int low, int high, int current) {
    Vector<Integer> postingList = index.get(term);
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
    return 0;
  }

  @Override
  public int corpusTermFrequency(String term) {
    return 0;
  }

  @Override
  public int documentTermFrequency(String term, String url) {
    SearchEngine.Check(false, "Not implemented!");
    return 0;
  }
}
