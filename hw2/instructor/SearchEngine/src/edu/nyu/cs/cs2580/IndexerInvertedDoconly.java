package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
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
      String token = s.next();
      if(tokens.containsKey(token))
      {
        //tokens.put(token, tokens.get(token) + 1);
      }
      else
      {
         tokens.put(token, 1);
      }
    }
    return;
  }
  
  private void updateIndex(HashMap<String, Integer> tokens ,int did) {
    
    List<String> tokenKeys = (List<String>)tokens.keySet();
    for(String tokenKey: tokenKeys)
    {
      //int count = tokens.get(tokenKey);
      if(index.containsKey(tokenKey))
      {
        index.get(tokenKey).add(did);
      }
      else
      {
        Vector<Integer> plist = new Vector<Integer>();
        plist.add(did);
        index.put(tokenKey, plist);        
      }
    }
    return;
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
  }

  @Override
  public Document getDoc(int docid) {
    return null;
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}
   */
  @Override
  public Document nextDoc(Query query, int docid) {
    return null;
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
