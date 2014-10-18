package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Vector;
import java.util.Scanner;
import java.util.List;
import java.util.Set;

import edu.nyu.cs.cs2580.SearchEngine.Options;

import java.io.File;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer implements Serializable {
  
  private Vector<Document> _documents = new Vector<Document>();
  private HashMap<String,Vector<Integer>> index = new HashMap<String,Vector<Integer>>();

  public IndexerInvertedDoconly(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }
  
  public void test() throws Exception {
    String corpusDirectoryString = _options._corpusPrefix + "/wiki";
    System.out.println("Construct index from: " + corpusDirectoryString);
    final File corpusDirectory = new File(corpusDirectoryString);
    int x = 3;
    for (final File fileEntry : corpusDirectory.listFiles()) {
      if (!fileEntry.isDirectory()) {
        org.jsoup.nodes.Document doc = Jsoup.parse(fileEntry, "UTF-8");

        Element head = doc.select("h1[id=firstHeading]").first();
        System.out.println(head.text().trim());
        Elements content_text = doc.select("div[id=mw-content-text]");
        for (Element elem : content_text) {
          Elements paras = elem.getElementsByTag("p");
          Elements h2s = elem.getElementsByTag("h2");

          for (Element h2 : h2s) {
            System.out.println(h2.getElementsByClass("mw-headline").first());
          }
          for (Element para : paras) {
            System.out.println(para.text().trim());
          }
        }

      }
      x--;
      if (x == 0)
        break;
    }
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
    Set<String> wordsInDoc = tokens.keySet();
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

    try{
    ObjectInputStream reader =
        new ObjectInputStream(new FileInputStream(indexFile));
    IndexerInvertedDoconly loaded = (IndexerInvertedDoconly) reader.readObject();

    this._documents = loaded._documents;
    // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
    this._numDocs = _documents.size();
    this.index = loaded.index;
    this._options = loaded._options;
    this._totalTermFrequency = loaded._totalTermFrequency;
    reader.close();

    System.out.println(Integer.toString(_numDocs) + " documents loaded " +
        "with " + Long.toString(_totalTermFrequency) + " terms!");
    }
    catch(IOException ioe) {
      ioe.printStackTrace();
    }
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
      Document doc = _documents.get(docids[0]);
      return doc;
    }
    return nextDoc(query, maxDocId);
  }
  
  private int next(String term,int docid) {
    Vector<Integer> postingList = index.get(term);
    if(postingList == null || postingList.size() == 0 || postingList.get(postingList.size()-1) <= docid) {
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
    while(high - low > 1) {
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
    return index.get(term).size();
  }
  
  public int corpusTermFrequency() {
    return index.keySet().size();
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
  
  public int documentTermFrequence(String term, int docid) {
    return 0;
  }
}
