package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

public class PhraseRanker extends Ranker
{
  Indexer myindexer;
  protected PhraseRanker(Options options, CgiArguments arguments,
      Indexer indexer) {
    super(options, arguments, indexer);
    myindexer = indexer;
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults) {
    Queue<ScoredDocument> rankQueue = new PriorityQueue<ScoredDocument>();
    Document doc = null;
    int docid = -1;
    while ((doc = _indexer.nextDoc(query, docid)) != null) {
      DocumentIndexed temp = (DocumentIndexed)doc;
      List<Integer> postingList= temp.getDocumentDetails();
      Iterator<Integer> iter = postingList.iterator();
      List<List<Integer>> l = new ArrayList<List<Integer>>();
      while (iter.hasNext()) {
        Integer integer = (Integer) iter.next();
        List<Integer> l2 = new ArrayList<Integer>();
        l.add(l2);
        for(int k = 0; k < integer; k++)
        {
          if(iter.hasNext())
            l2.add(iter.next());
          else
          {
            //Exception
          }
        }
      }
        
      int pharasecount = 0;
      List<Integer> firstterm = l.get(0);
      for(int i = 0; i < firstterm.size(); i++)
      {
        int firstpos = firstterm.get(i);
        boolean flag = true;
        for(int j = 1; j < l.size(); j++)
        {
          if(!l.get(j).contains(firstpos + j))
          {
            flag = false;
            break;
          }
        }
        if(flag)
          pharasecount++;
      }
        
      rankQueue.add(new ScoredDocument(doc, ((double)pharasecount)/(doc._numwords)));
      if (rankQueue.size() > numResults) {
        rankQueue.poll();
      }
    }
    docid = doc._docid;
    Vector<ScoredDocument> results = new Vector<ScoredDocument>();
    ScoredDocument scoredDoc = null;
    while ((scoredDoc = rankQueue.poll()) != null) {
      results.add(scoredDoc);
    }
    Collections.sort(results, Collections.reverseOrder());
    return results;
  }
}