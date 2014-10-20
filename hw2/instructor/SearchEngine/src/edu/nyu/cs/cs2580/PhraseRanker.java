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
  private double lambda = 0.5;
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
    double totalscore=0.0;
    int docid = -1;
    try {
    while ((doc = _indexer.nextDocument(query, docid)) != null) {
      DocumentIndexed temp = (DocumentIndexed)doc;
      List<Integer> postingList= temp.getDocumentDetails();
      Iterator<Integer> iter = postingList.iterator();
      List<Integer> countslist = new ArrayList<Integer>();
      while (iter.hasNext()) {
        Integer counts = (Integer) iter.next();
        countslist.add(counts);
        for(int k = 0; k < counts; k++)
        {
          iter.next();
        }
      }
      for(int i=0 ; i<query._tokens.size() ; i++)
      {
        int totaltokensdoc=0;
        long totaltokenscorpus=0;
        double scorecomp1 = 0.0;
        double scorecomp2 = 0.0;
        int corpustokenfrequency = _indexer.corpusTermFrequency(query._tokens.get(i));
        String[] tokencomponents = query._tokens.get(i).split(" ");
        if(tokencomponents.length > 1)
        {
          totaltokensdoc = doc._numwords - (tokencomponents.length - 1);
          totaltokenscorpus = _indexer.getTotalTokensCorpus(tokencomponents.length);
        }
        else
        {
          totaltokensdoc = doc._numwords;
          totaltokenscorpus = _indexer.totalWordsInCorpus;
        }
        if(totaltokensdoc > 0)
          scorecomp1 = countslist.get(i)/totaltokensdoc;
        else
          scorecomp1 = 0.0;
        if(totaltokenscorpus > 0)
          scorecomp2 = corpustokenfrequency/totaltokenscorpus;
        else
          scorecomp2 = 0.0;
        totalscore *= ((1 - lambda) * scorecomp1 + lambda * scorecomp2);
      }
      rankQueue.add(new ScoredDocument(doc, (Math.log(1+totalscore))));
      if (rankQueue.size() > numResults) {
        rankQueue.poll();
      }
    }
    }
    catch(Exception e) {
      e.printStackTrace();
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