package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;
import java.util.HashMap;
import java.util.Set;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

public class CosineRanker extends Ranker
{
    public CosineRanker(Options options,
      CgiArguments arguments, Indexer indexer) {
      super(options, arguments, indexer);
      System.out.println("Using Ranker: " + this.getClass().getSimpleName());
    }
    @Override
    public Vector<ScoredDocument> runQuery(Query query, int numResults) {
      Queue<ScoredDocument> rankQueue = new PriorityQueue<ScoredDocument>();
      Document doc = null;
      int docid = -1;
      while ((doc = _indexer.nextDoc(query, docid)) != null) {
        HashMap<String, Double> qv = TFIDF.CalculateQueryVector(query._query, _indexer);

        // Get the document vector. For hw1, you don't have to worry about the
        // details of how index works.
        HashMap<String, Double> dv = TFIDF.CalculateDocumentVector(doc, query._query, _indexer);
        Set<String> A = qv.keySet();
        Set<String> B = dv.keySet();
        Set<String> keys = qv.size() < dv.size() ? A : B;
        
        double dena = 0.0;
        for(String a : A)
        {
          dena+= qv.get(a)*qv.get(a);
        }
        double denb = 0.0;
        denb = doc._normfactor;
        
        double denominator = Math.sqrt(dena)* denb;
        
        
        double score = 0.0;
        for (String key: keys){
          double temp1 = 0.0;
          if(qv.containsKey(key))
            temp1 = qv.get(key);
          double temp2 = 0.0;
          if(dv.containsKey(key))
            temp2 = dv.get(key);
          
          score += temp1*temp2;
        }
        score /= denominator;
        rankQueue.add(new ScoredDocument(doc, score));
        if (rankQueue.size() > numResults) {
          rankQueue.poll();
        }
        docid = doc._docid;
      }

      Vector<ScoredDocument> results = new Vector<ScoredDocument>();
      ScoredDocument scoredDoc = null;
      while ((scoredDoc = rankQueue.poll()) != null) {
        results.add(scoredDoc);
      }
      Collections.sort(results, Collections.reverseOrder());
      return results;
    }
}