package edu.nyu.cs.cs2580;

import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

public class TFIDF {
	
	public static HashMap<String, Double> CalculateDocumentVector(Document doc, String query, Indexer indexer)
	{
	  
		HashMap<String, Double> vector = new HashMap<String, Double>();
		HashMap<String, Integer> counts = new HashMap<String, Integer>();
		Vector<String> bv = new Vector<String>();
		String[] queryelems = query.split(" ");
		for(int i=0 ; i<queryelems.length ; i++)
		  bv.add(queryelems[i]);
		//Vector<String> tv = doc.get_title_vector();
		int N = indexer.numDocs();
		//double total_terms = tv.size() + bv.size();
		//double incr = 1.0/total_terms;
		for(String term : bv)
		{
		  int count = indexer.documentTermFrequency(term, doc._docid);
		  counts.put(term, count);
		}
	
		double denominator = 0;
		
		Set<String> keys = counts.keySet();
	
		for(String key : keys)
		{
			int nd = indexer.corpusDocFrequencyByTerm(key);
			double weight = (Math.log((double)counts.get(key) + 1.0) + 1.0)*Math.log((N*1.0)/nd);
			denominator += weight*weight;
			vector.put(key, weight);
		}
		
		denominator = Math.sqrt(denominator);
		//System.out.println(denominator);
		for(String key : keys)
		{
			vector.put(key, vector.get(key)/denominator);
		}
		return vector;		
	}
	
	public static HashMap<String, Double> CalculateQueryVector(String query, Indexer indexer)
	{
		Scanner s = new Scanner(query);
	 
		HashMap <String, Integer> counts = new HashMap<String, Integer>();
	    while (s.hasNext()){
	      String term = s.next();
	      if(counts.containsKey(term))
	      {
	    	  counts.put(term, counts.get(term)+1);
	      }
	      else
	    	  counts.put(term, 1);
	    }
	    s.close();
		HashMap<String, Double> vector = new HashMap<String, Double>();
		Set<String> keys = counts.keySet();
		double denominator = 0;
		int N = indexer.numDocs();
		for(String key : keys)
		{
			int nd = indexer.corpusDocFrequencyByTerm(key);
			double weight = (Math.log((double)counts.get(key)) + 1.0)*Math.log((N*1.0)/nd);
			denominator += weight*weight;
			vector.put(key, weight);
		}
		denominator = Math.sqrt(denominator);
		for(String key : keys)
		{
			vector.put(key, vector.get(key)/denominator);
		}
		return vector;
	}
}
