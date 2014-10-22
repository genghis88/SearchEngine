package edu.nyu.cs.cs2580.SkipPointer;

import java.io.Serializable;
import java.util.ArrayList;

public class SkipPointer implements Serializable {
	
	/**
   * 
   */
  private static final long serialVersionUID = -4646700548252218847L;

  public class Pair implements Serializable
	{
		/**
     * 
     */
	  	private static final long serialVersionUID = -2637103976834923929L;
    	private int docid;
		private long pos;
		public int getDocid() {
			return docid;
		}
		public void setDocid(int docid) {
			this.docid = docid;
		}
		public long getPos() {
			return pos;
		}
		public void setPos(long pos) {
			this.pos = pos;
		}		
	}
	
	private ArrayList<Pair> pairlist;
	
	public SkipPointer() {
		// TODO Auto-generated constructor stub
		pairlist = new ArrayList<SkipPointer.Pair>();
	}
	
	public void addPointer(int docid, int pos)
	{
		Pair p = new Pair();
		p.setDocid(docid);
		p.setPos(pos);
		pairlist.add(p);
	}
	
	public Pair search(int docid)
	{
		int low = 0;
		int high = pairlist.size()-1;
		
		while(low < high)
		{
			int mid = (low + high)/2;
			Pair p = pairlist.get(mid);
			if(p.getDocid() == docid)
			{
				if(mid > 0)
					return pairlist.get(mid - 1);
				else
					return null;
			}
			else if(p.getDocid() > docid)
			{
				high = mid - 1;
			}
			else
			{
				if(pairlist.get(mid + 1).getDocid() < docid)
					low = mid + 1;
				else
				{
					return pairlist.get(mid);
				}
				
			}
		}
		
		return pairlist.get(low);
	}
}
