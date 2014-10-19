package edu.nyu.cs.cs2580.SkipPointer;

import java.util.ArrayList;

public class SkipPointer {
	
	public class Pair
	{
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
	
	public long search(int docid)
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
					return pairlist.get(mid - 1).getPos();
				else
					return 0;
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
					return pairlist.get(mid).getPos();
				}
				
			}
		}
		
		return pairlist.get(low).getPos();
	}
}
