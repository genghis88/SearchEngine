package edu.nyu.cs.cs2580.Compress;

import java.util.ArrayList;
import java.util.BitSet;

public class GammaCompression extends Compression{

	@Override
	public int compressBatch(int[] arg, BitSet b) {
		// TODO Auto-generated method stub
		int bitpos = 0;
		for(int i = 0; i < arg.length; i++)
		{
			int k = arg[i];
			int kd = (int)Math.floor((Math.log(k)/log2));
			int kr = k - (1<<kd);
			b.set(bitpos, bitpos + kd, true);
			bitpos += kd;
			b.set(bitpos, false);
			bitpos++;
			BitSet b2 = convert(kr, kd);
			set(b, b2, bitpos, bitpos + kd);
			bitpos += kd;
		}
		return bitpos;
	}

	@Override
	public int compress(int arg, BitSet b, int pos) {
		// TODO Auto-generated method stub
		int bitpos = pos;
		int k = arg;
		int kd = (int)Math.floor((Math.log(k)/log2));
		int kr = k - (1<<kd);
		b.set(bitpos, bitpos + kd, true);
		bitpos += kd;
		b.set(bitpos, false);
		bitpos++;
		BitSet b2 = convert(kr, kd);
		set(b, b2, bitpos, bitpos + kd);
		bitpos += kd;
		return bitpos;
	}

	@Override
	public ArrayList<Integer> deCompressBatch(BitSet b, int count) {
		// TODO Auto-generated method stub
		ArrayList<Integer> postinglist = new ArrayList<Integer>();
		for(int i = 0; i < count;)
		{
			int unary = 0;
			while(b.get(i))
			{
				unary++;
				i++;
			}
			
			i++;
			if(unary == 0)
			{
				postinglist.add(1);
			}
			
			else
			{
				int val = 0;
				for(int k = 0; k < unary; k++)
				{
					val = val << 1;
					val = val | (b.get(i + k)?1:0);
				}
				postinglist.add((1 << unary) + val);				
			}
			
			i = i + unary;
		}
		return postinglist;
	}

	@Override
	public int[] deCompress(BitSet b, int count, int pos) {
		// TODO Auto-generated method stub
		int unary = 0;
		int i = pos;
		while(b.get(i))
		{
			unary++;
			i++;
		}
		
		i++;
		if(unary == 0)
		{
			if(i < count)
				return new int[]{1, i};
			else
				return new int[]{1, -1};
		}
		
		else
		{
			int val = 0;
			for(int k = 0; k < unary; k++)
			{
				val = val << 1;
				val = val | (b.get(i + k)?1:0);
			}
			int ans = ((1 << unary) + val);
			i += unary;
			if(i < count)
				return new int[]{ans, i};
			else
				return new int[]{ans, -1};
		}
	}

}
