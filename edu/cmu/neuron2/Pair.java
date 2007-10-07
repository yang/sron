package edu.cmu.neuron2;

public class Pair<A,B>
{
	protected A first;
	protected B second;
	public Pair(A a, B b)
	{
		first = a;
		second = b;
	}
	public A first()
	{
		return first;
	}
	public B second()
	{
		return second;
	}
	public boolean equals(Object O)
	{
		if(O instanceof Pair)
		{
			Pair P = (Pair)O;
			if(first.equals(P.first()) && second.equals(P.second()))
				return true;
		}
		return false;
	}
	public int hashCode()
	{
		return first.hashCode()+second.hashCode();
	}

	public String toString()
	{
		String s = "";
		s = s + "(" + first.toString() + ", " + second.toString() + ")";
		return s;
	}

};
