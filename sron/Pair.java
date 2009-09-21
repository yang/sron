package sron;

public class Pair<A extends Comparable<A>, B> implements Comparable<Pair<A, B>> {

    @Override
    public int compareTo(Pair<A, B> o) {
        return first.compareTo(o.first);
    }

    protected A first;
    protected B second;

    public Pair(A a, B b) {
        first = a;
        second = b;
    }

    public A first() {
        return first;
    }

    public B second() {
        return second;
    }

    public boolean equals(Object O) {
        if (O instanceof Pair) {
            Pair<?, ?> P = (Pair<?, ?>) O;
            if (first.equals(P.first()) && second.equals(P.second()))
                return true;
        }
        return false;
    }

    public int hashCode() {
        return first.hashCode() ^ second.hashCode();
    }

    public static <A extends Comparable<A>, B> Pair<A, B> of(A a, B b) {
        return new Pair<A, B>(a, b);
    }

    public String toString() {
        String s = "";
        s = s + "(" + first.toString() + ", " + second.toString() + ")";
        return s;
    }

};
