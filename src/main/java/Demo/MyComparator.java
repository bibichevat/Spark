package Demo;

import java.io.Serializable;
import java.util.Comparator;

public class MyComparator implements Comparator<String>, Serializable {
    @Override
    public int compare(String k1, String k2) {
        return k1.length() - k2.length();
    }
}