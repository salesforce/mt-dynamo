package com.salesforce.dynamodbv2.mt.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A utility for joining strings such that they can be split again deterministically.
 *
 * <p>TODO Consider adding support for null values
 */
public class CompositeStrings {

    private final char separator;
    private final char escape;

    public CompositeStrings() {
        this('-', '\\');
    }

    public CompositeStrings(char separator, char escape) {
        this.separator = separator;
        this.escape = escape;
    }

    /**
     * Builds a composite key sequenceNumber from the given sequence of key values using the
     * configured separator and escape characters.
     *
     * @param values Values to join into a composite key sequenceNumber
     * @return Joined composite key containing all individual keys
     */
    public String join(Iterable<String> values) {
        // determine upper bound for composite key size
        int length = 0;
        for (String value : values) {
            length += 1 + value.length() * 2; // one separate per key plus one escape per char
        }

        // now construct composite key
        char[] composite = new char[length];
        int j = 0;
        for (String value : values) {
            if (j > 0) {
                composite[j++] = separator;
            }
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c == separator || c == escape) {
                    composite[j++] = escape;
                }
                composite[j++] = c;
            }
        }
        return new String(composite, 0, j);
    }

    /**
     * Splits the given composite key into individual keys using the configured
     * separator and escape characters.
     *
     * @param composite Composite key to split into individual keys.
     * @return Sequence of keys contained in the given composite key
     */
    public Iterator<String> split(String composite) {
        return new Iterator<String>() {

            private int index = 0;
            private String value;

            @Override
            public boolean hasNext() {
                if (value != null) {
                    return true;
                }
                if (index > composite.length()) {
                    return false;
                }
                // try to find next key
                char[] next = new char[composite.length() - index];
                int i = 0;
                for (; index < composite.length(); index++) {
                    char c = composite.charAt(index);
                    if (c == escape && index + 1 < composite.length()) {
                        next[i++] = composite.charAt(++index);
                    } else if (c == separator) {
                        break;
                    } else {
                        next[i++] = composite.charAt(index);
                    }
                }
                index++; // advance index beyond separator or end of array
                value = new String(next, 0, i);
                return true;
            }

            @Override
            public String next() {
                if (hasNext()) {
                    String k = value;
                    value = null;
                    return k;
                }
                throw new NoSuchElementException();
            }
        };
    }

}
