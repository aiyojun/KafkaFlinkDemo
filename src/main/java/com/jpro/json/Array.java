package com.jpro.json;


import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * JsonArray type, prepared for Json!
 * You shouldn't create JsonArray by this class.
 * Create JsonArray by Json::array() method, instead!
 */
public class Array extends Json implements Iterable<Json> {
    /**
     * Construct an empty JsonArray!
     */
    public Array () {
        root = JsonNodeFactory.instance.arrayNode();
    }

    /**
     * Construct an initialized JsonArray by the specific collection!
     */
    public <E> Array(Collection<E> collection) { this(); collection.forEach(this::add); }

    public Json get(int index) {
        JsonNode node = root.get(index);
        return new Json() {{root = node;}};
    }

    /**
     * Adding element of JsonArray.
     */
    public <E> Array add(E v) {
        if (v instanceof Json) {
            ((ArrayNode) root).add(((Json) v).root);
        } else if (v instanceof String) {
            ((ArrayNode) root).add((String) v);
        } else if (v instanceof Integer) {
            ((ArrayNode) root).add((Integer) v);
        } else if (v instanceof Long) {
            ((ArrayNode) root).add((Long) v);
        } else if (v instanceof Boolean) {
            ((ArrayNode) root).add((Boolean) v);
        } else {
            throw new RuntimeException("Unknown type");
        }
        return this;
    }

    /**
     * To JSON string of JsonArray!
     */
    public String dumps() { return root.toString(); }

    /**
     * Same as Array::dumps() method!
     */
    @Override
    public String toString() { return dumps(); }

    @Override
    public Iterator<Json> iterator() {
        return new Iter();
    }

    private class Iter implements Iterator<Json> {
        private int cursor;

        @Override
        public boolean hasNext() {
            return cursor != root.size();
        }

        @Override
        public Json next() {
            if (cursor >= root.size()) throw new NoSuchElementException();
            JsonNode pr = root.get(cursor);
            cursor++;
            return new Json() {{root = pr;}};
        }
    }
}
