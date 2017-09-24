package com.jajja.jorm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import com.jajja.jorm.Row.NamedField;

public class RecordBatch<T extends Record> {
    private final Iterable<T> records;
    private T template = null;
    private int size;

    private RecordBatch(Iterable<T> records) {
        this.records = records;
    }

    public static <T extends Record> RecordBatch<T> of(Iterable<T> records) {
        RecordBatch<T> batch = new RecordBatch<T>(records);
        batch.initalize();
        return batch;
    }

    private void initalize() {
        for (T record : records) {
            record.assertNotReadOnly();

            if (template == null) {
                template = record;
            } else {
                check(record);
            }
            size++;
        }
    }

    private void check(T record) {
        if (!template.getClass().equals(record.getClass())) {
            throw new IllegalArgumentException("all records must be of the same class");
        }
        if (template.table().getDatabase() == null) {
            throw new IllegalArgumentException("template record has no @Jorm database defined");
        }
        if (!template.table().getDatabase().equals(record.table().getDatabase())) {
            throw new IllegalArgumentException("all records must be bound to the same Database");
        }
    }

    public T template() {
        return template;
    }

    public Table table() {
        return template.table();
    }

    public Composite primaryKey() {
        return template.primaryKey();
    }

    public Iterable<T> records() {
        return records;
    }

    public Class<? extends Record> clazz() {
        return template.getClass();
    }

    public int size() {
        return size;
    }

    public Iterable<Slice<T>> slice(int size) {
        return new Slicer<T>(this, records, size);
    }

    public static class Slice<TT extends Record> extends ArrayList<TT> {
        private static final long serialVersionUID = 1L;
        private final RecordBatch<TT> batch;
        private Set<String> columns;
        private Set<String> dirtyColumns;

        public Slice(RecordBatch<TT> batch, int size) {
            super(size);
            this.batch = batch;
        }

        private void complete() {
            columns = new HashSet<String>();
            dirtyColumns = new HashSet<String>();
            for (TT record : this) {
                for (NamedField f : record.fields()) {
                    if (!table().isImmutable(f)) {
                        columns.add(f.name());
                        if (template().isChanged(f)) {
                            dirtyColumns.add(f.name());
                        }
                    }
                }
            }
            this.columns = Collections.unmodifiableSet(columns);
            this.dirtyColumns = Collections.unmodifiableSet(dirtyColumns);
        }

        public TT template() {
            return batch.template();
        }

        public Table table() {
            return batch.table();
        }

        public Set<String> columns() {
            return columns;
        }

        public Composite primaryKey() {
            return batch.primaryKey();
        }

        public Class<? extends Record> clazz() {
            return batch.clazz();
        }

        public Set<String> dirtyColumns() {
            return dirtyColumns;
        }
    }

    public static class Slicer<TT extends Record> implements Iterable<Slice<TT>> {
        private final RecordBatch<TT> batch;
        private final Iterable<TT> records;
        private final int size;

        public Slicer(RecordBatch<TT> batch, Iterable<TT> records, int size) {
            if (size <= 0) {
                throw new IllegalArgumentException("size must be greated than 0");
            }
            this.batch = batch;
            this.records = records;
            this.size = size;
        }

        @Override
        public Iterator<Slice<TT>> iterator() {
            return new Slicerator<TT>(batch, records.iterator(), size);
        }
    }

    public static class Slicerator<TT extends Record> implements Iterator<Slice<TT>> {
        private final RecordBatch<TT> batch;
        private final Iterator<TT> records;
        private final int size;
        private Slice<TT> next;

        public Slicerator(RecordBatch<TT> batch, Iterator<TT> records, int size) {
            this.batch = batch;
            this.records = records;
            this.size = size;
            this.next = makeNext();
        }

        private Slice<TT> makeNext() {
            Slice<TT> list = new Slice<TT>(batch, size);
            for (int i = 0; i < size && records.hasNext(); i++) {
                list.add(records.next());
            }
            list.complete();
            return list;
        }

        @Override
        public boolean hasNext() {
            return next.size() > 0;
        }

        @Override
        public Slice<TT> next() {
            if (next.isEmpty()) {
                throw new NoSuchElementException();
            }
            Slice<TT> ret = next;
            next = makeNext();
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
