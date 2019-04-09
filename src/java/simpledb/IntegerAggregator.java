package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;

import static simpledb.Aggregator.Op.AVG;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final Field DEFAULT_FIELD = new StringField("default", 0);
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> countMap;
    private HashMap<Field, Integer> minOrMaxMap;
    private HashMap<Field, Integer> sumMap;
    private ArrayList<Tuple> tuples;
    private TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // SELECT COUNT(aggregate_field) FROM table GROUP BY group_field;
        IntField aggregateField = (IntField)tup.getField(afield);
        Field gfield = gbfield == NO_GROUPING ? DEFAULT_FIELD : tup.getField(gbfield);
        switch (what) {
            case MIN:
                if (!minOrMaxMap.containsKey(gfield)) {
                    minOrMaxMap.put(gfield, aggregateField.getValue());
                } else if (minOrMaxMap.get(gfield) > aggregateField.getValue()){
                    minOrMaxMap.put(gfield, aggregateField.getValue());
                }
                break;
            case AVG:
                sumMap.put(gfield, sumMap.get(gfield) + aggregateField.getValue());
                countMap.put(gfield, countMap.get(gfield) + 1);
                break;
            case SUM:
                sumMap.put(gfield, sumMap.get(gfield) + aggregateField.getValue());
                break;
            case MAX:
                if (!minOrMaxMap.containsKey(gfield)) {
                    minOrMaxMap.put(gfield, aggregateField.getValue());
                } else if (minOrMaxMap.get(gfield) < aggregateField.getValue()){
                    minOrMaxMap.put(gfield, aggregateField.getValue());
                }
                break;
            case COUNT:
                countMap.put(gfield, countMap.get(gfield) + 1);
                break;
        }

        Tuple tuple = new Tuple(tupleDesc);
//        if (gbfield == NO_GROUPING) {
//            tuple.setField(0, gfield);
//            tuple.setField(1, newField);
//        } else {
//            tuple.setField(0, newField);
//        }
//        tuples.add();
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {
            @Override
            public void open() throws DbException, TransactionAbortedException {

            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return null;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {

            }

            @Override
            public TupleDesc getTupleDesc() {
                return null;
            }

            @Override
            public void close() {

            }
        };
    }
}
