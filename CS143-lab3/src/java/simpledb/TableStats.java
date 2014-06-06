package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private void fillhistograms(DbFileIterator iterator, TupleDesc td) {
    	Tuple currentTup;
    	try {
			iterator.open();
			while (iterator.hasNext()) {
				currentTup = iterator.next();
				for (int i = 0; i < td.numFields(); i++) {
					String fieldname = td.getFieldName(i);
					switch (td.getFieldType(i)) {
					case INT_TYPE:
						int intvalue = ((IntField) currentTup.getField(i)).getValue();
						m_intHistograms.get(fieldname).addValue(intvalue);
						break;
					case STRING_TYPE:
						String stringvalue = ((StringField) currentTup.getField(i)).getValue();
						m_stringHistograms.get(fieldname).addValue(stringvalue);
						break;
					}
				}
			}
			iterator.close();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    }
    
    private void setMinsAndMaxs(DbFileIterator iterator, TupleDesc td) {
    	Tuple currentTup;
    	try {
			iterator.open();
	    	while (iterator.hasNext()) {
	    		currentTup = iterator.next();
	    		m_numTuples++;
	    		for (int i = 0; i < td.numFields(); i++) {	    
	    			String fieldname = td.getFieldName(i);
	    			switch (td.getFieldType(i)) {
	    			case INT_TYPE:
	    				int fieldvalue = ((IntField) currentTup.getField(i)).getValue();
	    				if (!m_maxs.containsKey(fieldname))
	    					m_maxs.put(fieldname, fieldvalue);
	    				else {
	    					int currentMax = m_maxs.get(fieldname);
	    					int newMax;
	    					if(currentMax > fieldvalue) {
	    						newMax = currentMax;
	    					}
	    					else {
	    						newMax = fieldvalue;
	    					}
	    					m_maxs.put(fieldname, newMax);
	    				}
	    				if (!m_mins.containsKey(fieldname))
	    					m_mins.put(fieldname, fieldvalue);
	    				else {
	    					int currentMin = m_mins.get(fieldname);
	    					int newMin;
	    					if(currentMin < fieldvalue) {
	    						newMin = currentMin;
	    					}
	    					else {
	    						newMin = fieldvalue;
	    					}
	    					m_mins.put(fieldname, newMin);
	    				}
	    				break;
	    			case STRING_TYPE:
	    				break;
	    			}
	    		}
	    	}
	    	iterator.close();
		}
    	catch (DbException e) {
			e.printStackTrace();
		}
    	catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    }

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    
    private int m_iocostperpage;
	private int m_numTuples;
	private HashMap<String, Integer> m_maxs;
	private HashMap<String, Integer> m_mins;
	private HashMap<String, IntHistogram> m_intHistograms;
	private HashMap<String, StringHistogram> m_stringHistograms;
	private DbFile m_file;
	private TupleDesc td;
    
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	
    	m_maxs = new HashMap<String, Integer>();
    	m_mins = new HashMap<String, Integer>();
    	m_intHistograms = new HashMap<String, IntHistogram>();
    	m_stringHistograms = new HashMap<String, StringHistogram>();
    	m_numTuples = 0;
    	
    	m_file = Database.getCatalog().getDatabaseFile(tableid);
    	TransactionId tid = new TransactionId();
    	DbFileIterator iterator = m_file.iterator(tid);
    	m_iocostperpage = ioCostPerPage;
    	td = Database.getCatalog().getTupleDesc(tableid);
    	setMinsAndMaxs(iterator, td);
    
    	// initialize the histograms
    	for (int i = 0; i < td.numFields(); i++) {
    		String fieldname = td.getFieldName(i);
    		switch(td.getFieldType(i)) {
    		case INT_TYPE:
    			IntHistogram inthist = new IntHistogram(NUM_HIST_BINS, m_mins.get(fieldname), m_maxs.get(fieldname));
    			m_intHistograms.put(fieldname, inthist);
    			break;
    		case STRING_TYPE:
    			StringHistogram stringhist = new StringHistogram(NUM_HIST_BINS);
    			m_stringHistograms.put(fieldname, stringhist);
    			break;
    		}
    	}
    	
    	// populate the histograms
    	fillhistograms(iterator, td);
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
    	return ((HeapFile)m_file).numPages()*m_iocostperpage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
    	return (int)(m_numTuples*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
    	if (td.getFieldType(field) == simpledb.Type.INT_TYPE) {
    		IntHistogram hist = m_intHistograms.get(td.getFieldName(field));
    		return hist.avgSelectivity();
    	}
    	else {
    		StringHistogram hist = m_stringHistograms.get(td.getFieldName(field));
    		return hist.avgSelectivity();
    	}
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
    	switch (constant.getType()) {
		case INT_TYPE: {
			IntHistogram histogram = m_intHistograms.get(td.getFieldName(field));
			int value = ((IntField) constant).getValue();
			return histogram.estimateSelectivity(op, value);
		}
		case STRING_TYPE: {
			StringHistogram histogram = m_stringHistograms.get(td.getFieldName(field));
			String value = ((StringField) constant).getValue();
			return histogram.estimateSelectivity(op, value);
		}
		default:
			break;
	}
	return -1;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
    	return m_numTuples;
    }

}