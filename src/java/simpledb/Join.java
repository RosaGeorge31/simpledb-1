package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

	private static final long serialVersionUID = 1L;

	private JoinPredicate m_predicate;
	private DbIterator leftChild;
	private DbIterator rightChild;
	private Tuple leftTuple;
	private Tuple rightTuple;

	/**
	 * Constructor. Accepts to children to join and the predicate to join them
	 * on
	 * 
	 * @param p
	 *            The predicate to use to join the children
	 * @param child1
	 *            Iterator for the left(outer) relation to join
	 * @param child2
	 *            Iterator for the right(inner) relation to join
	 */
	public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
		// some code goes here
		m_predicate = p;
		leftChild = child1;
		rightChild = child2;
	}

	public JoinPredicate getJoinPredicate() {
		// some code goes here
		return m_predicate;
	}

	/**
	 * @return the field name of join field1. Should be quantified by alias or
	 *         table name.
	 * */
	public String getJoinField1Name() {
		// some code goes here
		// left: how to find table name????
		return leftChild.getTupleDesc().getFieldName(m_predicate.getField1());
	}

	/**
	 * @return the field name of join field2. Should be quantified by alias or
	 *         table name.
	 * */
	public String getJoinField2Name() {
		// some code goes here
		return rightChild.getTupleDesc().getFieldName(m_predicate.getField2());
	}

	/**
	 * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
	 *      implementation logic.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return TupleDesc.merge(leftChild.getTupleDesc(),
				rightChild.getTupleDesc());
	}

	public void open() throws DbException, NoSuchElementException,
			TransactionAbortedException {
		// some code goes here
		leftChild.open();
		rightChild.open();
		super.open();
		leftTuple = null;
		rightTuple = null;
	}

	public void close() {
		// some code goes here
		leftChild.close();
		rightChild.close();
		super.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		leftChild.rewind();
		rightChild.rewind();
		leftTuple = null;
		rightTuple = null;
	}

	/**
	 * Returns the next tuple generated by the join, or null if there are no
	 * more tuples. Logically, this is the next tuple in r1 cross r2 that
	 * satisfies the join predicate. There are many possible implementations;
	 * the simplest is a nested loops join.
	 * <p>
	 * Note that the tuples returned from this particular implementation of Join
	 * are simply the concatenation of joining tuples from the left and right
	 * relation. Therefore, if an equality predicate is used there will be two
	 * copies of the join attribute in the results. (Removing such duplicate
	 * columns can be done with an additional projection operator if needed.)
	 * <p>
	 * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
	 * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
	 * 
	 * @return The next matching tuple.
	 * @see JoinPredicate#filter
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		// use left child as a pivot
		if(leftTuple==null && !leftChild.hasNext())
			return null;
		while (leftChild.hasNext()) {
			if (leftTuple == null && rightTuple == null) {
				leftTuple = leftChild.next();
			}
			while (rightChild.hasNext()) {
				rightTuple = rightChild.next();
				if (m_predicate.filter(leftTuple, rightTuple)) {
					// merge two tuple
					TupleDesc totalTD = this.getTupleDesc();
					Tuple totalTuple = new Tuple(totalTD);
					for (int i = 0; i < leftChild.getTupleDesc().numFields(); i++)
						totalTuple.setField(i, leftTuple.getField(i));
					for (int i = 0; i < rightChild.getTupleDesc().numFields(); i++)
						totalTuple.setField(i
								+ leftChild.getTupleDesc().numFields(),
								rightTuple.getField(i));
					return totalTuple;
				}
			}
			rightChild.rewind();
			leftTuple = leftChild.next();
			//do the last round seperately
		}
		while (rightChild.hasNext()) {
			rightTuple = rightChild.next();
			if (m_predicate.filter(leftTuple, rightTuple)) {
				// merge two tuple
				TupleDesc totalTD = this.getTupleDesc();
				Tuple totalTuple = new Tuple(totalTD);
				for (int i = 0; i < leftChild.getTupleDesc().numFields(); i++)
					totalTuple.setField(i, leftTuple.getField(i));
				for (int i = 0; i < rightChild.getTupleDesc().numFields(); i++)
					totalTuple.setField(i
							+ leftChild.getTupleDesc().numFields(),
							rightTuple.getField(i));
				return totalTuple;
			}
		}
		return null;
	}

	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		return new DbIterator[] { leftChild, rightChild };
	}

	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
		leftChild = children[0];
		rightChild = children[1];
	}
}