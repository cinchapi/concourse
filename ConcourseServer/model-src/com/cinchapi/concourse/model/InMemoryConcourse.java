package com.cinchapi.concourse.model;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedLong;

/**
 * A {@link Concourse} store contained within memory and does not persist to
 * disk.
 * 
 * @author jnelson
 * 
 */
public class InMemoryConcourse extends AbstractConcourse {

	/**
	 * Maps row to columns to values. Columns are are hash sorted and values are
	 * time sorted.
	 */
	private Map<UnsignedLong, HashMap<String, TreeSet<ConcourseValue>>> rows;
	/**
	 * Maps column to values to rows. Values are logically sorted and rows are
	 * sorted in descending order.
	 */
	private Map<String, TreeMap<ConcourseValue, TreeSet<UnsignedLong>>> columns;

	/**
	 * Sorts <code>values</code> in descending order based on timestamp.
	 */
	private static Comparator<ConcourseValue> descendingTimeValueComparator = new Comparator<ConcourseValue>() {

		@Override
		public int compare(ConcourseValue o1, ConcourseValue o2) {
			return -1 * o1.getTimestamp().compareTo(o2.getTimestamp());
		}
	};

	/**
	 * Sorts <code>values</code> in logical order, taking into account type
	 * differences.
	 */
	private static Comparator<ConcourseValue> logicalValueComparator = new Comparator<ConcourseValue>() {

		@Override
		public int compare(ConcourseValue o1, ConcourseValue o2) {
			if(o1.getValue() instanceof Number
					&& o2.getValue() instanceof Number) {
				BigDecimal _o1;
				if(o1.getValue() instanceof Integer) {
					_o1 = new BigDecimal((int) o1.getValue());
				}
				else if(o1.getValue() instanceof Long) {
					_o1 = BigDecimal.valueOf((long) o1.getValue());
				}
				else {
					_o1 = BigDecimal.valueOf((double) o1.getValue());
				}

				BigDecimal _o2;
				if(o2.getValue() instanceof Integer) {
					_o2 = new BigDecimal((int) o2.getValue());
				}
				else if(o2.getValue() instanceof Long) {
					_o2 = BigDecimal.valueOf((long) o2.getValue());
				}
				else {
					_o2 = BigDecimal.valueOf((double) o2.getValue());
				}

				return _o1.compareTo(_o2);
			}
			else {
				return o1.toString().compareTo(o2.toString());
			}
		}

	};

	/**
	 * Sorts <code>rows</code> in descending order.
	 */
	private static Comparator<UnsignedLong> descendingRowComparator = new Comparator<UnsignedLong>() {

		@Override
		public int compare(UnsignedLong o1, UnsignedLong o2) {
			return -1 * o1.compareTo(o2);
		}

	};

	/**
	 * Construct a new instance.
	 * 
	 * @param expectedNumColumnsPerRow
	 */
	public InMemoryConcourse(int expectedNumColumnsPerRow) {
		this.rows = Maps.newTreeMap(descendingRowComparator);
		this.columns = Maps
				.newHashMapWithExpectedSize(expectedNumColumnsPerRow);
	}

	@Override
	protected boolean addSpi(UnsignedLong row, String column,
			ConcourseValue value) {
		if(!exists(row, column, value)) {
			ExecutorService executor = Executors.newCachedThreadPool();

			executor.execute(new ColumnIndexer(row, column, value));
			executor.execute(new RowIndexer(row, column, value));
			executor.execute(new FullTextIndexer(row, column, value));

			return exists(row, column, value);
		}
		else {
			return false;
		}
	}

	@Override
	public Set<String> describe(UnsignedLong row) {
		return rows.get(row).keySet();
	}

	@Override
	public boolean exists(UnsignedLong row) {
		return rows.containsKey(row);
	}

	@Override
	public boolean exists(UnsignedLong row, String column) {
		return rows.get(row).containsKey(column);
	}

	@Override
	public boolean existsSpi(UnsignedLong row, String column,
			ConcourseValue value) {
		return columns.get(column).get(value).contains(row);
	}

	@Override
	protected Set<ConcourseValue> getSpi(UnsignedLong row, String column) {
		return rows.get(row).get(column);
	}

	@Override
	protected boolean removeSpi(UnsignedLong row, String column,
			ConcourseValue value) {
		if(exists(row, column, value)) {
			ExecutorService executor = Executors.newCachedThreadPool();

			executor.execute(new ColumnDeIndexer(row, column, value));
			executor.execute(new RowDeIndexer(row, column, value));
			executor.execute(new FullTextDeIndexer(row, column, value));

			return exists(row, column, value);
		}
		else {
			return false;
		}
	}

	@Override
	public Set<UnsignedLong> selectSpi(String column, Operator operator,
			List<ConcourseValue> values) {
		AbstractSelector selector = null;
		switch (operator) {
		case BETWEEN:
			selector = new BetweenSelector(column, values);
			break;
		case CONTAINS:
			selector = new ContainsSelector(column, values);
			break;
		case NOT_CONTAINS:
			selector = new NotContainsSelector(column, values);
			break;
		case EQUALS:
			selector = new EqualsSelector(column, values);
			break;
		case NOT_EQUALS:
			selector = new NotEqualsSelector(column, values);
			break;
		case GREATER_THAN:
			selector = new GreaterThanSelector(column, values);
			break;
		case GREATER_THAN_OR_EQUALS:
			selector = new GreaterThanOrEqualsSelector(column, values);
			break;
		case LESS_THAN:
			selector = new LessThanSelector(column, values);
			break;
		case LESS_THAN_OR_EQUALS:
			selector = new LessThanOrEqualsSelector(column, values);
			break;
		case REGEX:
			selector = new RegexSelector(column, values);
			break;
		case NOT_REGEX:
			selector = new NotRegexSelector(column, values);
			break;
		}

		ExecutorService executor = Executors.newCachedThreadPool();
		Set<UnsignedLong> result;
		try {
			result = executor.submit(selector).get();
		}
		catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			result = Sets.newTreeSet();
		}

		return result;
	}

	@Override
	public UnsignedLong size() {
		return UnsignedLong.valueOf(rows.size());
	}

	/**
	 * Base class for index modifying objects.
	 */
	private abstract class AbstractIndexModifier implements Runnable {

		protected final UnsignedLong row;
		protected final String column;
		protected final ConcourseValue value;

		/**
		 * Contrusct a new instance.
		 * 
		 * @param row
		 * @param column
		 * @param value
		 */
		AbstractIndexModifier(UnsignedLong row, String column,
				ConcourseValue value) {
			this.row = row;
			this.column = column;
			this.value = value;
		}
	}

	/**
	 * Base class for selector objects.
	 */
	private abstract class AbstractSelector implements
			Callable<TreeSet<UnsignedLong>> {
		protected final String column;
		protected final List<ConcourseValue> values;
		protected TreeSet<UnsignedLong> result;

		AbstractSelector(String column, List<ConcourseValue> values) {
			this.column = column;
			this.values = values;
			this.result = Sets.newTreeSet();
		}
	}

	/**
	 * Perform a {@link Operator#BETWEEN} selection.
	 */
	private class BetweenSelector extends AbstractSelector {

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param values
		 */
		BetweenSelector(String column, List<ConcourseValue> values) {
			super(column, values);
		}

		@Override
		public TreeSet<UnsignedLong> call() {
			ConcourseValue value1 = values.get(0);
			ConcourseValue value2 = values.get(1);
			SortedMap<ConcourseValue, TreeSet<UnsignedLong>> _values = columns
					.get(column).subMap(value1, true, value2, false);
			Iterator<ConcourseValue> it = _values.keySet().iterator();
			while (it.hasNext()) {
				result.addAll(_values.get(it.next()));
			}

			return result;
		}
	}

	/**
	 * A class that deletes column indexes.
	 */
	private class ColumnDeIndexer extends AbstractIndexModifier {

		/**
		 * Construct a new instance.
		 * 
		 * @param row
		 * @param column
		 * @param value
		 */
		ColumnDeIndexer(UnsignedLong row, String column, ConcourseValue value) {
			super(row, column, value);
		}

		@Override
		public void run() {
			columns.get(column).get(value).remove(row);
		}
	}

	/**
	 * A class that inserts column indexes.
	 */
	private class ColumnIndexer extends AbstractIndexModifier {

		/**
		 * Construct a new instance.
		 * 
		 * @param row
		 * @param column
		 * @param value
		 */
		ColumnIndexer(UnsignedLong row, String column, ConcourseValue value) {
			super(row, column, value);
		}

		@Override
		public void run() {
			TreeMap<ConcourseValue, TreeSet<UnsignedLong>> values;
			if(columns.containsKey(column)) {
				values = columns.get(column);
			}
			else {
				values = Maps.newTreeMap(logicalValueComparator);
				columns.put(column, values);
			}

			TreeSet<UnsignedLong> rows;
			if(values.containsKey(value)) {
				rows = values.get(value);
			}
			else {
				rows = Sets.newTreeSet(descendingRowComparator);
				values.put(value, rows);
			}
			rows.add(row);
		}
	}

	/**
	 * Perform a {@link Operator#CONTAINS} selection.
	 */
	private class ContainsSelector extends AbstractSelector {

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param values
		 */
		ContainsSelector(String column, List<ConcourseValue> values) {
			super(column, values);
		}

		@Override
		public TreeSet<UnsignedLong> call() {
			// TODO implement
			return result;
		}
	}

	/**
	 * Perform a {@link Operator#EQUALS} selection.
	 */
	private class EqualsSelector extends AbstractSelector {

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param values
		 */
		EqualsSelector(String column, List<ConcourseValue> values) {
			super(column, values);
		}

		@Override
		public TreeSet<UnsignedLong> call() {
			ConcourseValue value = values.get(0);
			result = columns.get(column).get(value);
			return result;
		}
	}

	/**
	 * A class that deletes fulltext indexes.
	 */
	private class FullTextDeIndexer extends AbstractIndexModifier {

		/**
		 * Construct a new instance.
		 * 
		 * @param row
		 * @param column
		 * @param value
		 */
		FullTextDeIndexer(UnsignedLong row, String column, ConcourseValue value) {
			super(row, column, value);
		}

		@Override
		public void run() {
			// TODO implement
		}
	}

	/**
	 * A class that inserts fulltext indexes.
	 */
	private class FullTextIndexer extends AbstractIndexModifier {

		/**
		 * Construct a new instance.
		 * 
		 * @param row
		 * @param column
		 * @param value
		 */
		FullTextIndexer(UnsignedLong row, String column, ConcourseValue value) {
			super(row, column, value);
		}

		@Override
		public void run() {
			// TODO implement
		}
	}

	/**
	 * Perform a {@link Operator#GREATER_THAN_OR_EQUALS} selection.
	 */
	private class GreaterThanOrEqualsSelector extends AbstractSelector {

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param values
		 */
		GreaterThanOrEqualsSelector(String column, List<ConcourseValue> values) {
			super(column, values);
		}

		@Override
		public TreeSet<UnsignedLong> call() {
			ConcourseValue value = values.get(0);
			SortedMap<ConcourseValue, TreeSet<UnsignedLong>> _values = columns
					.get(column).tailMap(value, true);
			Iterator<ConcourseValue> it = _values.keySet().iterator();
			while (it.hasNext()) {
				result.addAll(_values.get(it.next()));
			}

			return result;
		}
	}

	/**
	 * Perform a {@link Operator#GREATER_THAN} selection.
	 */
	private class GreaterThanSelector extends AbstractSelector {

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param values
		 */
		GreaterThanSelector(String column, List<ConcourseValue> values) {
			super(column, values);
		}

		@Override
		public TreeSet<UnsignedLong> call() {
			ConcourseValue value = values.get(0);
			SortedMap<ConcourseValue, TreeSet<UnsignedLong>> _values = columns
					.get(column).tailMap(value, false);
			Iterator<ConcourseValue> it = _values.keySet().iterator();
			while (it.hasNext()) {
				result.addAll(_values.get(it.next()));
			}

			return result;
		}
	}

	/**
	 * Perform a {@link Operator#LESS_THAN_OR_EQUALS} selection.
	 */
	private class LessThanOrEqualsSelector extends AbstractSelector {

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param values
		 */
		LessThanOrEqualsSelector(String column, List<ConcourseValue> values) {
			super(column, values);
		}

		@Override
		public TreeSet<UnsignedLong> call() {
			ConcourseValue value = values.get(0);
			SortedMap<ConcourseValue, TreeSet<UnsignedLong>> _values = columns
					.get(column).headMap(value, true);
			Iterator<ConcourseValue> it = _values.keySet().iterator();
			while (it.hasNext()) {
				result.addAll(_values.get(it.next()));
			}

			return result;
		}
	}

	/**
	 * Perform a {@link Operator#LESS_THAN} selection.
	 */
	private class LessThanSelector extends AbstractSelector {

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param values
		 */
		LessThanSelector(String column, List<ConcourseValue> values) {
			super(column, values);
		}

		@Override
		public TreeSet<UnsignedLong> call() {
			ConcourseValue value = values.get(0);
			SortedMap<ConcourseValue, TreeSet<UnsignedLong>> _values = columns
					.get(column).headMap(value, false);
			Iterator<ConcourseValue> it = _values.keySet().iterator();
			while (it.hasNext()) {
				result.addAll(_values.get(it.next()));
			}

			return result;
		}
	}

	/**
	 * Perform a {@link Operator#NOT_CONTAINS} selection.
	 */
	private class NotContainsSelector extends AbstractSelector {

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param values
		 */
		NotContainsSelector(String column, List<ConcourseValue> values) {
			super(column, values);
		}

		@Override
		public TreeSet<UnsignedLong> call() {
			// TODO implement
			return result;
		}
	}

	/**
	 * Perform a {@link Operator#NOT_EQUALS} selection.
	 */
	private class NotEqualsSelector extends AbstractSelector {

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param values
		 */
		NotEqualsSelector(String column, List<ConcourseValue> values) {
			super(column, values);
		}

		@Override
		public TreeSet<UnsignedLong> call() {
			ConcourseValue value = values.get(0);
			Map<ConcourseValue, TreeSet<UnsignedLong>> _values = columns
					.get(column);

			Iterator<ConcourseValue> it = _values.keySet().iterator();
			while (it.hasNext()) {
				ConcourseValue val = it.next();
				if(!val.equals(value)) {
					result.addAll(_values.get(val));
				}
			}

			return result;
		}
	}

	/**
	 * Perform a {@link Operator#NOT_REGEX} selection.
	 */
	private class NotRegexSelector extends AbstractSelector {

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param values
		 */
		NotRegexSelector(String column, List<ConcourseValue> values) {
			super(column, values);
		}

		@Override
		public TreeSet<UnsignedLong> call() {
			// TODO implement
			return result;
		}
	}

	/**
	 * Perform a {@link Operator#REGEX} selection.
	 */
	private class RegexSelector extends AbstractSelector {

		/**
		 * Construct a new instance.
		 * 
		 * @param column
		 * @param values
		 */
		RegexSelector(String column, List<ConcourseValue> values) {
			super(column, values);
		}

		@Override
		public TreeSet<UnsignedLong> call() {
			// TODO implement
			return result;
		}
	}

	/**
	 * A class that inserts row indexes.
	 */
	private class RowDeIndexer extends AbstractIndexModifier {

		/**
		 * Construct a new instance.
		 * 
		 * @param row
		 * @param column
		 * @param value
		 */
		RowDeIndexer(UnsignedLong row, String column, ConcourseValue value) {
			super(row, column, value);
		}

		@Override
		public void run() {
			rows.get(row).get(column).remove(value);
		}
	}

	/**
	 * A class that inserts row indexes.
	 */
	private class RowIndexer extends AbstractIndexModifier {

		/**
		 * Construct a new instance.
		 * 
		 * @param row
		 * @param column
		 * @param value
		 */
		RowIndexer(UnsignedLong row, String column, ConcourseValue value) {
			super(row, column, value);
		}

		@Override
		public void run() {
			HashMap<String, TreeSet<ConcourseValue>> columns;
			if(rows.containsKey(row)) {
				columns = rows.get(row);
			}
			else {
				columns = Maps.newHashMap();
				rows.put(row, columns);
			}

			TreeSet<ConcourseValue> values;
			if(columns.containsKey(column)) {
				values = columns.get(column);
			}
			else {
				values = Sets.newTreeSet(descendingTimeValueComparator);
				columns.put(column, values);
			}
			values.add(value);
		}
	}
}
