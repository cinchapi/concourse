package com.cinchapi.concourse.model;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cinchapi.util.search.Indexer;
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
public class HeapBasedConcourse extends AbstractConcourse {

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
	 * Maps index to columns to to rows to values. Everything is hash sorted.
	 */
	private Map<String, HashMap<String, HashMap<UnsignedLong, HashSet<ConcourseValue>>>> fulltext;

	/**
	 * Sorts <code>values</code> in descending order based on timestamp.
	 */
	private static Comparator<ConcourseValue> descendingTimeValueComparator = new Comparator<ConcourseValue>() {

		@Override
		public int compare(ConcourseValue o1, ConcourseValue o2) {
			if(o1.getTimestamp().equals(ConcourseValue.EMPTY_TIMESTAMP)
					|| o2.getTimestamp().equals(ConcourseValue.EMPTY_TIMESTAMP)) {
				// this means a "comparison" value is being compared to a
				// "stored" value
				return o1.equals(o2) ? 0 : -1;
			}
			else {
				return -1 * o1.getTimestamp().compareTo(o2.getTimestamp());
			}
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
				Number _o1 = (Number) o1.getValue();
				Number _o2 = (Number) o2.getValue();
				if(_o1 instanceof Integer) {
					return Integer.valueOf(_o1.intValue()).compareTo(
							_o2.intValue());
				}
				else if(_o1 instanceof Double) {
					return Double.valueOf(_o1.doubleValue()).compareTo(
							_o2.doubleValue());
				}
				else if(_o1 instanceof Float) {
					return Float.valueOf(_o1.floatValue()).compareTo(
							_o2.floatValue());
				}
				else if(_o1 instanceof Long) {
					return Long.valueOf(_o1.longValue()).compareTo(
							_o2.longValue());
				}
				else {
					return Integer.valueOf(_o1.intValue()).compareTo(
							_o2.intValue());
				}
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
	public HeapBasedConcourse(int expectedNumColumnsPerRow) {
		this.rows = Maps.newTreeMap(descendingRowComparator);
		this.columns = Maps
				.newHashMapWithExpectedSize(expectedNumColumnsPerRow);
		this.fulltext = Maps.newHashMap();
	}

	@Override
	protected boolean addSpi(UnsignedLong row, String column,
			ConcourseValue value) {
		if(!exists(row, column, value)) {
			ExecutorService executor = Executors.newCachedThreadPool();

			executor.execute(new ColumnIndexer(row, column, value));
			executor.execute(new RowIndexer(row, column, value));
			executor.execute(new FullTextIndexer(row, column, value));
			executor.shutdown();
			try {
				// try to avoid race conditions where certain threads finish
				// before others and prevent some of the indexing from occuring
				executor.awaitTermination(1, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}

			return existsSpi(row, column, value);
		}
		else {
			return false;
		}
	}

	@Override
	public Set<String> describe(UnsignedLong row) {
		return exists(row) ? rows.get(row).keySet() : new TreeSet<String>();
	}

	@Override
	public boolean exists(UnsignedLong row) {
		return rows.containsKey(row);
	}

	@Override
	public boolean exists(UnsignedLong row, String column) {
		return exists(row) ? rows.get(row).containsKey(column) : false;
	}

	@Override
	public boolean existsSpi(UnsignedLong row, String column,
			ConcourseValue value) {
		if(columns.containsKey(column)) {
			TreeMap<ConcourseValue, TreeSet<UnsignedLong>> values = columns
					.get(column);
			return values.containsKey(value) ? values.get(value).contains(row)
					: false;
		}
		else {
			return false;
		}
	}

	@Override
	protected Set<ConcourseValue> getSpi(UnsignedLong row, String column) {
		return exists(row, column) ? rows.get(row).get(column)
				: new TreeSet<ConcourseValue>();
	}

	@Override
	protected boolean removeSpi(UnsignedLong row, String column,
			ConcourseValue value) {
		if(existsSpi(row, column, value)) {
			ExecutorService executor = Executors.newCachedThreadPool();

			executor.execute(new ColumnDeIndexer(row, column, value));
			executor.execute(new RowDeIndexer(row, column, value));
			executor.execute(new FullTextDeIndexer(row, column, value));
			executor.shutdown();
			try {
				// try to avoid race conditions where certain threads finish
				// before others and prevent some of the deindexing from
				// occuring
				executor.awaitTermination(1, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}

			return !existsSpi(row, column, value);
		}
		else {
			return false;
		}
	}

	@Override
	protected Set<UnsignedLong> rowSet() {
		return rows.keySet();
	}

	@Override
	protected Set<UnsignedLong> selectSpi(String column, Operator operator,
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
		executor.shutdown();
		try {
			// try to avoid race conditions where certain threads finish
			// before others and prevent certain selections from finishing
			executor.awaitTermination(1, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
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
			Iterator<Entry<ConcourseValue, TreeSet<UnsignedLong>>> it = _values
					.entrySet().iterator();
			while (it.hasNext()) {
				result.addAll(it.next().getValue());
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
			if(columns.get(column).get(value).isEmpty()) {
				columns.get(column).remove(value);
			}
			if(columns.get(column).isEmpty()) {
				columns.remove(column);
			}
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
			String value = values.get(0).toString();
			result.addAll(fulltext.get(value).get(column).keySet());
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
			Iterator<String> indexes = fulltext.keySet().iterator();
			while(indexes.hasNext()){
				String index = indexes.next();
				if(fulltext.get(index).containsKey(column) && fulltext.get(index).get(column).containsKey(row)){
					Iterator<ConcourseValue> values = fulltext.get(index).get(column).get(row).iterator();
					while(values.hasNext()){
						if(values.next().equals(value)){
							values.remove();
						}
					}
				}
			}
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
			Iterator<String> it = Indexer.index(value.toString()).values()
					.iterator();
			while (it.hasNext()) {
				String index = it.next();

				HashMap<String, HashMap<UnsignedLong, HashSet<ConcourseValue>>> columns;
				if(fulltext.containsKey(index)) {
					columns = fulltext.get(index);
				}
				else {
					columns = Maps.newHashMap();
					fulltext.put(index, columns);
				}

				HashMap<UnsignedLong, HashSet<ConcourseValue>> rows;
				if(columns.containsKey(column)) {
					rows = columns.get(column);
				}
				else {
					rows = Maps.newHashMap();
					columns.put(column, rows);
				}

				HashSet<ConcourseValue> values;
				if(rows.containsKey(row)) {
					values = rows.get(row);
				}
				else {
					values = Sets.newHashSet();
					rows.put(row, values);
				}

				values.add(value);
			}
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
			Iterator<Entry<ConcourseValue, TreeSet<UnsignedLong>>> it = _values
					.entrySet().iterator();
			while (it.hasNext()) {
				result.addAll(it.next().getValue());
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
			Iterator<Entry<ConcourseValue, TreeSet<UnsignedLong>>> it = _values
					.entrySet().iterator();
			while (it.hasNext()) {
				result.addAll(it.next().getValue());
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
			Iterator<Entry<ConcourseValue, TreeSet<UnsignedLong>>> it = _values
					.entrySet().iterator();
			while (it.hasNext()) {
				result.addAll(it.next().getValue());
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
			Iterator<Entry<ConcourseValue, TreeSet<UnsignedLong>>> it = _values
					.entrySet().iterator();
			while (it.hasNext()) {
				result.addAll(it.next().getValue());
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
			Iterator<String> it = fulltext.keySet().iterator();
			String value = values.get(0).toString();
			while (it.hasNext()) {
				String index = it.next();
				if(!value.equals(index)) {
					result.addAll(fulltext.get(index).get(column).keySet());
				}
			}
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
			Iterator<Entry<ConcourseValue, TreeSet<UnsignedLong>>> it = _values
					.entrySet().iterator();
			while (it.hasNext()) {
				Entry<ConcourseValue, TreeSet<UnsignedLong>> entry = it.next();
				if(entry.getKey().equals(value)) {
					result.addAll(entry.getValue());
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
			String regex = values.get(0).toString();
			TreeMap<ConcourseValue, TreeSet<UnsignedLong>> _values = columns
					.get(column);
			Iterator<Entry<ConcourseValue, TreeSet<UnsignedLong>>> it = _values
					.entrySet().iterator();
			while (it.hasNext()) {
				Entry<ConcourseValue, TreeSet<UnsignedLong>> entry = it.next();
				Pattern p = Pattern.compile(regex);
				Matcher m = p.matcher(entry.getValue().toString());
				if(!m.matches()) {
					result.addAll(entry.getValue());
				}
			}

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
			String regex = values.get(0).toString();
			TreeMap<ConcourseValue, TreeSet<UnsignedLong>> _values = columns
					.get(column);
			Iterator<Entry<ConcourseValue, TreeSet<UnsignedLong>>> it = _values
					.entrySet().iterator();
			while (it.hasNext()) {
				Entry<ConcourseValue, TreeSet<UnsignedLong>> entry = it.next();
				Pattern p = Pattern.compile(regex);
				Matcher m = p.matcher(entry.getValue().toString());
				if(m.matches()) {
					result.addAll(entry.getValue());
				}
			}

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
			if(rows.get(row).get(column).isEmpty()) {
				rows.get(row).remove(column);
			}
			if(rows.get(row).isEmpty()) {
				rows.remove(row);
			}
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
