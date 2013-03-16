/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.concourse.cal.statement;

import java.text.ParseException;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.common.Strings;
import com.cinchapi.concourse.services.QueryableService.Operator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <ul>
 * <li>column = value</li>
 * <li>column != value</li>
 * <li>column > value</li>
 * <li>column >= value</li>
 * <li>column < value</li>
 * <li>column <= value</li>
 * <li>column REGEX value</li>
 * <li>column NOT REGEX value</li>
 * <li>column CONTAINS value</li>
 * <li>column BETWEEN value1 value2</li>
 * </ul>
 * 
 * @author jnelson
 */
@Immutable
public class Query {

	public static void main(String[] args) throws ParseException {
		String query = "name>\"jeff nelson\" AND age <= 12 (OR years BETWEEN 3 7 AND column contains 'cool string right here') AND boolean = true OR ash REGEX [\\d]* AND phil = \"duncontains\" OR chris > \"<\"";
		System.out.println(Query.fromString(query));
		System.out
				.println(Query
						.fromString("name = jeff AND (age > 3 OR (range between 7 9 AND time CONTAINS 1))"));
	}

	/**
	 * Return a new Query from a string.
	 * 
	 * @param query
	 * @return the Query
	 * @throws ParseException
	 */
	public static Query fromString(String query) throws ParseException {
		return new Query(Query.toReversePolishNotation(query));
	}

	/**
	 * Convert a string to Reverse Polish Notation.
	 * 
	 * @param query
	 * @return a RPN queue
	 * @throws ParseException
	 * @see http://en.wikipedia.org/wiki/Reverse_Polish_notation
	 */
	private static Queue<Component> toReversePolishNotation(String query)
			throws ParseException {
		// This method uses the Shunting Yard Algorithm:
		// http://en.wikipedia.org/wiki/Shunting-yard_algorithm
		Queue<Component> queue = Lists.newLinkedList();
		for (Operator operator : Operator.values()) { // remove spaces that
														// surround query
														// operator to
														// prevent split
			query = query.replaceAll("(?i)[\\s]*(" + operator + ")[\\s]*",
					operator.toString().toUpperCase());
		}
		query = query.replaceAll("(\\(|\\))", " $1 ");
		query = query.replaceAll("(" + Operator.BETWEEN
				+ ")([\\w\\d]+[\\s]+[\\w\\d]+)", "$1\"$2\""); // adding
																// quotes
																// around
																// values
																// to
																// the
																// BETWEEN
																// operator
																// to
																// prevent
																// split
		List<String> toks = Strings.tokenizeKeepQuotes(query);
		int i = 0;
		while (i < toks.size()) { // check to see if the token ends with an
									// operator, if so join it with the next
									// token. I'm pretty sure I could do this
									// with regex, but this gets the job done
			String tok = toks.get(i);
			for (Operator operator : Operator.values()) {
				String regex = ".*" + operator + "$";
				if(tok.matches(regex) && i < toks.size() - 1) {
					toks.remove(i);
					toks.add(i, tok + toks.get(i));
					toks.remove(i + 1);
					break;
				}
			}
			i++;
		}
		Stack<Component> stack = new Stack<Component>();
		try {
			for (String tok : toks) {
				if(Conjunction.AND.matches(tok) || Conjunction.OR.matches(tok)) {
					while (!stack.isEmpty()) {
						Component topOfStack = stack.peek();
						if(Conjunction.OR.matches(tok)
								&& Conjunction.OR.matches(topOfStack)
								|| Conjunction.AND.matches(topOfStack)) {
							queue.offer(stack.pop());
						}
						else {
							break;
						}
					}
					stack.push(Component.fromString(tok));
				}
				else if(tok.equalsIgnoreCase("(")) {
					stack.push(Component.forLeftParenthesis());
				}
				else if(tok.equalsIgnoreCase(")")) {
					boolean foundLeftParen = false;
					while (!stack.isEmpty()) {
						Component topOfStack = stack.peek();
						if(Parenthesis.LEFT.matches(topOfStack)) {
							foundLeftParen = true;
							break;
						}
						else {
							queue.offer(stack.pop());
						}
					}
					if(!foundLeftParen) {
						throw new ParseException("Syntax error in query '"
								+ query + "', mismatched parenthesis", 0);
					}
					stack.pop(); // get rid of parenthesis
				}
				else {
					queue.offer(Component.fromString(tok));
				}
			}
			while (!stack.isEmpty()) {
				Component topOfStack = stack.peek();
				if(Parenthesis.RIGHT.matches(topOfStack)
						|| Parenthesis.LEFT.matches(topOfStack)) {
					throw new ParseException("Syntax error in query '" + query
							+ "', mismatched parenthesis", 0);
				}
				queue.offer(stack.pop());
			}
		}
		catch (ParseException e) {
			throw e;
		}
		return queue;
	}

	private final Queue<Component> components;

	/**
	 * Construct a new instance.
	 * 
	 * @param components
	 */
	private Query(Queue<Component> components) {
		this.components = components;
	}

	/**
	 * Return the components of the query.
	 * 
	 * @return the components
	 */
	public Queue<Component> getComponents() {
		return components;
	}

	@Override
	public String toString() {
		return Strings.toString(this);
	}

	/**
	 * A component can either be a single {@link Conjunction} (operator) or a
	 * collection of columns, (query) operators and values that represent an
	 * operand.
	 */
	@Immutable
	public static class Component {

		/**
		 * Return a Conjunction (operator) component.
		 * 
		 * @param conjunction
		 * @return the component
		 */
		public static Component forConjunction(Conjunction conjunction) {
			return new Component(conjunction, EMPTY_PARENTHESIS, EMPTY_COLUMN,
					EMPTY_OPERATOR, EMPTY_VALUES);
		}

		/**
		 * Return an operand component.
		 * 
		 * @param column
		 * @param operator
		 * @param values
		 * @return the component
		 */
		public static Component forOperand(String column, Operator operator,
				Object... values) {
			return new Component(EMPTY_CONJUNCTION, EMPTY_PARENTHESIS, column,
					operator, values);
		}

		/**
		 * Return a component for a left parenthesis.
		 * 
		 * @return the component
		 */
		public static Component forLeftParenthesis() {
			return new Component(EMPTY_CONJUNCTION, Parenthesis.LEFT,
					EMPTY_COLUMN, EMPTY_OPERATOR, EMPTY_VALUES);
		}

		/**
		 * Return a component for a right parenthesis.
		 * 
		 * @return the component
		 */
		public static Component forRightParenthesis() {
			return new Component(EMPTY_CONJUNCTION, Parenthesis.RIGHT,
					EMPTY_COLUMN, EMPTY_OPERATOR, EMPTY_VALUES);
		}

		/**
		 * Return a Component from parsing a string
		 * 
		 * @param string
		 * @return the component
		 * @throws ParseException
		 */
		public static Component fromString(String string) throws ParseException {
			if(Conjunction.AND.matches(string)) {
				return Component.forConjunction(Conjunction.AND);
			}
			else if(Conjunction.OR.matches(string)) {
				return Component.forConjunction(Conjunction.OR);
			}
			else {
				for (Operator operator : Operator.values()) {
					int start = string.indexOf("" + operator + "");
					int end = start + operator.toString().length();
					if(start > 0
							&& !string.substring(end, end).equalsIgnoreCase(
									"" + Operator.EQUALS + "")) { // prevent
																	// splitting
																	// <= or >=
						String column = string.substring(0, start);
						List<String> valToks;
						String value = string.substring(end, string.length());
						if(operator == Operator.BETWEEN) {
							valToks = Strings.tokenizeKeepQuotes(value);
						}
						else {
							valToks = Lists.newArrayList();
							valToks.add(value);
						}
						Object[] values = valToks.toArray();
						return Component.forOperand(column, operator, values);
					}
				}

			}
			throw new ParseException(
					"An error occured while trying to parse a Query component from the token '"
							+ string + "'", 0);
		}

		private static final String EMPTY_COLUMN = null;
		private static final Operator EMPTY_OPERATOR = null;
		private static final Object[] EMPTY_VALUES = null;
		private static final Conjunction EMPTY_CONJUNCTION = null;
		private static final Parenthesis EMPTY_PARENTHESIS = null;

		private final String column;
		private final Operator operator;
		private Object[] values;
		private final Conjunction conjunction;
		private final Parenthesis parenthesis;

		/**
		 * Construct a new instance.
		 * 
		 * @param conjunction
		 * @param parenthesis
		 * @param column
		 * @param operator
		 * @param values
		 */
		private Component(Conjunction conjunction, Parenthesis parenthesis,
				String column, Operator operator, Object... values) {
			if(conjunction != EMPTY_CONJUNCTION) {
				Preconditions.checkArgument(column == EMPTY_COLUMN,
						"A conjunction component cannot specify a column");
				Preconditions.checkArgument(operator == EMPTY_OPERATOR,
						"A conjunction component cannot specify a operator");
				Preconditions.checkArgument(values == EMPTY_VALUES,
						"A conjunction component cannot specify any values");
				Preconditions.checkArgument(parenthesis == EMPTY_PARENTHESIS,
						"A conjunction component cannot specify a parenthesis");
			}
			else if(parenthesis != EMPTY_PARENTHESIS) {
				Preconditions.checkArgument(column == EMPTY_COLUMN,
						"A parenthesis component cannot specify a column");
				Preconditions.checkArgument(operator == EMPTY_OPERATOR,
						"A parenthesis component cannot specify a operator");
				Preconditions.checkArgument(values == EMPTY_VALUES,
						"A parenthesis component cannot specify any values");
				Preconditions
						.checkArgument(conjunction == EMPTY_CONJUNCTION,
								"An parenthesis component cannot specify a conjunction");
			}
			else {
				Preconditions.checkArgument(conjunction == EMPTY_CONJUNCTION,
						"An operand component cannot specify a conjunction");
				Preconditions.checkArgument(parenthesis == EMPTY_PARENTHESIS,
						"An operand component cannot specify a parenthesis");
				Preconditions.checkArgument(column != EMPTY_COLUMN,
						"An operand component MUST specify a column");
				Preconditions.checkArgument(operator != EMPTY_OPERATOR,
						"An operand component MUST specify an operator");
				Preconditions.checkArgument(values != EMPTY_VALUES,
						"An operand component MUST specify values");
			}
			this.conjunction = conjunction;
			this.parenthesis = parenthesis;
			this.column = column;
			this.operator = operator;
			this.values = values;

		}

		/**
		 * Return the column
		 * 
		 * @return the column
		 */
		public String getColumn() {
			Preconditions.checkState(isOperand(),
					"Cannot return a column for a conjunction or parenthesis");
			return column;
		}

		/**
		 * Return the conjunction
		 * 
		 * @return the conjunction
		 */
		public Conjunction getConjunction() {
			Preconditions
					.checkState(isConjunction(),
							"Cannot return a conjunction for an operand or parenthesis");
			return conjunction;
		}

		/**
		 * Return the operator
		 * 
		 * @return the operator
		 */
		public Operator getOperator() {
			Preconditions
					.checkState(isOperand(),
							"Cannot return an operator for a conjunction or parenthesis");
			return operator;
		}

		/**
		 * Return the values
		 * 
		 * @return the values
		 */
		public Object[] getValues() {
			Preconditions.checkState(isOperand(),
					"Cannot return values for a conjunction or parenthesis");
			return values;
		}

		/**
		 * Return the parenthesis
		 * 
		 * @return the parenthesis
		 */
		public Parenthesis getParenthesis() {
			Preconditions.checkState(isParenthesis(),
					"Cannot return a parenthesis for a conjunction or operand");
			return parenthesis;
		}

		/**
		 * Return {@code true} if the component encapsulates a conjunction
		 * 
		 * @return {@code true} if this is a conjunction
		 */
		public boolean isConjunction() {
			return conjunction != EMPTY_CONJUNCTION;
		}

		/**
		 * Return {@code true} if the component encapsulates a parenthesis
		 * 
		 * @return {@code true} if this is a parenthesis
		 */
		public boolean isParenthesis() {
			return parenthesis != EMPTY_PARENTHESIS;
		}

		/**
		 * Return {@code true} if the component encapsulates an operand
		 * 
		 * @return {@code true} if this is an operand
		 */
		public boolean isOperand() {
			return !isConjunction();
		}

		@Override
		public String toString() {
			return Strings.toString(this);
		}
	}

	/**
	 * Parenthesis that can go into a component
	 */
	public enum Parenthesis {
		LEFT, RIGHT;

		@Override
		public String toString() {
			if(this == LEFT) {
				return "(";
			}
			else {
				return ")";
			}
		}

		/**
		 * Return {@code true} if the string matches this parenthesis.
		 * 
		 * @param string
		 * @return {@code true} if the string matches
		 */
		public boolean matches(String string) {
			return this.toString().equalsIgnoreCase(string);
		}

		/**
		 * Return {@code true} if the component matches this parenthesis.
		 * 
		 * @param component
		 * @return {@code true} if the component matches
		 */
		public boolean matches(Component component) {
			try {
				return component.getParenthesis() == this;
			}
			catch (IllegalStateException e) {
				return false;
			}
		}
	}

	/**
	 * The coordinating conjunctions that determine how to operate on the
	 * results from a clause/operand. Essentially, one of these
	 * operators determines if the true result set is the union or intersection
	 * of two distinct result sets.
	 */
	public enum Conjunction { // these are not called Operator so as to not
								// ambiguate with
								// com.cinchapi.concourse.store.api.Queryable.Operator,
								// but they should be thought of as operators on
								// the result set between two components
		AND,
		OR;

		@Override
		public String toString() {
			return name();
		}

		/**
		 * Return {@code true} if the string matches the conjunction.
		 * 
		 * @param string
		 * @return {@code true} if the string matches
		 */
		public boolean matches(String string) {
			return string.equalsIgnoreCase(toString());
		}

		/**
		 * Return {@code true} if the component matches the conjunction
		 * 
		 * @param component
		 * @return {@code true} if the component matches
		 */
		public boolean matches(Component component) {
			try {
				return component.getConjunction() == this;
			}
			catch (IllegalStateException e) {
				return false;
			}
		}
	}

}
