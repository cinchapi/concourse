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
package com.cinchapi.concourse.cal;

import org.apache.commons.lang.math.NumberUtils;

import com.cinchapi.concourse.services.ConcourseService;
import com.google.common.base.Preconditions;

/**
 * <p>
 * The FSM that is used for validating and parsing a CAL statement.
 * </p>
 * <p>
 * <strong>Expected Formats:</strong>
 * <ul>
 * <li>ADD column as value (, column2 as value2)* IN row</li>
 * <li>SET column as value (, column2 as value2)* IN row</li>
 * <li>REMOVE column as value (, column2 as value2)* IN row</li>
 * </ul>
 * 
 * @author jnelson
 */
public class FiniteStateMachine {

	/**
	 * Return a new {@link FiniteStateMachine} at {@link State#START}.
	 * 
	 * @return the machine
	 */
	public static FiniteStateMachine atStartState() {
		return new FiniteStateMachine();
	}

	private State state = State.START;
	private String input = "";

	private FiniteStateMachine() {} // private constructor

	/**
	 * Return the current state of the machine.
	 * 
	 * @return the current state
	 */
	public State getState() {
		return state;
	}

	/**
	 * Return the input that produced the current state.
	 * 
	 * @return the current input
	 */
	public String getInput() {
		return input;
	}

	/**
	 * Consume {@code input} and transition to the next state.
	 * 
	 * @param input
	 * @return the next state.
	 */
	public void consume(String input) {
		Preconditions.checkState(state != State.END,
				"Cannot consume additional input while in the {} state",
				State.END);
		state = nextState(input, state); // throws IllegalArgumentException
		this.input = input;
	}

	/**
	 * Return the next state that is reached when consuming {@code input} in the
	 * current {@link #state}.
	 * 
	 * @param input
	 * @param current
	 * @return the next state
	 * @throws IllegalArgumentException
	 *             if there is no rule such that CURRENT --> input NEXT
	 */
	private State nextState(String input, State current)
			throws IllegalArgumentException {
		State next = State.INVALID;
		if(current == State.START) {
			if(Token.ADD.matches(input) || Token.REMOVE.matches(input)
					|| Token.SET.matches(input)) {
				next = State.EXPECTING_COLUMN_V;
			}
		}
		else if(current == State.EXPECTING_COLUMN_V) {
			if(ConcourseService.checkColumnName(input)) { // throws
															// IllegalArgumentException
				next = State.AT_COLUMN_V;
			}
		}
		else if(current == State.AT_COLUMN_V) {
			if(Token.AS.matches(input)) {
				next = State.AT_AS_T;
			}
		}
		else if(current == State.AT_AS_T) {
			next = State.AT_VALUE_V;
		}
		else if(current == State.AT_VALUE_V) {
			if(Token.AND.matches(input)) {
				next = State.EXPECTING_COLUMN_V;
			}
			else if(Token.IN.matches(input)) {
				next = State.AT_IN_T;
			}
		}
		else if(current == State.AT_IN_T) {
			if(NumberUtils.isDigits(input)) { // expecting a row key
				next = State.END;
			}
		}
		Preconditions
				.checkArgument(
						next != State.INVALID,
						"Could not transition to another state from %s with an input of '%s'",
						current, input);
		return next;
	}

	/**
	 * A list of tokens used by the machine.
	 * 
	 * @author jnelson
	 */
	public static enum Token {
		ADD("ADD"),
		REMOVE("REMOVE"),
		SET("SET"),
		AND("AND"),
		IN("IN"),
		AS("AS");

		private final String rep;

		private Token(String rep) {
			this.rep = rep;
		}

		/**
		 * Return {@code true} if {@code string} matches the expecation of
		 * the token.
		 * 
		 * @param string
		 * @return {@code true} if the {@code string} and {@code token} "match"
		 */
		public boolean matches(String string) {
			return string.equalsIgnoreCase(toString());
		}

		@Override
		public String toString() {
			return rep;
		}
	}

	/**
	 * A list of states used in the machine.
	 * 
	 * @author jnelson
	 */
	public static enum State {
		START,
		END,
		INVALID,
		EXPECTING_COLUMN_V,
		AT_COLUMN_V,
		AT_AS_T,
		AT_VALUE_V,
		AT_AND_T,
		AT_IN_T;

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("{");
			if(ordinal() == START.ordinal()) {
				sb.append("The initial state");
			}
			else if(ordinal() == END.ordinal()) {
				sb.append("Unable to consume additional input");
			}
			else if(ordinal() == INVALID.ordinal()) {
				sb.append("An invalid state");
			}
			else if(ordinal() == EXPECTING_COLUMN_V.ordinal()) {
				sb.append("Expecting next input to be a COLUMN variable");
			}
			else {
				/*
				 * Each State name is in the form AT_INPUT_TYPE and has three
				 * parts:
				 * 0 --> AT
				 * 1 --> the last INPUT that was read
				 * 2 --> the type of INPUT last read
				 * ----------> T indicates a token or static input
				 * ----------> V indicates a variable or dynamic input
				 */
				String[] parts = name().split("_");
				sb.append("Last consumed");
				sb.append(" ");
				sb.append(parts[1]);
				sb.append(" ");
				sb.append(parts[2].equals("T") ? "token" : "variable");
			}
			sb.append("}");
			return sb.toString();
		}

	}

}
