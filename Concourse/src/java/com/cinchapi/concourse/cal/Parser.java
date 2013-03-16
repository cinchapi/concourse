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

import java.text.ParseException;
import java.util.List;

import com.cinchapi.concourse.cal.FiniteStateMachine.State;

/**
 * 
 * 
 * @author jnelson
 */
public class Parser {

	private static final String ERROR_HINT = "-->";

	/**
	 * Parse the {@code statement} and return the corresponding
	 * {@link Statement} object.
	 * 
	 * @param statement
	 * @return the parsed {@link Statement}
	 * @throws ParseException
	 */
	public Statement parse(String statement)
			throws ParseException {
		List<String> tokens = com.cinchapi.common.Strings
				.tokenizeKeepQuotes(statement);
		FiniteStateMachine machine = FiniteStateMachine.atStartState();
		for (String token : tokens) {
			try {
				machine.consume(token);
				State state = machine.getState();
				String input = machine.getInput();
				// TODO build the statement
			}
			catch (IllegalArgumentException e) {
				int pos = statement.indexOf(token);
				String hint = statement.substring(0, pos) + ERROR_HINT
						+ statement.substring(pos, statement.length());
				throw new ParseException(
						"There was an error in the syntax of the statement near '"
								+ token + "' at position " + pos + ": '" + hint
								+ "'", pos);
			}

		}
		return null;
	}

	public static void main(String[] args) throws ParseException {
		String cal = "REMOVE name as \"jeff nelson\" AND age as 23 AND age as 25 FOR 14";
		System.out.println(new Parser().parse(cal));
	}

}
