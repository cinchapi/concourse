/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.storage;

/**
 * A {@link Store} that provides mechanisms to revert data to previous states.
 * 
 * @author jnelson
 */
public interface VersionControlStore extends Store {

	/**
	 * Revert {@code key} in {@code record} to {@code timestamp}.
	 * <p>
	 * This method returns {@code key} in {@code record} to its previous state
	 * at {@code timestamp} by reversing all revisions in the field that have
	 * occurred since. This method <em>does not rollback</em> any revisions, but
	 * creates new revisions that are the reverse of the reverted revisions:
	 * <table>
	 * <tr>
	 * <th>Time</th>
	 * <th>Revision</th>
	 * </tr>
	 * <tr>
	 * <td>T1</td>
	 * <td>ADD A</td>
	 * </tr>
	 * <tr>
	 * <td>T2</td>
	 * <td>ADD B</td>
	 * </tr>
	 * <tr>
	 * <td>T3</td>
	 * <td>REMOVE A</td>
	 * </tr>
	 * <tr>
	 * <td>T4</td>
	 * <td>ADD C</td>
	 * </tr>
	 * <tr>
	 * <td>T5</td>
	 * <td>REMOVE C</td>
	 * </tr>
	 * <tr>
	 * <td>T6</td>
	 * <td>REMOVE B</td>
	 * </tr>
	 * <tr>
	 * <td>T7</td>
	 * <td>ADD D</td>
	 * </tr>
	 * </table>
	 * In the example above, after {@code T7}, the field contains value
	 * {@code D}. If the field is reverted to T3, the following new revisions
	 * are added:
	 * <table>
	 * <tr>
	 * <th>Time</th>
	 * <th>Revision</th>
	 * </tr>
	 * <tr>
	 * <td>T8</td>
	 * <td>REMOVE D</td>
	 * </tr>
	 * <tr>
	 * <td>T9</td>
	 * <td>ADD B</td>
	 * </tr>
	 * <tr>
	 * <td>T10</td>
	 * <td>ADD C</td>
	 * </tr>
	 * <tr>
	 * <td>T11</td>
	 * <td>REMOVE C</td>
	 * </tr>
	 * </table>
	 * After {@code T11}, the field contains value {@code B}. Regardless of the
	 * current state, ever revision to the field exists in history so it is
	 * possible to revert to any previous state, even after reverting to a much
	 * earlier state (i.e. after reverting to {@code T3} it is possible to
	 * revert to {@code T5}).
	 * </p>
	 * 
	 * @param key
	 * @param record
	 * @param timestamp
	 */
	public void revert(String key, long record, long timestamp);

}
