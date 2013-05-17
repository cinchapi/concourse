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
package com.cinchapi.concourse.internal;

import java.util.Comparator;

import com.cinchapi.common.math.Numbers;

/**
 * A {@link Comparator} that sorts {@link Value} objects logically using weak
 * typing.
 * 
 * @see {@link Value#compareToLogically(Value)}
 * @author jnelson
 */
class WeakTypingValueComparator implements Comparator<Value> {

	@Override
	public int compare(Value o1, Value o2) {
		if(o1.getQuantity() instanceof Number
				&& o2.getQuantity() instanceof Number) {
			return Numbers.compare((Number) o1.getQuantity(),
					(Number) o2.getQuantity());
		}
		else {
			return o1.getQuantity().toString()
					.compareTo(o2.getQuantity().toString());
		}
	}

}