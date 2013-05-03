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
package com.cinchapi.concourse.db;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.cinchapi.common.math.Numbers;
import com.cinchapi.common.time.Time;
import com.cinchapi.concourse.io.ByteSized;
import com.google.common.collect.Lists;

/**
 * Unit tests for {@link Cell}
 * 
 * @author jnelson
 */
public abstract class CellTest<I extends ByteSized, O extends Storable> extends
		DatabaseTest {

	@Test
	public void testAdd() throws Exception {
		Cell<I, O> cell = newInstance();
		O forStorage = forStorageObject();
		O notForStorage = notForStorageObject();

		// Can add a forStorage object if it does not exist and i get no
		// exception
		cell.add(forStorage);

		// Should get exception for trying to add a object that already exists
		try {
			cell.add(forStorage);
			fail("Was expecting an IllegalStateException");
		}
		catch (IllegalStateException e) {}

		// Cannot add a notForStorage object
		try {
			cell.add(notForStorage);
			fail("Was expecting an IllegalArgumentException");
		}
		catch (IllegalArgumentException e) {}

	}

	@Test
	public void testContains() {
		O v = forStorageObject();
		Cell<I, O> cell = newInstance();
		assertFalse(cell.exists(v));
		cell.add(v);
		assertTrue(cell.exists(v));
	}

	@Test
	public void testCount() {
		Cell<I, O> cell = newInstance();
		int count = randomScaleFrequency();
		for (int i = 0; i < count; i++) {
			int currentCount = cell.count();
			O v = forStorageObject();
			while (cell.exists(v)) {
				v = forStorageObject();
			}
			cell.add(v);
			assertEquals(currentCount + 1, cell.count());
		}

		while (!cell.fetch().isEmpty()) {
			int currentCount = cell.count();
			cell.remove(copy(cell.fetch().get(0)));
			assertEquals(currentCount - 1, cell.count());
		}
	}

	@Test
	public void testGetBytes() {
		Cell<I, O> c1 = populatedInstance();
		ByteBuffer bytes = ByteBuffer.wrap(c1.getBytes());
		Cell<I, O> c2 = populatedInstanceFromBytes(bytes);

		// NOTE: Cell does not define equals() or hashCode()
		assertEquals(c1.getId(), c2.getId());
		assertEquals(c1.fetch(), c2.fetch());
	}

	@Test
	public void testGetId() {
		I id = id();
		Cell<I, O> cell = newInstance(id);
		assertEquals(id, cell.getId());
	}

	@Test
	public void testGetValues() {
		Cell<I, O> cell = newInstance();
		int scale = randomScaleFrequency();
		List<O> objects = Lists.newArrayListWithCapacity(scale);
		for (int i = 0; i < scale; i++) {
			O object = null;
			while (object == null || cell.exists(object)) {
				object = forStorageObject();
			}
			cell.add(object);
			objects.add(object);
			assertEquals(objects, cell.fetch());
		}

		Iterator<O> it = objects.iterator();
		while (it.hasNext()) {
			O object = copy(it.next());
			if(Numbers.isEven(rand.nextInt())) {
				cell.remove(object);
				it.remove();
				assertEquals(objects, cell.fetch());
			}
		}
		assertEquals(objects, cell.fetch());

		// get objects at specified timestamp
		cell = newInstance();
		scale = randomScaleFrequency();
		objects = Lists.newArrayListWithCapacity(scale);
		for (int i = 0; i < scale; i++) {
			O object = null;
			while (object == null || cell.exists(object)) {
				object = forStorageObject();
			}
			cell.add(object);
			objects.add(object);
			if(rand.nextInt() % 3 == 0) {
				cell.remove(copy(object));
				cell.add(copy(object));
			}
		}

		long at = Time.now();
		sleep();

		it = objects.iterator();
		while (it.hasNext()) {
			O object = copy(it.next());
			if(Numbers.isOdd(rand.nextInt())) {
				cell.remove(object);
			}
		}
		int scale2 = randomScaleFrequency();
		for (int i = 0; i < scale2; i++) {
			O object = null;
			while (object == null || cell.exists(object)) {
				object = forStorageObject();
			}
			cell.add(object);
		}
		assertEquals(objects, cell.fetchAt(at));
	}

	@Test
	public void testIsEmpty() {
		Cell<I, O> cell = newInstance();
		int scale = randomScaleFrequency();
		assertTrue(cell.isEmpty());
		List<O> objects = Lists.newArrayList();
		for (int i = 0; i < scale; i++) {
			O object = null;
			while (object == null || cell.exists(object)) {
				object = forStorageObject();
			}
			cell.add(object);
			objects.add(i, object);
			assertFalse(cell.isEmpty());
		}

		for (O object : objects) {
			assertFalse(cell.isEmpty());
			cell.remove(copy(object));
		}

		assertTrue(cell.isEmpty());
	}

	@Test
	public void testRemove() {
		Cell<I, O> cell = newInstance();
		O object = forStorageObject();
		assertFalse(cell.exists(object));
		cell.add(object);
		assertTrue(cell.exists(object));
		cell.remove(copy(object));
		assertFalse(cell.exists(object));

		// cannot remove object that does not exist
		try {
			cell.remove(object);
			fail("Was expecting an IllegalStateException");
		}
		catch (IllegalStateException e) {}

	}

	/**
	 * Return a forStorage object.
	 * 
	 * @return the object
	 */
	protected abstract O forStorageObject();

	/**
	 * Return an {@code id} instance.
	 * 
	 * @return the id
	 */
	protected abstract I id();

	/**
	 * Return a new instance.
	 * 
	 * @return the cell
	 */
	protected abstract Cell<I, O> newInstance();

	/**
	 * Return a new instance with the specified {@code id}.
	 * 
	 * @param id
	 * @return the cell
	 */
	protected abstract Cell<I, O> newInstance(I id);

	/**
	 * Return a notForStorage object.
	 * 
	 * @return the object
	 */
	protected abstract O notForStorageObject();

	/**
	 * Return an populated instance.
	 * 
	 * @return the cell
	 */
	protected abstract Cell<I, O> populatedInstance();

	/**
	 * Return a populated instance from the sequence of {@code bytes}.
	 * 
	 * @param bytes
	 * @return the cell
	 */
	protected abstract Cell<I, O> populatedInstanceFromBytes(ByteBuffer bytes);

	/**
	 * Return a copy of {@code object}.
	 * 
	 * @param object
	 * @return the copy
	 */
	protected abstract O copy(O object);

}
