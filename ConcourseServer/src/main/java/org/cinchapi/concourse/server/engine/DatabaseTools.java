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
package org.cinchapi.concourse.server.engine;

import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.annotate.UtilityClass;
import org.cinchapi.common.io.Byteable;

/**
 * 
 * 
 * @author jnelson
 */
@PackagePrivate
@UtilityClass
final class DatabaseTools {

	/**
	 * Return a {@link Runnable} that will execute the appropriate write
	 * function in {@code record} to store {@code write} in the {@link Database}.
	 * 
	 * @param record
	 * @param write
	 * @return the appropriate Runnable
	 */
	public static <L extends Byteable, K extends Byteable, V extends Storable> Runnable invokeWriteRunnable(
			final Record<L, K, V> record, final Write write) {
		return new Runnable() {

			@Override
			public void run() {
				if(record instanceof PrimaryRecord) {
					if(write.getType() == WriteType.ADD) {
						((PrimaryRecord) record).add(write.getKey(),
								write.getValue());
					}
					else if(write.getType() == WriteType.REMOVE) {
						((PrimaryRecord) record).remove(write.getKey(),
								write.getValue());
					}
					else {
						throw new IllegalArgumentException();
					}
				}
				else if(record instanceof SecondaryIndex) {
					if(write.getType() == WriteType.ADD) {
						((SecondaryIndex) record).add(write.getValue(),
								write.getRecord());
					}
					else if(write.getType() == WriteType.REMOVE) {
						((SecondaryIndex) record).remove(write.getValue(),
								write.getRecord());
					}
					else {
						throw new IllegalArgumentException();
					}
				}
				else if(record instanceof SearchIndex) {
					if(write.getType() == WriteType.ADD) {
						((SearchIndex) record).add(write.getValue(),
								write.getRecord());
					}
					else if(write.getType() == WriteType.REMOVE) {
						((SearchIndex) record).remove(write.getValue(),
								write.getRecord());
					}
					else {
						throw new IllegalArgumentException();
					}
				}
				else {
					throw new IllegalArgumentException();
				}
				record.fsync();
			}

		};
	}

}
