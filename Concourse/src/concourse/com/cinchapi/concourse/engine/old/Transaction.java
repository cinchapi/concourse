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
package com.cinchapi.concourse.engine.old;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.util.Strings;
import com.cinchapi.concourse.db.Key;
import com.cinchapi.concourse.db.old.Value.Values;
import com.cinchapi.concourse.exception.ConcourseRuntimeException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * A {@link Transaction} is initiated from a {@link TransactionService} for the
 * purpose of conducting an set of ACID writes. This object provides a
 * similar action interface as the the parent.
 * 
 * @author jnelson
 */
public final class Transaction extends BufferedWriteService {
	// NOTE: This class does not define hashCode() or equals() because the
	// defaults are the desired behaviour.

	/**
	 * Start a {@code transaction} for the {@code parent} service.
	 * 
	 * @param parent
	 * @return the transaction
	 */
	protected static Transaction initFrom(TransactionService parent) {
		return new Transaction(parent);
	}

	/**
	 * Return a closed {@code transaction} that has been deserialized and
	 * recovered from the {@code file}.
	 * 
	 * @param file
	 * @param parent
	 * @return the recovered Transaction
	 */
	protected static Transaction recoverFrom(String file,
			TransactionService parent) {
		String content = "";
		FileInputStream stream;
		try {
			stream = new FileInputStream(new File(file));
			FileChannel channel = stream.getChannel();
			MappedByteBuffer buffer = channel.map(
					FileChannel.MapMode.READ_ONLY, 0, channel.size());
			content = ByteBuffers.charset().decode(buffer).toString();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		Gson gson = new LocalGsonBuilder(parent).build();
		return gson.fromJson(content, Transaction.class);
	}

	/**
	 * Replay the {@code transaction} against the most current view of the
	 * parent service and see if the commits succeed.
	 * 
	 * @param transaction
	 * @return a new {@code transaction} if the replay succeeds, and
	 *         {@code null} if the replay fails
	 */
	@Nullable
	private static Transaction replay(Transaction transaction) {
		Transaction t = Transaction
				.initFrom((TransactionService) transaction.primary);
		Preconditions.checkState(transaction.isClosed(),
				"Cannot replay a transaction that is not closed");
		Preconditions.checkState(transaction.primary.equals(t.primary),
				"Cannot replay a transaction with a different parent service");
		synchronized (transaction.primary) {
			Iterator<Operation> operations = transaction.flusher();
			while (operations.hasNext()) {
				Operation operation = operations.next();
				if(Utilities.flush(operation, t)) {
					continue;
				}
				else { // detected a merge conflict that would cause the
						// transaction to fail
					t.closed = true;
					return null;
				}
			}
		}
		t.closed = true;
		transaction.shutdown();
		return t;
	}

	/**
	 * Return a {@link Transaction} seeded with the {@code operations}.
	 * 
	 * @param operations
	 * @param parent
	 * @return the Transaction
	 */
	private static Transaction withOperations(List<Operation> operations,
			TransactionService parent) {
		Transaction transaction = new Transaction(parent);
		transaction.closed = true;
		transaction.operations = operations; // authorized
		return transaction;
	}

	private static final Logger log = LoggerFactory
			.getLogger(Transaction.class);
	private static final int INITIAL_CAPACITY = 10;

	private List<Operation> operations = Lists
			.newArrayListWithExpectedSize(INITIAL_CAPACITY); // treat as if
																// final
	private boolean closed = false;

	/**
	 * Construct a new instance for the purpose of replaying.
	 * 
	 * @param transaction
	 */
	private Transaction(Transaction transaction) {
		super(
				VolatileStorage
						.newInstancewithExpectedCapacity(transaction.operations
								.size()), transaction.primary);
	}

	/**
	 * Construct a new instance.
	 * 
	 * @param parent
	 *            - must be an instance of ConcourseService
	 */
	private Transaction(TransactionService parent) {
		super(
				VolatileStorage
						.newInstancewithExpectedCapacity(INITIAL_CAPACITY),
				(ConcourseService) parent);
	}

	/**
	 * Commit this transaction to the parent service.
	 * 
	 * @return {@code true} if the transaction is successfully committed.
	 */
	public synchronized boolean commit() {
		closed = true;
		Transaction current = Transaction.replay(this);
		if(current != null) {
			current.doCommit();
			return true;
		}
		else {
			log.info("Failed to commit transaction {}", this);
			return false;
		}

	}

	/**
	 * Return {@code true} if the transaction is closed.
	 * 
	 * @return {@code true} if the transaction is closed.
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
	 * Rollback and cancel all the changes in the transaction.
	 */
	public void rollback() {
		closed = true;
	}

	@Override
	public synchronized void shutdown() {
		buffer.shutdown(); // shutdown the volatile db
		super.shutdown();
	}

	@Override
	public String toString() {
		return Strings.toString(operations);
	}

	@Override
	protected boolean addSpi(String column, Object value, long row) {
		if(super.addSpi(column, value, row) && !isClosed()) {
			return storeOperation(WriteType.ADD, row, column, value);
		}
		return false;
	}

	/**
	 * Flush the transaction to its parent service. This method does not perform
	 * ANY checks, so use with caution.
	 */
	protected synchronized void doCommit() {
		Preconditions.checkState(closed,
				"The transaction must be closed in order to commit");
		final String transactionFile = ((TransactionService) primary)
				.getTransactionFileName();
		final File file = new File(transactionFile);
		Runnable cleanup = new Runnable() { // Delete files, release locks, etc
			@Override
			public void run() {
				file.delete();
				primary.notifyAll();
				shutdown();
			}
		};
		synchronized (primary) {
			while (file.exists()) {
				try {
					primary.wait();
				}
				catch (InterruptedException e) {
					throw new ConcourseRuntimeException(e);
				}
			}
			try {
				serialize(transactionFile);
			}
			catch (IOException e) {
				cleanup.run();
				log.error(
						"An error occured while trying to serialize transaction {}: {}",
						this, e);
				throw new ConcourseRuntimeException(e);
			}
			Iterator<Operation> operations = flusher();
			while (operations.hasNext()) {
				Operation operation = operations.next();
				if(Utilities.flush(operation, primary)) {
					log.debug("Successfully saved transaction operation {}:",
							operation);
				}
				else {
					log.warn(
							"Failed attempt to save transaction operation "
									+ "{} because it appears that the primary service "
									+ "was previously shutdown while committing this "
									+ "transaction, yet managed to successfully commit "
									+ "this particular operation.", operation);
				}
			}
			cleanup.run();
		}
	}

	@Override
	protected boolean removeSpi(String column, Object value, long row) {
		if(super.removeSpi(column, value, row) && !isClosed()) {
			return storeOperation(WriteType.REMOVE, row, column, value);
		}
		return false;
	}

	/**
	 * Assert that the number of operations is equal to the number of commits.
	 */
	private void assertSize() {
		assert operations.size() == ((VolatileStorage) buffer).ordered.size() : "There is a discrepency between the number of operations and the number of commits";
	}

	/**
	 * Return an iterator of mappings from {@link Write} to
	 * {@link OperationType} for the purpose of flushing this transaction to
	 * its parent service.
	 * 
	 * @return the flusher
	 */
	private Iterator<Operation> flusher() {
		Preconditions.checkState(isClosed(),
				"Cannot flush a transaction that is not closed"); // Ensuring
																	// that the
																	// transaction
																	// is closed
																	// protects
																	// against
																	// concurrent
																	// modification
		return operations.iterator();
	}

	/**
	 * Serialize the transaction and store it to the {@code file}.
	 * 
	 * @param file
	 * @throws IOException
	 */
	private void serialize(String file) throws IOException {
		Gson gson = new LocalGsonBuilder((TransactionService) primary).build();
		String json = gson.toJson(this);
		BufferedWriter out;
		out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
				file), ByteBuffers.charset().displayName()));
		out.write(json);
		out.flush();
		out.close();
	}

	/**
	 * Store an operation in the transaction. This method should ONLY be called
	 * from the {@link #addSpi(String, Object, long)} or
	 * {@link #removeSpi(String, Object, long)} method.
	 * 
	 * @param type
	 * @param row
	 * @param column
	 * @param value
	 * @return
	 */
	private boolean storeOperation(WriteType type, long row, String column,
			Object value) {
		operations.add(new Operation(type, row, column, value));
		assertSize();
		return true;
	}

	/**
	 * Encapsulates serializable information about the type, row, column and
	 * value (raw) that define an operation. Timestamp information is lost, but
	 * that is okay because each operation will receive its true timestamp at
	 * the time of commit.
	 * 
	 * @author jnelson
	 */
	@Immutable
	public static class Operation {
		// NOTE: This class does not define hashCode() or equals() because the
		// defaults are the desired behaviour.

		private final WriteType type;
		private final long row;
		private final String column;
		private final Object value;

		/**
		 * Construct a new instance.
		 * 
		 * @param type
		 * @param row
		 * @param column
		 * @param value
		 */
		public Operation(WriteType type, long row, String column, Object value) {
			this.type = type;
			this.row = row;
			this.column = column;
			this.value = value;
		}

		/**
		 * Return the {@code column}.
		 * 
		 * @return the column
		 */
		public String getColumn() {
			return column;
		}

		/**
		 * Return the {@code row}.
		 * 
		 * @return the row
		 */
		public long getRow() {
			return row;
		}

		/**
		 * Return the {@code type}.
		 * 
		 * @return the type
		 */
		public WriteType getType() {
			return type;
		}

		/**
		 * Return the {@code value}.
		 * 
		 * @return the value
		 */
		public Object getValue() {
			return value;
		}

		@Override
		public String toString() {
			return Strings.toString(this);
		}

		/**
		 * GSON type adapter for {@link Operation}.
		 * 
		 * @author jnelson
		 */
		private static class GsonTypeAdapter implements
				JsonSerializer<Operation>,
				JsonDeserializer<Operation> {

			static final String TYPE_MEMBER = "type";
			static final String ROW_MEMBER = "row";
			static final String COLUMN_MEMBER = "column";
			static final String VALUE_MEMBER = "value";
			static final String VALUE_TYPE_MEMBER = "value_type";

			@Override
			public Operation deserialize(JsonElement json,
					java.lang.reflect.Type typeOfT,
					JsonDeserializationContext context)
					throws JsonParseException {
				JsonObject obj = json.getAsJsonObject();

				WriteType type = WriteType.values()[obj.get(TYPE_MEMBER)
						.getAsInt()];
				long row = obj.get(ROW_MEMBER).getAsLong();
				String column = obj.get(COLUMN_MEMBER).getAsString();
				com.cinchapi.concourse.engine.old.Value.Type valueType = com.cinchapi.concourse.engine.old.Value.Type
						.values()[obj.get(VALUE_TYPE_MEMBER).getAsInt()];
				Object value = null;
				if(valueType == com.cinchapi.concourse.engine.old.Value.Type.BOOLEAN) {
					value = obj.get(VALUE_MEMBER).getAsBoolean();
				}
				else if(valueType == com.cinchapi.concourse.engine.old.Value.Type.DOUBLE) {
					value = obj.get(VALUE_MEMBER).getAsDouble();
				}
				else if(valueType == com.cinchapi.concourse.engine.old.Value.Type.FLOAT) {
					value = obj.get(VALUE_MEMBER).getAsFloat();
				}
				else if(valueType == com.cinchapi.concourse.engine.old.Value.Type.INTEGER) {
					value = obj.get(VALUE_MEMBER).getAsInt();
				}
				else if(valueType == com.cinchapi.concourse.engine.old.Value.Type.LONG) {
					value = obj.get(VALUE_MEMBER).getAsLong();
				}
				else if(valueType == com.cinchapi.concourse.engine.old.Value.Type.RELATION) {
					value = Key.notForStorage(obj.get(VALUE_MEMBER).getAsLong());
				}
				else {
					value = obj.get(VALUE_MEMBER).getAsString();
				}
				return new Operation(type, row, column, value);
			}

			@Override
			public JsonElement serialize(Operation src,
					java.lang.reflect.Type typeOfSrc,
					JsonSerializationContext context) {
				JsonObject object = new JsonObject();
				object.addProperty(TYPE_MEMBER, src.getType().ordinal());
				object.addProperty(ROW_MEMBER, src.getRow());
				object.addProperty(COLUMN_MEMBER, src.getColumn());
				object.addProperty(VALUE_MEMBER, src.getValue().toString());
				object.addProperty(VALUE_TYPE_MEMBER,
						ValueSet.getObjectType(src.getValue()).ordinal());
				return object;
			}

		}
	}

	/**
	 * GSON type adapter for {@link Transaction}.
	 * 
	 * @author jnelson
	 */
	private static class GsonTypeAdapter implements
			JsonSerializer<Transaction>,
			JsonDeserializer<Transaction> {

		static final String OPERATIONS_MEMBER = "operations";
		final TransactionService parent;

		public GsonTypeAdapter(TransactionService parent) {
			this.parent = parent;
		}

		@Override
		public Transaction deserialize(JsonElement json,
				java.lang.reflect.Type typeOfT,
				JsonDeserializationContext context) throws JsonParseException {
			Gson gson = new GsonBuilder().registerTypeAdapter(Operation.class,
					new Operation.GsonTypeAdapter()).create();
			JsonObject object = json.getAsJsonObject();
			JsonArray jsonOperations = object.get(OPERATIONS_MEMBER)
					.getAsJsonArray();
			List<Operation> operations = Lists
					.newArrayListWithCapacity(jsonOperations.size());
			Iterator<JsonElement> elements = jsonOperations.iterator();
			while (elements.hasNext()) {
				operations.add(gson.fromJson(elements.next(), Operation.class));
			}
			return Transaction.withOperations(operations, parent);
		}

		@Override
		public JsonElement serialize(Transaction src,
				java.lang.reflect.Type typeOfSrc,
				JsonSerializationContext context) {
			Gson gson = new GsonBuilder().registerTypeAdapter(Operation.class,
					new Operation.GsonTypeAdapter()).create();
			Iterator<Operation> flusher = src.flusher();
			JsonArray operations = new JsonArray();
			while (flusher.hasNext()) {
				operations
						.add(gson.toJsonTree(flusher.next(), Operation.class));
			}
			JsonObject object = new JsonObject();
			object.add(OPERATIONS_MEMBER, operations);
			return object;
		}

	}

	/**
	 * A {@link GsonBuilder} that depends on a specific
	 * {@link TransactionService} for the purpose of initiating a
	 * {@link Transaction.GsonTypeAdapter}.
	 * 
	 * @author jnelson
	 */
	private static class LocalGsonBuilder {

		static final GsonBuilder builder = new GsonBuilder()
				.registerTypeAdapter(Operation.class,
						new Operation.GsonTypeAdapter());
		final TransactionService parent;

		/**
		 * Construct a new instance.
		 * 
		 * @param parent
		 */
		public LocalGsonBuilder(TransactionService parent) {
			this.parent = parent;
		}

		public Gson build() {
			return builder.registerTypeAdapter(Transaction.class,
					new Transaction.GsonTypeAdapter(parent)).create();
		}

	}

	/**
	 * Utility methods for {@link Transaction}.
	 * 
	 * @author jnelson
	 */
	private static class Utilities {

		/**
		 * Flush a transaction operation to another service.
		 * 
		 * @param operation
		 * @param to
		 * @return {@code true} if the operation is successfully flushed.
		 */
		public static boolean flush(Operation operation, ConcourseService to) {
			Preconditions.checkArgument(to instanceof Transaction
					|| to instanceof TransactionService,
					"A transaction operation can only be flushed to a "
							+ "TransactionService or another Transaction");
			if(operation.getType() == WriteType.ADD
					&& to.add(operation.getColumn(), operation.getValue(),
							operation.getRow())) {
				return true;
			}
			else if(operation.getType() == WriteType.REMOVE
					&& to.remove(operation.getColumn(), operation.getValue(),
							operation.getRow())) {
				return true;
			}
			else { // detected a merge conflict that would cause the
					// transaction to fail
				return false;
			}
		}

	}

}
