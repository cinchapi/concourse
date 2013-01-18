package com.cinchapi.concourse.server;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.cinchapi.concourse.services.ConcourseService;
import com.cinchapi.concourse.store.ConcourseStore;
import com.cinchapi.concourse.store.ModificationLog;

/**
 * Provides a {@link ConcourseService}.
 * 
 * @author jnelson
 */
public class Node {

	private final ConcourseService service;
	private int hashCode;

	private EqualsBuilder equals = new EqualsBuilder();

	/**
	 * Create a new {@link Node} that provides a {@link ConcourseService} that
	 * interacts with the <code>log</code> and <code>store</code>.
	 * 
	 * @param log
	 * @param store
	 */
	public Node(ModificationLog log, ConcourseStore store) {
		this.service = new ConcourseService(log,store);
	}

	/**
	 * Returns the {@link ConcourseService} associated with this
	 * <code>node</code>.
	 * 
	 * @return the <code>node</code>.
	 */
	public ConcourseService getService(){
		return service;
	}

	@Override
	public int hashCode(){
		if(hashCode == 0){
			hashCode = new HashCodeBuilder().append(service).toHashCode();
		}
		return hashCode;
	}

	@Override
	public boolean equals(Object obj){
		if(obj == null){
			return false;
		}
		else if(obj == this){
			return true;
		}
		else if(obj.getClass() != this.getClass()){
			return false;
		}
		else{
			Node other = (Node) obj;
			return equals.append(service,other.service).isEquals();
		}
	}

}
