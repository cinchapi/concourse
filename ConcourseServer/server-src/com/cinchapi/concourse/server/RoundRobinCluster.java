package com.cinchapi.concourse.server;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * A {@link Cluster} that uses a round robin algorithm to provide {@link Node}
 * objects from the collection.
 * 
 * @author jnelson
 */
public class RoundRobinCluster extends AbstractCluster {

	private final List<Node> nodes;
	private int next;
	
	private int hashCode;
	private EqualsBuilder equals;

	/**
	 * Create a new {@link RoundRobinCluster}.
	 */
	public RoundRobinCluster() {
		this.nodes = new ArrayList<Node>();
		this.next = 0;
	}

	/**
	 * Add a {@link Node} to the <code>cluster</code> if it is not present.
	 * 
	 * @param node
	 * @return <code>true</code> if the <code>node</code> is added.
	 */
	public boolean addNode(Node node){
		return !nodes.contains(node) ? nodes.add(node) : false;
	}

	/**
	 * Remove a {@link Node} from the <code>cluster</code> if it is present.
	 * 
	 * @param node
	 * @return <code>true</code> if the <code>node</code> is removed.
	 */
	public boolean removeNode(Node node){
		return nodes.remove(node);
	}

	@Override
	protected Node getNodeSpi(){
		next = next > nodes.size() ? 0 : next;
		Node node = nodes.get(next);
		next++;
		return node;
	}
	
	@Override
	public int hashCode(){
		if(hashCode == 0){
			hashCode = new HashCodeBuilder().append(nodes).toHashCode();
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
			RoundRobinCluster other = (RoundRobinCluster) obj;
			return equals.append(nodes,other.nodes).isEquals();
		}
	}
	

}
