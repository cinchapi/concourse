package com.cinchapi.concourse.annotations;

/**
 * An immutable object
 * <ul>
 * 	<li>forces callers to construct an object in a single step, instead of using a no-argument constructor combined
 * 	with subsequent calls to <code>setXXX</code> methods (that is, avoids the Java Beans convention),</li>
 * 	<li>has member variables that are private and final,</li>
 * 	<li>does not provide any methods which can change the state of the object in any way, and</li>
 * 	<li>is a final or uses static factories with private constructors, where possible.</li>
 * </ul>
 * @author jnelson
 * @see <a href="http://www.javapractices.com/topic/TopicAction.do?Id=29">http://www.javapractices.com/topic/TopicAction.do?Id=29</a>
 *
 */
public @interface Immutable {

}
