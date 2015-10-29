<?php
/*
* Copyright 2015 Cinchapi Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#########################################################################
# This file contains functions that should be in the PHP core, but are  #                                    # not. These functions are not namespaced.
#########################################################################

/**
 * @ignore
 * Return the value associate with the $key in the $array or the
 * specified $default value if the $key doesn't exist.
 *
 * @param array $array The array to search
 * @param mixed $key The lookup key
 * @param mixed $default The default value to return if the key doesn't exist
 * @return mixed The associated value or $default
 */
function array_fetch($array, $key, $default){
    $value = $array[$key];
    return $value === null ? $default: $value;
}

/**
 * @ignore
 * Remove and return the value associated with the $key in the $array if it
 * exists.
 *
 * @param array $array The array to search
 * @param mixed $key The lookup key
 * @return mixed The associated value or NULL
 */
function array_fetch_unset(&$array, $key){
    $value = $array[$key];
    unset($array[$key]);
    return $value;
}

/**
 * @ignore
 *  A wrapper function to time the execution runtime for a <em>callable</em>.If the callable returns a value, it will be assigned to the <em>retval</em> parameter.
 *
 * @param  callable $callable the function to benchmark
 * @param  mixed $retval a variable to store the return value of the <em>callable</em>
 * @return integer the runtime of the callable in <strong>microseconds</strong>
 */
function benchmark($callable, &$retval = null){
    if(is_callable($callable)){
        $start = current_time_micros();
        $retvalue = $callable();
        $end = current_time_micros();
        $elapsed = $end - $start;
        return $elapsed;
    }
    else{
        throw new \InvalidArgumentException("The first argument to the ".get_caller()." method must be callable");
    }
}

/**
 * Perform an optimized count of the number of keys in both $array1 and
 * $array2.
 *
 * @param array $array1 the first array
 * @param array $array2 the second array
 * @param integer $count1 (optional) the size of $array1
 * @param integer $count2 (optional) the size of $array2
 * @return integer the number of keys in the intersection
 */
function count_array_keys_intersect($array1, $array2, $count1 = -1, $count2 = -2) {
    $count1 = $count1 == -1 ? count($array1) : $count1;
    $count2 = $count2 == -1 ? count($array2) : $count2;
    if($count1 < $count2) {
        $smaller = $array1;
        $larger = $array2;
    }
    else {
        $smaller = $array2;
        $larger = $array1;
    }
    $count = 0;
    foreach($smaller as $key => $value) {
        if(isset($larger[$key])){
            $count += 1;
        }
        else if(array_key_exists($key, $larger)){
            // isset returns false if the key exists, but maps to an explicit
            // NULL value so we need to perform this additional check.
            $count += 1;
        }
    }
    return $count;
}

/**
 * @ignore
 * Returns the current time in microseconds subject to the granularity of the
 * underlying operating system.
 *
 * @return integer
 */
function current_time_micros(){
    return (integer) current_time_millis() * 1000;
}

/**
 * @ignore
 * Returns the current time in milliseconds subject to the granularity of the
 * underlying operating system.
 *
 * @return integer
 */
function current_time_millis(){
    return (integer) round(microtime(true) * 1000);
}

/**
 * @ignore
 * Expand any tilde's and ".." components of the path.
 *
 * @param string $path
 * @return string the real path
 */
function expand_path($path){
    if (function_exists('posix_getuid') && strpos($path, '~') !== false) {
        $info = posix_getpwuid(posix_getuid());
        $path = str_replace('~', $info['dir'], $path);
    }
    $newpath = realpath($path);
    return $newpath ?: $path;
}

/**
 * @ignore
 * Tests if an input is valid PHP serialized string.
 *
 * Checks if a string is serialized using quick string manipulation
 * to throw out obviously incorrect strings. Unserialize is then run
 * on the string to perform the final verification.
 *
 * Valid serialized forms are the following:
 * <ul>
 * <li>boolean: <code>b:1;</code></li>
 * <li>integer: <code>i:1;</code></li>
 * <li>double: <code>d:0.2;</code></li>
 * <li>string: <code>s:4:"test";</code></li>
 * <li>array: <code>a:3:{i:0;i:1;i:1;i:2;i:2;i:3;}</code></li>
 * <li>object: <code>O:8:"stdClass":0:{}</code></li>
 * <li>null: <code>N;</code></li>
 * </ul>
 *
 * @author		Chris Smith <code+php@chris.cs278.org>
 * @copyright	Copyright (c) 2009 Chris Smith (http://www.cs278.org/)
 * @license		http://sam.zoy.org/wtfpl/ WTFPL
 * @param		string	$value	Value to test for serialized form
 * @param		mixed	$result	Result of unserialize() of the $value
 * @return		boolean			True if $value is serialized data, otherwise false
 */
function try_unserialize($value, &$result = null) {
	// Bit of a give away this one
	if (!is_string($value)) {
		return false;
	}
	// Serialized false, return true. unserialize() returns false on an
	// invalid string or it could return false if the string is serialized
	// false, eliminate that possibility.
	if ($value === 'b:0;') {
		$result = false;
		return true;
	}
	$length	= strlen($value);
	$end = '';
	switch ($value[0]) {
		case 's':
			if ($value[$length - 2] !== '"') {
				return false;
			}
		case 'b':
		case 'i':
		case 'd':
			// This looks odd but it is quicker than isset()ing
			$end .= ';';
		case 'a':
		case 'O':
			$end .= '}';
			if ($value[1] !== ':') {
				return false;
			}
			switch ($value[2]) {
				case 0:
				case 1:
				case 2:
				case 3:
				case 4:
				case 5:
				case 6:
				case 7:
				case 8:
				case 9:
				break;
				default:
					return false;
			}
		case 'N':
			$end .= ';';
			if ($value[$length - 1] !== $end[0]) {
				return false;
			}
		break;
		default:
			return false;
	}
	if (($result = @unserialize($value)) === false) {
		$result = null;
		return false;
	}
	return true;
}

/**
 * @ignore
 * Return the name of the method from which this function is called. This is
 * normally used when dynamically displaying the method name in an error message
 * or something.
 *
 * @return string the name of the caller
 */
function get_caller(){
    return debug_backtrace()[1]['function']."()";
}

/**
 * @ignore
 * Recursively implode an array and all of its sub arrays.
 *
 * @param array $array
 * @param string $glue (optional, default is ', ')
 * @return string
 */
function implode_all($array, $glue=", "){
    $ret = '';
    foreach ($array as $item) {
        if (is_assoc_array($item)){
            $ret.= "[".implode_all_assoc($item, $glue)."]". $glue;
        }
        else if (is_array($item)) {
            $ret .= "[".implode_all($item, $glue)."]". $glue;
        }
        else if(is_object($item)){
            $ref = new ReflectionClass(get_class($item));
            if(!$ref->hasMethod("__toString")){
                $ret .= serialize($item) . $glue;
            }
            else{
                $ret .= $item . $glue;
            }
        }
        else {
            $ret .= $item . $glue;
        }
    }
    $ret = substr($ret, 0, 0-strlen($glue));
    return $ret;
}

/**
 * @ignore
 * Recursively implode an associative array and all of its sub arrays.
 *
 * @param array $array
 * @param string $glue (optional, default is ', ')
 * @return string
 */
function implode_all_assoc($array, $glue=", "){
    if(is_assoc_array($array)) {
        $ret = '';
        foreach($array as $key => $value){
            if (is_array($value)) {
                $ret .= $key . " => [". implode_all_assoc($value, $glue)."]" . $glue;
            }
            else if(is_object($value)){
                $ref = new ReflectionClass(get_class($item));
                if(!$ref->hasMethod("__toString")){
                    $ret .= $key ." => ". serialize($value) . $glue;
                }
                else{
                    $ret .= $key . $value . $glue;
                }

            }
            else {
                $ret .= $key ." => ". $value . $glue;
            }
        }
        $ret = substr($ret, 0, 0-strlen($glue));
        return $ret;
    }
    else if(is_array($array)) {
        return implode_all($array, $glue);
    }
}

/**
 * @ignore
 * Prints a message and then terminates the line.
 *
 * @param mixed $message the primitive, array or object to print
 */
function println($message){
    if(is_assoc_array($message)) {
        $output = "[".implode_all_assoc($message)."]";
    }
    else if(is_array($message)) {
        $output = "[".implode_all($message)."]";
    }
    else {
        $output = $message;
    }
    print_r($output);
    if(!empty($_SERVER['HTTP_USER_AGENT'])){
        // Assume this is being displayed on a web page
        print_r("<br />");
    }
    else {
        print_r(PHP_EOL);
    }
}

/**
* @ignore
* Return {@code true} if {@code $var} is an assoc array. An array is considered
* to be associative if it has at least one string key or nonsequential integer
* keys.
* @param mixed $var
* @return boolean
*/
function is_assoc_array($var){
    if(is_array($var)) {
        $index = 0;
        foreach($var as $key => $value){
            if(is_string($key)){
                return true;
            }
            else if($key != $index){
                // This enforces the constraint that an array with numerical
                // indexes should be considered assoc if the indexes aren't 0
                // based and aren't monotonically increasing.
                return true;
            }
            ++$index;
        }
    }
    return false;
}

/**
 * @ignore
 * Return TRUE if the $haystack contains the $needle.
 * @param string $haystack
 * @param string $needle
 * @return boolean
 */
function str_contains($haystack, $needle){
    return strpos($haystack, $needle) !== false;
}

/**
 * @ignore
 * Return {@code true} if the {@code $haystack} begins with the {@code $needle}.
 * @param string $haystack
 * @param string $needle
 * @return boolean
 */
function str_starts_with($haystack, $needle){
    return $needle === "" || strrpos($haystack, $needle, -strlen($haystack)) !== false;
}

/**
 * @ignore
 * A container that maps arbitrary keys to arbitrary values. Unlike builtin
 * arrays and the SplObjectStorage class, a Dictionary does not place limits
 * on the valid types that can be assumed by keys.
 *
 * @author Jeff Nelson
 */
class Dictionary implements ArrayAccess {

    /**
     * The backing store
     * @var array
     */
    private $data;

    /**
     * Construct a new instance.
     */
    public function __construct(){
        $this->data = array();
    }

    /**
     * @Override
     */
    public function offsetExists($offset){
        return isset($this->data[$offset]);
    }

    /**
     * @Override
     */
    public function offsetUnset($offset){
        unset($this->data[$offset]);
    }

    /**
     * @Override
     */
    public function offsetSet($offset, $value){
        if($offset === null){
            $this->data[] = $value;
        }
        else{
            $this->data[$offset] = $value;
        }
    }

    /**
     * @Override
     */
    public function offsetGet($offset) {
        return $this->data[$offset];
    }

}
