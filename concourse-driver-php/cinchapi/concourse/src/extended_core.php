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
 * Returns the current time in milliseconds subject to the granularity of the
 * underlying operating system.
 *
 * @return integer
 */
function current_time_millis(){
    return (integer) round(microtime(true) * 1000);
}

/**
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
 * Recursively implode an array and all of its sub arrays.
 *
 * @param array $array
 * @param string $glue (optional, default is ', ')
 * @return string
 */
function implode_all($array, $glue=", "){
    $ret = '';
    foreach ($array as $item) {
        if (is_array($item)) {
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
        foreach($var as $key => $value){
            if(is_string($key)){
                return true;
            }
        }
    }
    return false;
}

/**
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
        if(is_null($offset)){
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
