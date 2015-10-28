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
namespace concourse;

require_once dirname(__FILE__). "/base.php"; # For access to $kwarg_aliases
require_once dirname(__FILE__) . "/autoload.php";

use Concourse\Thrift\ConcourseServiceClient;
use Concourse\Thrift\Shared\Operator;

/**
 * The Dispatcher is responsible for taking method invocations and dynamically
 * determining the correct Thrift method to call.
 *
 * We simulate polymorphic method overloading in languages that don't natively
 * support it by taking advantage of the fact that an arbitrary mix of arguments
 * can be passed to methods at runtime. So, for each method invocation, if we
 * inspect the arguments that are passed, we can determine the appropriate
 * Thrift method to use for dispatch.
 *
 * As you can imagine, implementing this logic is tedious and requires a lot of
 * ugly if/else-if/else blocks. Futhermore, this manual process doesn't scale
 * well as the Thrift API evolves and we need more client drivers. So, the
 * Dispatcher is here to save the day!
 *
 * The Dispatcher takes advantage of the fact that methods in the Thrift API
 * generally advertise their signature in the method name. This allows this
 * class to implement a lot of logic to support dynamic dispatch at runtime.
 *
 * The client driver should use the Dispatcher when there are multiple possible
 * parameter configurations for a method. Obviously all of this runtime
 * inspection comes with some overhead, so we should only use this feature
 * when it will greatly reduce development complexity.
 *
 * @author Jeff Nelson
 * @ignore
 */
class Dispatcher {

    /**
     * @lazyload
     * A collection that contains all of the callable thrift methods. This is
     * inspected in order to calculate polymorphic signatures for dynamic
     * dispatch.
     */
    private static $THRIFT_METHODS = null;

    /**
     * @lazyload
     * A collection mapping a thrift method to its suite of required argument
     * types.
     */
    private static $SIGNATURES = array();

    /**
     * A mapping from php methods to the general order of kwargs that is desired
     * in order to correctly compute the dynamic call signature.
     */
    private static $SORT_SPEC = array(
        'get' => array('keys', 'criteria', 'records', 'timestamp'),
        'select' => array('keys', 'criteria', 'records', 'timestamp')
    );

    /**
     * A mapping from alias to the canonical kwarg. This method is initialized
     * in the #staticInitAliases method.
     */
    private static $ALIASES = array();

    /**
     * For the specified PHP $method that was passed the following $args and
     * $kwargs, lookup the appropriate thrift method for dispatch. This method
     * is public, but it should not be called lightly.
     *
     * @param string $method - the name of the PHP method
     * @param array $args - the positional arguments that were passed with the
     * invocation of $method
     * @param array $kwargs - the keyword (associative array) arguments that
     * were passed with the invocation of $method
     * @return An array where the first element is the name of the thrift method
     * and the second element is an array containing the arguments to pass to
     * the method in order
     */
    public static function send($method, $args=array(), $kwargs=array()){
        static::enableDynamicDispatch($method);
        $okwargs = $kwargs;
        $kwargs = static::resolveKwargAliases($kwargs);
        $kwargs = static::sortKwargs($method, $kwargs);
        $tocall = array();
        foreach(static::$SIGNATURES[$method] as $tmethod => $signature) {
            if(count($tocall) > 1){
                break; //break out early since we know there will be ambiguity
            }
            if(count(array_intersect(array_keys($kwargs), array_keys($signature))) == count($kwargs)) {
                // If the intersection of the $kwargs and $signature is the same
                // size as the number of $kwargs, then that means the signature
                // is a potential match since it has all the $kwargs
                $comboargs = array();
                $largs = $args; //local copy of $args
                if(empty($signature) && (!empty($args) || !empty($kwargs))){
                    // Signature does not expect any parameters, so if there are
                    // kw/args, move on.
                    continue;
                }
                foreach($signature as $kwarg => $type) {
                    if((count($kwargs) + count($args)) == count($signature)) {
                        if(array_key_exists($kwarg, $kwargs)){
                            $arg = $kwargs[$kwarg];
                        }
                        else {
                            $arg = array_fetch_unset($largs, 0);
                            $largs = array_values($largs);
                        }
                        // Perform reasonable conversion on the $arg to account
                        // for things that the caller may have forgotten to do
                        if($type == "Concourse\Thrift\Data\TObject" && !is_a($arg, $type)){
                            $arg = Convert::phpToThrift($arg);
                        }
                        else if($type == "json"){
                            if((is_array($arg) || is_object($arg))){
                                $arg = json_encode($arg);
                            }
                            $type = "string";
                        }
                        else if($type == "Concourse\Thrift\Shared\Operator" && array_key_exists($arg, Operator::$__names)){
                            $type = "integer";
                        }
                        else if($type == "array" && $kwarg == "Values"){
                            if(!is_array($arg)){
                                $arg = array($arg);
                            }
                            foreach($arg as $argk => $argv){
                                if(!is_a($argv, "Concourse\Thrift\Data\TObject")){
                                    $arg[$argk] = Convert::phpToThrift($argv);
                                }
                            }
                        }
                        // Finally, given the type, decide if this is valid for
                        // the signature we are looking at
                        if($type == "object" || (is_object($arg) && is_a($arg, $type)) || gettype($arg) == $type){
                            $comboargs[] = $arg;
                        }
                        else {
                            continue 2; //signature does not match
                        }
                    }
                    else {
                        continue 2; //signature can't possibly match
                    }
                }
                $tocall[$tmethod] = $comboargs;
            }
        }
        $found = count($tocall);
        if($found < 1) {
            throw new \RuntimeException("No signature of method '$method' is applicable for positional arguments [".implode_all($args, ", ")."] and keyword arguments [".implode_all_assoc(($okwargs), ", ")."].");
        }
        else if($found > 1) {
            throw new \RuntimeException("Cannot deterministically dispatch because there are multiple signatures for method '$method' that can handle positional arguments [".implode_all($args, ", ")."] and keyword arguments [".implode_all_assoc($okwargs, ", ")."]. The possible solutions are: [".implode(array_keys($tocall), ", ")."]. Please use more keyword arguments to clarify your intent.");
        }
        else {
            return $tocall;
        }
    }

    /**
     * Go through the $kwarg_aliases that are defined in base.php and transform
     * the map to one that maps every possible alias to the canonical kwarg.
     *
     * @param array $kwarg_aliases - This value must be passed from base.php
     */
     static function staticInitAliases($kwarg_aliases){
        foreach($kwarg_aliases as $kwarg => $aliases){
            foreach($aliases as $alias){
                if($alias != "value"){
                    static::$ALIASES[$alias] = $kwarg;
                }
            }
        }
    }

    /**
     * Setup the Dispatcher to handle requests to dynamically dispatch calls to
     * the specified PHP $method based on the arguments that are passed.
     *
     * @param string $method - The PHP method for which an attempt is made to
     * enable dynamic dispatch
     */
    private static function enableDynamicDispatch($method){
        if(!array_key_exists($method, static::$SIGNATURES)){
            static::$SIGNATURES[$method] = array();
            if(is_null(static::$THRIFT_METHODS)){
                static::$THRIFT_METHODS = array_values(array_filter(get_class_methods("Concourse\Thrift\ConcourseServiceClient"), function($element) {
                    return !str_starts_with($element, "send_") && !str_starts_with($element, "recv_") && !str_contains($element, "Criteria");
                }));
            }
            $methods = array();
            foreach(static::$THRIFT_METHODS as $tmethod){
                if(str_starts_with($tmethod, $method) && $tmethod != "getServerVersion" && $tmethod != "getServerEnvironment"){
                    $args = array();
                    preg_match_all('/((?:^|[A-Z])[a-z]+)/',$tmethod,$args);
                    $args = array_shift($args);
                    array_shift($args);
                    $signature = array();
                    foreach($args as $arg){
                        $type = static::getArgType($arg);
                        $signature[$arg] = $type;
                    }
                    static::$SIGNATURES[$method][$tmethod] = $signature;
                }
            }
        }
    }

    /**
     * Return the type that Thrift expects for the parameter.
     *
     * @param string $arg - the name of the method parameter
     * @return The arg type for the parameter
     */
    private static function getArgType($arg){
        if(str_ends_with($arg, "str") || in_array($arg, array('Key', 'Ccl', 'Phrase'))){
            return "string";
        }
        else if($arg == "Value"){
            return "Concourse\Thrift\Data\TObject";
        }
        else if($arg == "Operator"){
            return "Concourse\Thrift\Shared\Operator";
        }
        else if(in_array($arg, array('Record', 'Time', 'Start', 'End'))){
            return "integer";
        }
        else if(str_ends_with($arg, "s")){
            return "array";
        }
        else{
            return strtolower($arg);
        }
    }

    /**
     * Given a collection of $kwargs, change any aliases to the canonical
     * keywords that are expected by thrift.
     *
     * @param array $kwargs - The $kwargs that need to be resolved
     * @return An array of resolved kwargs
     */
    private static function resolveKwargAliases($kwargs){
        $nkwargs = array();
        foreach($kwargs as $key => $value){
            $k = strtolower(array_fetch(static::$ALIASES, $key, $key));
            if(!is_array($value) && str_ends_with($k, "s")){
                // Account for cases when the plural kwarg is provided, but the
                // actual value is a single item.
                $k = rtrim($k, "s");
            }
            else if(is_array($value) && !str_ends_with($k, "s") && !in_array($k, array('json'))){
                //Account for cases when the singular kwarg is provided, but the
                // actual value is an array
                $k .= "s";
            }
            else if(is_string($value) && in_array($k, array('time', 'start', 'end', 'operator'))){
                $k .= "str";
            }
            $nkwargs[ucfirst($k)] = $value;
        }
        return $nkwargs;
    }

    /**
     * Sort the $kwargs that were passed to the PHP $method, according to the
     * appropriate sort spec.
     *
     * @param string $method - The name of the PHP method
     * @param array $kwargs - The kwargs that were passed to the method
     * @return The sorted $kwargs
     */
    private static function sortKwargs($method, $kwargs){
        $spec = static::$SORT_SPEC[$method];
        if(!empty($spec)){
            // Go through the spec and pull out elements in order from $kwargs
            // and then just merge any remaining kwargs
            $nkwargs = array();
            foreach($spec as $key){
                $value = array_fetch_unset($kwargs, $key);
                if(!is_null($value)){
                    $nkwargs[$key] = $value;
                }
            }
            // Do the merge with any remaining original $kwargs
            foreach($kwargs as $key => $value){
                $nkwargs[$key] = $value;
            }
            $kwargs = $nkwargs;
        }
        return $kwargs;
    }
}

// This is a hack for Java style static initialization.
Dispatcher::staticInitAliases($kwarg_aliases);
