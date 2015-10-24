<?php

/**
 * A Tag is a String data type that does not get full-text indexed.
 * 
 * Each Tag is equivalent to its String counterpart. Tags merely exist for the
 * client to instruct Concourse not to full text index the data. Tags are stored
 * as Strings within Concourse. Any data value that is written as a Tag is always
 * returned as a String when reading from Concourse.
 */
class Tag {
    
    private $value;
    
    public static function create($value){
        return new Tag($value);
    }
    
    private function __construct($value){
        $this->value = $value;
    }
    
    public function __toString(){
        return $this->value;
    }
          
    
}

