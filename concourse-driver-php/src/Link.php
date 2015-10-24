<?php

/**
 * A Link is a wrapper around a {@link Long} that represents the primary key of 
 * a record and distinguishes from simple long values. A Link is returned from 
 * read methods in Concourse if data was added using one of the #link operations.
 */
class Link{
    
    public static function to($record){
        return new Link($record);
    }
    
    private $record;
    
    private function __construct($record) {
        $this->record = $record;
    }
    
    public function __toString() {
        return "@".$this->record."@";
    }
    
    public function getRecord(){
        return $this->record;
    }
    
}
