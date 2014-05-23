package org.cinchapi.concourse.util;


import java.lang.reflect.Field;
import org.cinchapi.concourse.annotate.UtilityClass;
 /**
 * This file contains String related utility functions
 **/

@UtilityClass
public class Strings {


    /**
     * Use of this method is to nullify the strings.
     * Warning: this method will modify the string in place which departs from the normal contract that strings are immutable
     * and will also make the string unusable
     **/
    public static void nullify(String string){
        try{
            Field value = String.class.getDeclaredField("value");
            value.setAccessible(true);
            char[] mem = (char[]) value.get(string);
            for(int i=0; i<mem.length; i++){
                mem[i] = (Character) null;
            }
        }
        catch(Exception e){
            throw Throwables.propogate(e);
        }

    }

}
