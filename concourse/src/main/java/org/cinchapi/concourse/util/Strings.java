package org.cinchapi.concourse.util;

import java.lang.reflect.Field;
import org.cinchapi.concourse.annotate.UtilityClass;


@UtilityClass
public class Strings {
    /**
     * Use of this method is to nullify the strings.
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
            System.out.println(e.getStackTrace());
        }

    }

}
