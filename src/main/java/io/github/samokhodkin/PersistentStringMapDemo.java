package io.github.samokhodkin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import io.github.whippetdb.db.api.DbBuilder;
import io.github.whippetdb.db.api.types.CharsIO;

/**
 * An example of persistent database with interactive console interface.
 * 
 * The database is represented as Map<CharSequence,CharSequence> and 
 * is configured as persitent and journaling. 
 * 
 * The console interface allows to add, update, delete and list entries (see usage()).
 * 
 * Thanks to journaling the database is tolerant to sudden process termination or any 
 * software or hardware failure, including power outage. The next time you start the program, 
 * previously entered data will be there.
 * For the same reason we don't worry about flushing of closing the database on exit, 
 * it's all handled by the journal. 
 */

public class PersistentStringMapDemo {
   /**
    * The database file path.
    * Due to the minor bug, the path must contain at least one slash.   
    */
   final static String dbPath = "tmp/PersistentStringMapDemo.db";
   
   /**
    * The database instance.
    * The types in the constructor define the type of the Map. 
    * Persistence is achieved with openOrCreate(path).
    * The journaling(true) makes the database fault-tolerant.
    * The autocommit(true) ensures that each put() or remove() is handled as complete transaction.
    * The synchronize(true) ensures thread safety (not actually nesessary here).
    */
   final static Map<CharSequence, CharSequence> db = 
         new DbBuilder<>(new CharsIO(), new CharsIO())
         .journaling(true)
         .autocommit(true)
         .synchronize(true)
         .openOrCreate(dbPath)
         .asMap();

   /**
    * The command-processing loop.
    * @param args
    * @throws IOException
    */
   public static void main(String[] args) throws IOException {
      BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
      
      for (;;) {
         System.out.print("Enter command: ");

         if(!processCmd(in.readLine().trim())) {
            usage();
         }
      }
   }
   
   private static boolean processCmd(String line) {
      if(line.length() == 0) return false;
      
      String[] cmd = line.split("\\s+");
      
      switch (cmd[0].charAt(0)) {
         case 'l':
            list();
            break;
            
         case 'p':
            if (cmd.length < 3)  return false;
            db.put(cmd[1], cmd[2]);
            break;
            
         case 'q':
            if (cmd.length < 2) return false;
            System.out.println(db.get(cmd[1]));
            break;
            
         case 'd':
            if (cmd.length < 2) return false;
            db.remove(cmd[1]);
            break;
         
         default: return false;
      }
      
      return true;
   }

   private static void list() {
      System.out.println("DB contains " + db.size() + " entries:");
      db.keySet().stream().forEachOrdered(k -> {
         System.out.println("\t" + k + " = " + db.get(k));
      });
   }

   private static void usage() {
      System.out.println("Supported commands:");
      System.out.println("\tl[ist] - list entries");
      System.out.println("\tp[ut] <key> <value> - add/update entry");
      System.out.println("\tq[uery] <key> - query key");
      System.out.println("\td[elete] <key> - delete entry");
   }
}
