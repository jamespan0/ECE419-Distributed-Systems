package app_kvServer;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;

class LFUCache {

    class Cache {
        private int frequency;
        private String key;

        public Cache(String key) {
                this.frequency = 0 ; //initialized
                this.key = key;
        }

        public void incr_freq() {
            this.frequency = this.frequency + 1 ;
        }

        public void set_freq(int newFreq) {
            this.frequency = newFreq;
        }

        public int get_freq() {
            return this.frequency;
        }

        public String LFU_getKey() {
            return this.key;
        }

        public void resetFreq() {
            this.frequency = 0;
        }   

    }

    //public HashMap<String, String> KV_LFU;
    public HashMap<String, String> KV_LFU = new HashMap<String, String>();
    public List<Cache> LFU_cache = new ArrayList<Cache>();
    public Comparator<Cache> freq_comparator = new Comparator<Cache>() {
        public int compare(Cache c1, Cache c2) {
            return c1.frequency - c2.frequency;
        }
    };
    private int maxSize;

    public
        LFUCache(int capacity) {
            this.maxSize = capacity;
        //    HashMap<String, String> KV_LFU = new HashMap<String, String>();
        }
/*
//For testing purposes
        public static void main(String[] args) {
        }
*/

        public int getSize() {
            return this.maxSize;
        }
        // is the key in the cache?

        public boolean lfu_containsKey (String key) {
            return KV_LFU.containsKey(key); 
        }

        public String lfu_get(String key) {

            // 1st find if it is in cache --> 
            if (lfu_containsKey(key)) {
                // cache contains key, need to update value
                for (int i = 0; i < LFU_cache.size(); i++) {
                    if (key.equals(LFU_cache.get(i).LFU_getKey())) {
                        //found the keyA
                        LFU_cache.get(i).incr_freq(); //update the frequency
                        Collections.sort(LFU_cache,new Comparator<Cache>(){
                                             public int compare(Cache c1,Cache c2){
                                                   return c1.get_freq() - c2.get_freq();
                                             }});
                        return KV_LFU.get(key);
                    }
                }
            } 
            return "ERROR_NO_KEY_FOUND";
        }   

        public void lfu_put(String key, String value) {
            if (lfu_containsKey(key)) {
                //evict old and replace
                Cache renewCache = new Cache(key);
                for (int i = 0; i < LFU_cache.size(); i++) {
                    if (LFU_cache.get(i).LFU_getKey().equals(key)) {
                        // evict and increment frequency
                        renewCache.set_freq(LFU_cache.get(i).get_freq());
                        LFU_cache.remove(i);
                        renewCache.incr_freq();
                        LFU_cache.add(renewCache);
                        Collections.sort(LFU_cache,new Comparator<Cache>(){
                                             public int compare(Cache c1,Cache c2){
                                                   return c1.get_freq() - c2.get_freq();
                                             }});
                        break;
                    }
                }
            } else if (getSize() == LFU_cache.size()) {
                //new entry, but cache is full
                lfu_evict();
                KV_LFU.put(key,value);  //input into hashmap for locating
                Cache newEntry = new Cache(key);
                newEntry.incr_freq();
                LFU_cache.add(newEntry);
                Collections.sort(LFU_cache,new Comparator<Cache>(){
                                     public int compare(Cache c1,Cache c2){
                                           return c1.get_freq() - c2.get_freq();
                                     }});
            } else {
                //no eviction otherwise
                KV_LFU.put(key,value);  //input into hashmap for locating
                Cache newEntry = new Cache(key);
                newEntry.incr_freq();
                LFU_cache.add(newEntry);
                Collections.sort(LFU_cache,new Comparator<Cache>(){
                                     public int compare(Cache c1,Cache c2){
                                           return c1.get_freq() - c2.get_freq();
                                     }});
            }


        }

        public void lfu_evict() {
            //evict first --> one with smallest frequency
            LFU_cache.get(0).resetFreq();
            String evict_key = LFU_cache.get(0).LFU_getKey();
            KV_LFU.remove(evict_key);
            LFU_cache.remove(0);
        }

        public void lfu_remove(String key){
            // removed in the value is null case
            KV_LFU.remove(key);
            for (int i = 0; i < LFU_cache.size(); i++) {
                if (key.equals(LFU_cache.get(i).LFU_getKey())) {
                    LFU_cache.get(i).resetFreq();
                    LFU_cache.remove(i);
                    break;
                }
            }
        }

        public void lfu_clear() {
            KV_LFU.clear();
            LFU_cache.clear();
        }

        public void lfu_print() {

            for (int i = 0; i < LFU_cache.size(); i++) {
                String keyval = LFU_cache.get(i).LFU_getKey();
                String valval = KV_LFU.get(keyval);
                //Do something
                System.out.println("System key: " + keyval + " with value: " + valval);
            }
        }


};
