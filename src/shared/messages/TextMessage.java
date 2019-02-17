package shared.messages;

import java.io.Serializable;

/**
 * Represents a simple text message, which is intended to be received and sent 
 * by the server.
 */
public class TextMessage implements KVMessage, Serializable {

        private static final long serialVersionUID = 5549512212003782618L;
        private String msg;
        private byte[] msgBytes;
        private static final char LINE_FEED = 0x0A;
        private static final char RETURN = 0x0D;

        /**
         * Constructs a TextMessage object with a given array of bytes that 
         * forms the message.
         * 
         * @param bytes the bytes that form the message in ASCII coding.
         */
        public TextMessage(byte[] bytes) {
                this.msgBytes = addCtrChars(bytes);
                this.msg = new String(msgBytes).trim();
        }

        /**
        * Constructs a TextMessage object with a given String that
        * forms the message. 
        * 
        * @param msg the String that forms the message.
        */
        public TextMessage(String msg) {
                this.msg = msg;
                this.msgBytes = toByteArray(msg);
        }


        /**
         * Returns the content of this TextMessage as a String.
         * 
         * @return the content of this message in String format.
         */
        // Full Message
        public String getMsg() {
                return msg.trim();
        }

        @Override public String getKey() {
            
            String[] tokens = getMsg().split("\\s+");
            
            // should not have more than 3 parameters
            if (tokens.length > 3) {
                return null;
            }
            
            if (tokens.length == 2 || tokens.length == 3) {
                // it's either PUT <key> <value>
                // or <STATUS> <key> (<value>)
                // or GET <key>
                return tokens[1];
            }
            
            // tokens.length < 2
            // eg. STATUS(error)
            
            return null;
        }

        @Override public String getValue() {
            
            String[] tokens = getMsg().split("\\s+");

            if (tokens.length == 3) {
                // it's either PUT <key> <value>
                // or <STATUS> <key> <value>
                return tokens[2];
            }
            
            // tokens.length < 2
            // eg. STATUS(error)
            
            return null;
        }

        @Override public StatusType getStatus() {
            
            String[] tokens = getMsg().split("\\s+");
            
            // should not have more than 3 parameters
            if (tokens.length > 3) {
                return null;
            }
            
            if (tokens.length > 0) {
                String status_str = tokens[0];
                try {
                    return StatusType.valueOf(status_str);
                    
                } catch (IllegalArgumentException e) {
                    // No match of the status type is found in the enum block.
                    return null;
                }
            }
            
            return null;
        }
        
        public int isValid() {            
            // must have a status
            StatusType status = getStatus();
            if (status == null) {
                return 0;
            }
            
            // must have a key and value if status valid
            if (status == StatusType.GET_SUCCESS || status == StatusType.PUT_SUCCESS || status == StatusType.PUT_UPDATE || status == StatusType.DELETE_SUCCESS) {
                if (getKey() == null || getValue() == null) return 0;
                return 1;
            }
            
            return 1;
        }

        /**
         * Returns an array of bytes that represent the ASCII coded message content.
         * 
         * @return the content of this message as an array of bytes 
         * 		in ASCII coding.
         */
        public byte[] getMsgBytes() {
                return msgBytes;
        }

        private byte[] addCtrChars(byte[] bytes) {
                byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
                byte[] tmp = new byte[bytes.length + ctrBytes.length];

                System.arraycopy(bytes, 0, tmp, 0, bytes.length);
                System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

                return tmp;		
        }

        private byte[] toByteArray(String s){
                byte[] bytes = s.getBytes();
                byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
                byte[] tmp = new byte[bytes.length + ctrBytes.length];

                System.arraycopy(bytes, 0, tmp, 0, bytes.length);
                System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

                return tmp;		
        }

}
