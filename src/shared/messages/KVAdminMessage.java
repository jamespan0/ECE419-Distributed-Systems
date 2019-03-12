package shared.messages;

import java.io.Serializable;

public class KVAdminMessage implements Serializable {

        private static final long serialVersionUID = 5549512212003782618L;
        private String msg;
        private byte[] msgBytes;
        private static final char LINE_FEED = 0x0A;
        private static final char RETURN = 0x0D;
	
        public enum Command {
                INIT, 
                START, 
                STOP, 
                SHUT_DOWN, 
                LOCK,
                UNLOCK, 
                UPDATE,
                NULL
        }

        public KVAdminMessage(byte[] bytes) {
                this.msgBytes = addCtrChars(bytes);
                this.msg = new String(msgBytes).trim();
        }

        public KVAdminMessage(String msg) {
                this.msg = msg;
                this.msgBytes = toByteArray(msg);
        }


        public String getMsg() {
                return msg.trim();
        }

        public String getCommand() {
            
            String[] tokens = getMsg().split("\\s+");
            
            if (tokens.length > 0) {
                try {
                    return tokens[0];
                    
                } catch (IllegalArgumentException e) {
                    // No match of the status type is found in the enum block.
                    return null;
                }
            }
            
            return null;
        }

        public String getCacheSize() {
            
            String[] tokens = getMsg().split("\\s+");
            
            if (tokens.length > 1) {
            	return tokens[1];
            }
            
            return null;
        }

        public String getCacheStrategy() {
            
            String[] tokens = getMsg().split("\\s+");
            
            if (tokens.length > 1) {
            	return tokens[2];
            }
            
            return null;
        }

        public String getMetadata() {
            
            String[] tokens = getMsg().split(" ", 4);
            
            if (tokens[0] == "INIT") {
				return tokens[3];
            } else {
				return tokens[1];
			}
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
