package testing;

import org.junit.Test;

import junit.framework.TestCase;

import client.KVStore;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class AdditionalTest extends TestCase {

	private KVStore kvClient;
	
	@Test
	public void testGetDeleted() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}
/*	
	@Test
	public void testGetUpdated() {
		String key = "getUpdateTestValue";
		String value = "toUpdate";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "updatedTestValue");
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getValue().equals("updatedTestValue"));
	}

    */
/*	
	@Test
	public void testPutDeleted() {
		String key = "putDeleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			response = kvClient.put(key, "newValue");
		} catch (Exception e) {
			ex = e;
		}

		//should return PUT_SUCCESS instead of PUT_UPDATED 
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
    */
}
