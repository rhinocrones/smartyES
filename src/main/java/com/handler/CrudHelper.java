package com.handler;

public interface CrudHelper {

	void createIndex(String indexName);

	void createDocument(String index, String type, String id, String json);

	void getDocument(String index, String type, String id);

	void deleteDocument(String index, String type, String id);

	void updateDocument(String index, String type, String id, String json);

	void upsertDocument(String index, String type, String id, String json);

	void searchByIndexAndId(String index, String id);
}
