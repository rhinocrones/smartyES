package com.handler.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import static org.elasticsearch.index.query.QueryBuilders.*;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.handler.CrudHelper;

public class ESHelper implements CrudHelper {

	private static volatile ESHelper instance;

	private Client client;

	@SuppressWarnings({ "resource" })
	private ESHelper() {
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
			        .addTransportAddress(new InetSocketTransportAddress(
			                InetAddress.getByName("localhost"), 9300));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public static ESHelper getInstance() {
		ESHelper localInstance = instance;
		if (localInstance == null) {
			synchronized (ESHelper.class) {
				localInstance = instance;
				if (localInstance == null) {
					instance = localInstance = new ESHelper();
				}
			}
		}
		return localInstance;
	}

	public void closeConnection() {
		if (this.client != null) {
			this.client.close();
		}
	}

	@Override
	public void createIndex(String indexName) {
		client.admin().indices().create(new CreateIndexRequest(indexName));
	}

	@Override
	public void createDocument(String index, String type, String id,
	        String json) {
		System.out.println(
		        "Document is created: " + client.prepareIndex(index, type)
		                .setId(id).setSource(json, XContentType.JSON).execute()
		                .actionGet().toString());
	}

	@Override
	public void getDocument(String index, String type, String id) {
		System.out.println(client.prepareGet(index, type, id).execute()
		        .actionGet().toString());
	}

	@Override
	public void deleteDocument(String index, String type, String id) {
		System.out.println("Document is deleted");
		getDocument(index, type, id);
		client.prepareDelete(index, type, id).execute();

	}

	@Override
	public void updateDocument(String index, String type, String id,
	        String json) {
		System.out.println("Updated document: " + client
		        .prepareUpdate(index, type, id).setDoc(json, XContentType.JSON)
		        .execute().actionGet().toString());
	}

	@Override
	public void upsertDocument(String index, String type, String id,
	        String json) {
		IndexRequest indexRequest = new IndexRequest(index, type, id)
		        .source(json, XContentType.JSON);
		UpdateRequest updateRequest = new UpdateRequest(index, type, id)
		        .doc(json, XContentType.JSON).upsert(indexRequest);
		try {
			System.out.println(client.update(updateRequest).get().toString());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	public void searchByIndexAndId(String index, String id) {
		QueryBuilder builder = termQuery("_id", id);
		System.out.println(client.prepareSearch(index).setQuery(builder)
		        .execute().actionGet());
	}

	public void searchByIndexAndName(String index, String name) {
		QueryBuilder builder = termQuery("name", name);
		System.out.println(client.prepareSearch(index).setQuery(builder)
		        .execute().actionGet());
	}

	public void searchByIndexAndAge(String index, String age) {
		QueryBuilder builder = termQuery("age", age);
		System.out.println(client.prepareSearch(index).setQuery(builder)
		        .execute().actionGet());
	}

	public void searchByIndexAndApi(String index, String api) {
		QueryBuilder builder = termQuery("apis", api);
		System.out.println(client.prepareSearch(index).setQuery(builder)
		        .execute().actionGet());
	}
}
