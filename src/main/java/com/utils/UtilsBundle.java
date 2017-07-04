package com.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.entity.Human;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.handler.impl.ESHelper;
import redis.clients.jedis.Jedis;

public class UtilsBundle {

	private final static Jedis JEDIS = new Jedis("localhost");

	private final static Gson gson = new GsonBuilder().setPrettyPrinting()
	        .create();

	public static String createSource(String room, String color) {
		try {
			return XContentFactory.jsonBuilder().startObject()
			        .field("room_name", room).field("color", color).endObject()
			        .string();
		} catch (IOException e) {

			e.printStackTrace();
		}
		return null;
	}

	public static void preapareDocs(ESHelper esUtils) {
		esUtils.createDocument("house", "room", "1",
		        UtilsBundle.createSource("livingroom", "red"));
		esUtils.createDocument("house", "room", "2",
		        UtilsBundle.createSource("familyroom", "white"));
		esUtils.createDocument("house", "room", "3",
		        UtilsBundle.createSource("kitchen", "blue"));
		esUtils.createDocument("house", "room", "4",
		        UtilsBundle.createSource("bathroom", "white"));
		esUtils.createDocument("house", "room", "5",
		        UtilsBundle.createSource("garage", "blue"));
	}

	public static void extractHumanFromRedisToES() {
		fillUpRedis();
		System.out.println("Filled");
		try {
			@SuppressWarnings("resource")
			Client client = new PreBuiltTransportClient(Settings.EMPTY)
			        .addTransportAddress(new InetSocketTransportAddress(
			                InetAddress.getByName("localhost"), 9300));
			for (int j = 0; j < 10; j++) {
				final String i = Integer.toString(j);
				client.prepareIndex("humans", "human").setId(i)
				        .setSource(JEDIS.lpop("Human"), XContentType.JSON)
				        .execute();
			}
			client.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	private static void fillUpRedis() {
		for (int j = 0; j < 10; j++) {
			Random random = new Random();
			int randomInt = random.nextInt(4);
			Human human = new Human();
			human.setName("Vasja NO " + j);
			human.setAge(18 + j);
			if (randomInt == 3) {
				human.getApis().add("uk");
				human.getApis().add("en");
				human.getApis().add("ru");
			} else if (randomInt == 2) {
				human.getApis().add("en");
				human.getApis().add("uk");
			} else {
				human.getApis().add("uk");
			}
			String objectForStore = gson.toJson(human);
			JEDIS.rpush("Human", objectForStore);
		}
	}
}
