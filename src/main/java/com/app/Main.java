package com.app;

import com.handler.impl.ESHelper;

public class Main {

	public static void main(String[] args) {

/*		UtilsBundle.extractHumanFromRedisToES();*/
		ESHelper esHelper = ESHelper.getInstance();
		esHelper.searchByIndexAndId("humans", "1");
		esHelper.searchByIndexAndAge("humans", "23");
		esHelper.searchByIndexAndName("humans", "Vasja NO 5");
/*		esHelper.searchByIndexAndApi("humans", "uk");*/
		
		esHelper.closeConnection();
	}

}
