package com.ji.bigdata001;

import java.util.ArrayList;

public class GetAirController {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ArrayList<Air> alar = new ArrayList<Air>();

		try {
			
			alar = AirDAO.getAir();
			String result = AirDAO.whireAir(alar);
			System.out.println(result);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
