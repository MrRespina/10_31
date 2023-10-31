package com.ji.bigdata001;

import java.util.Date;

public class Air {
	
	private int air_num;
	private Date air_msrdt;
	private String air_msrrgn_nm;
	private String air_msrste_nm;
	private int air_pm10;
	private int air_pm25;
	private double air_o3;
	private String air_idex_nm;
	
	public Air(int air_num, Date air_msrdt, String air_msrrgn_nm, String air_msrste_nm, int air_pm10, int air_pm25,
			double air_o3, String air_idex_nm) {
		super();
		this.air_num = air_num;
		this.air_msrdt = air_msrdt;
		this.air_msrrgn_nm = air_msrrgn_nm;
		this.air_msrste_nm = air_msrste_nm;
		this.air_pm10 = air_pm10;
		this.air_pm25 = air_pm25;
		this.air_o3 = air_o3;
		this.air_idex_nm = air_idex_nm;
	}

	public Air() {
		super();
	}

	public int getAir_num() {
		return air_num;
	}

	public void setAir_num(int air_num) {
		this.air_num = air_num;
	}

	public Date getAir_msrdt() {
		return air_msrdt;
	}

	public void setAir_msrdt(Date air_msrdt) {
		this.air_msrdt = air_msrdt;
	}

	public String getAir_msrrgn_nm() {
		return air_msrrgn_nm;
	}

	public void setAir_msrrgn_nm(String air_msrrgn_nm) {
		this.air_msrrgn_nm = air_msrrgn_nm;
	}

	public String getAir_msrste_nm() {
		return air_msrste_nm;
	}

	public void setAir_msrste_nm(String air_msrste_nm) {
		this.air_msrste_nm = air_msrste_nm;
	}

	public int getAir_pm10() {
		return air_pm10;
	}

	public void setAir_pm10(int air_pm10) {
		this.air_pm10 = air_pm10;
	}

	public int getAir_pm25() {
		return air_pm25;
	}

	public void setAir_pm25(int air_pm25) {
		this.air_pm25 = air_pm25;
	}

	public double getAir_o3() {
		return air_o3;
	}

	public void setAir_o3(double air_o3) {
		this.air_o3 = air_o3;
	}

	public String getAir_idex_nm() {
		return air_idex_nm;
	}

	public void setAir_idex_nm(String air_idex_nm) {
		this.air_idex_nm = air_idex_nm;
	}
}
