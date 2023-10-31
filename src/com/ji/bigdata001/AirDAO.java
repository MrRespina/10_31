package com.ji.bigdata001;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserFactory;

import com.chung.db.managaer.JiDBManager;
import com.ji.http001.JiHttpClient;

// http://openapi.seoul.go.kr:8088/70646b63596a69683437716f794e49/xml/RealtimeCityAir/1/25/
// Model > DAO
// 원래 Method는 알파벳 순으로 정령.

public class AirDAO {

	public static ArrayList<Air> getAir() throws Exception {

		String url = "http://openapi.seoul.go.kr:8088/70646b63596a69683437716f794e49/xml/RealtimeCityAir/1/25/";
		InputStream is = JiHttpClient.download(url);
		ArrayList<Air> ar = new ArrayList<Air>();
		Air a = null;

		XmlPullParserFactory xppf = XmlPullParserFactory.newInstance();
		XmlPullParser xpp = xppf.newPullParser();
		xpp.setInput(is, "UTF-8");

		int type = xpp.getEventType();
		String tagName = null;

		while (type != XmlPullParser.END_DOCUMENT) {

			if (type == XmlPullParser.START_TAG) { // <>

				tagName = xpp.getName();
				if (tagName.equals("row")) {
					a = new Air();
				}

			} else if (type == XmlPullParser.TEXT) {

				if (tagName.equals("MSRDT")) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
					Date d = sdf.parse(xpp.getText());
					a.setAir_msrdt(d);

				} else if (tagName.equals("MSRRGN_NM")) {
					a.setAir_msrrgn_nm(xpp.getText());
				} else if (tagName.equals("MSRSTE_NM")) {
					a.setAir_msrste_nm(xpp.getText());
				} else if (tagName.equals("PM10")) {
					a.setAir_pm10(Integer.parseInt(xpp.getText()));
				} else if (tagName.equals("PM25")) {
					a.setAir_pm25(Integer.parseInt(xpp.getText()));
				} else if (tagName.equals("O3")) {
					a.setAir_o3(Double.parseDouble(xpp.getText()));
				} else if (tagName.equals("IDEX_NM")) {
					a.setAir_idex_nm(xpp.getText());
					ar.add(a);
				}

			} else if (type == XmlPullParser.END_TAG) { // </>
				tagName = "";
			}

			xpp.next(); // 다음 Data로 넘어감.
			type = xpp.getEventType(); // 다음 TAG 값을 가짐.

		}

		return ar;

	}

	public static String whireAir(ArrayList<Air> alar) {

		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		int res = 0;

		try {
			con = JiDBManager.connect();

			for (int i = 0; i < alar.size(); i++) {

				sql = "INSERT INTO seoul_air VALUES(seoul_air_seq.nextval,to_date(?,'YYYY-MM-DD HH24:MI:SS'),?,?,?,?,?,?)";
				pstmt = con.prepareStatement(sql);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

				// util.Date 형 > sql.Date 형으로 변환하는 작업. (같은 이름의 Date형이지만 다른 객체이다.)
				// Date 형을 변환하면 시/분/초가 사라져서 넘어가기 떄문에 원하는 값을 넣을수가 없다. ('2023-10-31' 이런 식으로 변환되어버림.)
				
				/*	util.Date > sql.Date 변환 코드
				java.util.Date utilDate = new java.util.Date();
				utilDate = alar.get(i).getAir_msrdt();
				java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
				*/

				// 그래서 String 형으로 형변환 후에 to_date로 데이터 형으로 만들어주는 방법을 선택했다.
				
				String dat = sdf.format(alar.get(i).getAir_msrdt());

				pstmt.setString(1, dat);
				pstmt.setString(2, alar.get(i).getAir_msrrgn_nm());
				pstmt.setString(3, alar.get(i).getAir_msrste_nm());
				pstmt.setInt(4, alar.get(i).getAir_pm10());
				pstmt.setInt(5, alar.get(i).getAir_pm25());
				pstmt.setDouble(6, alar.get(i).getAir_o3());
				pstmt.setString(7, alar.get(i).getAir_idex_nm());

				res = pstmt.executeUpdate();

			}

			return "DB 입력 성공 : " + res;

		} catch (Exception e) {
			e.printStackTrace();
			return "DB 입력 에러";
		} finally {
			JiDBManager.close(con, pstmt, rs);
		}

	}

}
