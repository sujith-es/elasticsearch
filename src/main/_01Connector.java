package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class _01Connector {

	public static void main(String[] args) throws IOException {
		BufferedReader br = null;
		try {
			URL url = new URL("http://localhost:9200");
			URLConnection conn = url.openConnection();
			br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String inputLine;

			while ((inputLine = br.readLine()) != null) {

				System.out.println(inputLine);
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			br.close();
		}

	}

}
