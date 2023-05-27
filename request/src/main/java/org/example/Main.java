package org.example;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import java.io.BufferedReader;
import java.io.InputStreamReader;


public class Main {
    public static void sendMessageToFlaskApp(String message) {
        try {
            CloseableHttpClient client = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost("http://elastic-service:5000/api"); // Replace <flask-app-service> with the service name or IP of your Flask app

            // Set the message in the request payload
            StringEntity entity = new StringEntity("{\"path\": \"" + message + "\"}");
            httpPost.setEntity(entity);
            httpPost.setHeader("Content-Type", "application/json");

            // Execute the POST request
            HttpResponse response = client.execute(httpPost);

            // Read and print the response
            BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String line;
            StringBuilder result = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
            System.out.println("Response from Flask app: " + result.toString());

            // Close the HTTP client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


        public static void main(String[] args) {
            String message = "example.parquet"; // Replace with the actual file path you want to send
            sendMessageToFlaskApp(message);
        }

    }
