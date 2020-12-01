package tp.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import tp.database.Database;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.Collections;
import java.util.Date;

public class ConsumerUn implements Runnable {

    private static final SimpleDateFormat pattern = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private Consumer<String, String> consumer;
    private static String topic;
    private Database database;
    private JSONParser jsonParser;

    public ConsumerUn() {
        consumer = new ConsumerFactory().createConsumer();
        topic = "Topic1";
        consumer.subscribe(Collections.singletonList(topic));
        database = Database.getInstance();
        jsonParser = new JSONParser();
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(stringStringConsumerRecord -> {
                String s = stringStringConsumerRecord.value();
                try {
                    JSONObject json = (JSONObject) jsonParser.parse(s);
                    if (!json.isEmpty()) {
                        String dateJsonString = (String) json.get("Date");
                        Instant d = Instant.parse(dateJsonString);
                        LocalDateTime sqlDate = d.atZone(ZoneId.of("Europe/Paris")).toLocalDateTime();


                        JSONObject jsonGlobal = (JSONObject) json.get("Global");
                        long new_confirmed = (long) jsonGlobal.get("NewConfirmed");
                        long total_confirmed = (long) jsonGlobal.get("TotalConfirmed");
                        long new_deaths = (long) jsonGlobal.get("NewDeaths");
                        long total_deaths = (long) jsonGlobal.get("TotalDeaths");
                        long new_recovered = (long) jsonGlobal.get("NewRecovered");
                        long total_recovered = (long) jsonGlobal.get("TotalRecovered");

                        database.inserOrUpdatetIntoGlobal(new_confirmed, total_confirmed, new_deaths, total_deaths,
                                new_recovered, total_recovered, sqlDate);

                        JSONArray jsonCountries = (JSONArray) json.get("Countries");
                        if (jsonCountries != null) {
                            jsonCountries.forEach(c -> {

                                JSONObject jsonCourant = (JSONObject) c;
                                String country = (String) jsonCourant.get("Country");
                                String countryCode = (String) jsonCourant.get("CountryCode");
                                String slug = (String) jsonCourant.get("Slug");
                                long new_confirmed_c = (long) jsonCourant.get("NewConfirmed");
                                long total_confirmed_c = (long) jsonCourant.get("TotalConfirmed");
                                long new_deaths_c = (long) jsonCourant.get("NewDeaths");
                                long total_deaths_c = (long) jsonCourant.get("TotalDeaths");
                                long new_recovered_c = (long) jsonCourant.get("NewRecovered");
                                long total_recovered_c = (long) jsonCourant.get("TotalRecovered");
                                try {
                                    database.inserOrUpdatetIntoCountry(country, countryCode, slug, new_confirmed_c,
                                            total_confirmed_c, new_deaths_c, total_deaths_c, new_recovered_c, total_recovered_c,
                                            sqlDate);
                                } catch (SQLException throwables) {
                                    throwables.printStackTrace();
                                }
                            });
                        }


                    }
                } catch (ParseException | SQLException e) {
                    e.printStackTrace();
                }
            });
        }
        consumer.close();
        try {
            database.closeConnection();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        consumer.close();
    }
}
