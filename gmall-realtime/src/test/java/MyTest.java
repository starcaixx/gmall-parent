import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class MyTest {
    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(1631461178000l), ZoneId.systemDefault());
//        String format = sdf.format(localDateTime.toLocalDate());
        System.out.println(localDateTime.toLocalDate().toString().replace("-", ""));
        System.out.println("20210901".compareTo("20210830"));

        Jedis jedis = new Jedis("localhost", 6379);
        System.out.println(jedis.ping());

    }
}
