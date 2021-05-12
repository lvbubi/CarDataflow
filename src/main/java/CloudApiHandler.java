import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Function;

public class CloudApiHandler implements Function<String, String> {

    public static final String PROJECT = "europe-west3-car-dataflow";
    private static final String API_URL = "https://%s.cloudfunctions.net";

    private final String paramName;
    private final String pathSegment;

    public CloudApiHandler(String function, String paramName){
        this.paramName = paramName;
        this.pathSegment = function;
    }

    @Override
    public String apply(String s) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            URI uri = new URIBuilder(String.format(API_URL, PROJECT))
                    .setPathSegments(pathSegment)
                    .setParameter(paramName, s)
                    .build();

            HttpGet request = new HttpGet(uri);

            //return "audiYeah";
            return client.execute(request, response -> EntityUtils.toString(response.getEntity()));
        } catch (URISyntaxException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws JsonProcessingException {
        CloudApiHandler scrapeFunction = new CloudApiHandler("car_scraper", "car_type");
        CloudApiHandler visionFunction = new CloudApiHandler("car_vision", "image_uri");


        //System.out.println(scrapeFunction.apply("bmw"));


        System.out.println(visionFunction.apply("gs://car-dataflow/images/audi.jpeg"));
    }
}
