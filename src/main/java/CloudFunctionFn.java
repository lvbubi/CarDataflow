import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CloudFunctionFn extends DoFn<String, String> {

    private final String paramName;
    private final String pathSegment;

    public CloudFunctionFn(String function, String paramName){
        this.paramName = paramName;
        this.pathSegment = function;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        MainApp.CarPipelineOptions options = context.getPipelineOptions().as(MainApp.CarPipelineOptions.class);

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            URI uri = new URIBuilder(options.getFunctionBase())
                    .setPathSegments(pathSegment)
                    .setParameter(paramName, context.element())
                    .build();

            HttpGet request = new HttpGet(uri);

            context.output(client.execute(request, response -> EntityUtils.toString(response.getEntity())));
        } catch (URISyntaxException | IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws JsonProcessingException {
        CloudFunctionFn scrapeFunction = new CloudFunctionFn("car_scraper", "car_type");
        CloudFunctionFn visionFunction = new CloudFunctionFn("car_vision", "image_uri");


        //System.out.println(scrapeFunction.apply("bmw"));


        //System.out.println(visionFunction.apply("gs://car-dataflow/images/audi.jpeg"));
    }
}
