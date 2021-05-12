import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class MainApp {

    private static final CloudApiHandler SCRAPE_API = new CloudApiHandler("car_scraper", "car_type");
    private static final CloudApiHandler VISION_API = new CloudApiHandler("car_vision", "image_uri");

    private static final DoFn<String, String> VISION_FN = new CloudFunctionFn("car_vision", "image_uri");
    private static final DoFn<String, String> SCRAPE_FN = new CloudFunctionFn("car_scraper", "car_type");

    /**
     * Options supported by {@link MainApp}.
     *
     * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments to
     * be processed by the command-line parser, and specify default values for them. You can then
     * access the options values in your pipeline code.
     *
     * <p>Inherits standard configuration options.
     */
    public interface CarPipelineOptions extends GcpOptions {
        @Required
        @Description("Bucket of input images")
        String getBucket();
        void setBucket(String value);

        @Required
        @Description(("Base url of Google Cloud functions"))
        String getFunctionBase();
        void setFunctionBase(String value);
    }

    static void runCarDataflow(CarPipelineOptions options) {
        Pipeline p = Pipeline.create(options);
        p.apply("Read Images", Create.of(ListObjects.getImagePaths(options.getProject(), options.getBucket())))
                .apply("Vision Api", ParDo.of(VISION_FN))
                .apply("Scrape API", ParDo.of(SCRAPE_FN))
                .apply("SaveToFireStore", ParDo.of(new FirestoreWriteDoFn<>()));
        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        CarPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(CarPipelineOptions.class);
        runCarDataflow(options);

        //System.out.println(ListObjects.getImagePaths("cloud9-305911", "cloud9-car"));
    }
}
