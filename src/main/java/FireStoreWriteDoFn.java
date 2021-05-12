import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

class FirestoreWriteDoFn<In> extends DoFn<In, Void> {

    private static final long serialVersionUID = 2L;
    private transient Firestore db;

    public FirestoreWriteDoFn() {
    }

    @StartBundle
    public void setupFirestore(StartBundleContext startBundleContext) throws IOException {
        FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance().toBuilder()
                .setProjectId("car-dataflow")
                .setCredentials(GoogleCredentials.getApplicationDefault())
                .build();
        this.db = firestoreOptions.getService();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        Map<?,?> jsonResult = mapper.readValue(Objects.requireNonNull(context.element()).toString(), LinkedHashMap.class);

        DocumentReference docRef = db.collection("cars")
                .document(jsonResult.get("car_type").toString());

// Add document data  with id "alovelace" using a hashmap
        Map<String, Object> data = new HashMap<>();
        data.put("prices", jsonResult.get("prices"));
        data.put("reviews", jsonResult.get("reviews"));
//asynchronously write data
        ApiFuture<WriteResult> result = docRef.set(data);
// ...
// result.get() blocks on response
        LoggerFactory.getLogger(FirestoreWriteDoFn.class).info("Update time : " + result.get().getUpdateTime());
    }
    @FinishBundle
    public synchronized void finishBundle(FinishBundleContext context) throws Exception {
        db.close();
    }

    public static void main(String... args) throws IOException, ExecutionException, InterruptedException {
        FirestoreOptions firestoreOptions =
                FirestoreOptions.getDefaultInstance().toBuilder()
                        .setProjectId("car-dataflow")
                        .setCredentials(GoogleCredentials.getApplicationDefault())
                        .build();
        Firestore db = firestoreOptions.getService();

        DocumentReference docRef = db.collection("Test").document("test");
// Add document data  with id "alovelace" using a hashmap
        Map<String, Object> data = new HashMap<>();
        data.put("first", Arrays.asList("asd", "asdasd"));
        data.put("last", "Lovelace");
        data.put("born", 1815);
//asynchronously write data
        ApiFuture<WriteResult> result = docRef.set(data);
// ...
// result.get() blocks on response
        System.out.println("Update time : " + result.get().getUpdateTime());
    }
}