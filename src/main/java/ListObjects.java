import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ListObjects {

  public static List<String> getImagePaths(String projectId, String bucketName) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    Page<Blob> blobs = storage.list(
            bucketName
            //Storage.BlobListOption.prefix("images/")
    );
    return StreamSupport.stream(blobs.iterateAll().spliterator(),false)
            .map(Blob::getName)
            .filter(name -> name.endsWith("jpeg") || name.endsWith("jpg"))
            .map(name -> String.format("gs://%s/%s", bucketName, name))
            .collect(Collectors.toList());
  }
}