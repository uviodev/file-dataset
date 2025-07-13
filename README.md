# File Dataset
Cloud Blob Storage systems like S3 are often used as a File System.

Developers want to abstract from the cloud, however, and prefer working with local files, process them locally, then upload them back to object storage.

We want to provide a workflow that makes the upload and download step clear and explicit.

Further, if users are processing lots of little files, it will be more effective if the actual files are fused together into one larger archive object.


In particular, for workflows that typically transform one directory of files to another, the user may want to.


The key aspects are:

* `user_defined_image_procesing` function is independent of the file dataset; it can be called simply with a directory in a stand-alone way
* `file_dataset` handles all the s3 upload and downloads.
* `file_dataset` is eager.

## Example usage
Assume a user has defined a function:

```python
from typing import TypedDict
from pathlib import Path
import SimpleITK as sitk

ResampledOutput = TypedDict(
    'ResampledOutput',
    {
        "output.mha": Path,
    }
)    

def resample_local_image_file(
    input_dir: Path,
    output_dir: Path) -> ResampledOutput:
    image = sitk.ReadImage(working_dir / "input.mha")
    resampled_image = sitk.ResampleImage(image)
    output_file = output_dir / "output.mha"
    sitk.WriteImage(resampled_image, output_file)
    return {"output.mha": output_file}

```

## Usage 1: Download s3 data

File dataset can download data

```python
file_dataset = FileDataset(
    credentials=boto3.Session().get_frozen_credentials())
s3_data = {"image.mha": "s3://my-bucket/image.mha"}
with file_dataset.downloader(s3_data).into_temp_dir() as tmp:
    # Tmp contains a file: image.mha
    local_output = resample_local_image_file(
        input_dir=tmp,
        output_dir=tmp)
    # Can continue processing with `local_output`.
```


## Usage 2: Upload local data to s3
Uploading data requires an ID, if you don't provide one a unique ID will be created for you


```python
local_output = {
    "resampled_image.mha": Path("./resampled_image.mha")
}
file_dataset.upload_files(
    local_output,
    into_path="s3://my-bucket/resampled/"
)
# Returns: {"image.mha": "s3://my-bucket/resampled/abcd-01234-56789/image.mha"}
```

If one provides a local path then the temp files will be copied:

```python
local_output = {
    "resampled_image.mha": Path("./resampled_image.mha")
}
file_dataset.upload_files(
    local_output,
    into_path="./resampled/"
)
# Returns: {"image.mha": Path("./resampled/abcd-01234-56789/image.mha")}
```

## Usage 3: Data frames
This API also works for data frames
When a data frame is provided then each row is sequentially processed. The dataset downloads the files for each row, invokes a user function on the directory, then uploads the output files back to s3.


```python
file_dataset = FileDataset(
    credentials=boto3.Session().get_frozen_credentials())
file_dataframe = pandas.DataFrame({
    "image.mha": [
        "s3://my-bucket/image1.mha",
        "s3://my-bucket/image2.mha",
        "s3://my-bucket/image3.mha"],
    "id": [
        1, 2, 3
    ]
})

file_dataset.map_batch(
    file_dataframe,
    fn=resample_local_image_file,
    upload_options={
        "into_path": "s3://my-bucket/resampled/"
    })
```

For each row in the dataset, the data is copied to a temp dir, processed with `fn`, and then uploaded to a path with its given ID.
