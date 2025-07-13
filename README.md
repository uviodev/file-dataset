# File Dataset
Cloud Blob Storage systems like S3 are often used as a File System. When performing bulk operations, developers need to "de-reference" data frames that contain references to s3 data.  

We want to provide a workflow that makes the read and write steps to cloud storage clear and explicit.

The user provides:

* data pipelines as python function that operates on local files. This type of pipeline is common in image processing workflows.
* a data frame where columns correspond to file names, and their values corresponed to s3 urls such as s3://my-bucket/large_image.mha

The file_dataset then provides functionality to read these files to local storage or as blobs in memory, with a focus on simplicity:

* Core operations are *eager*. When you call `file_dataset.reader(dataframe).into_temp_dir()` that immediately copies the data, or when you call `file_dataset.write_files()` that copies the files into s3 immediately.
* User may call a Pipeline that reads data locally, invokes their function, and then uploads the outputs to s3. This pipeline can then be called for each row a pandas dataframe.
* Ray Data integration is provided to simplify large-scale dataset processing.

Differences from other libraries:

* Unlike [s3fs](https://s3fs.readthedocs.io/en/latest/) users will have data explicitly read from s3 or written to s3 in the pipeline, without blurring the distinction between local and remote data storage.
* Unlike LanceDB this library does not create a persistent data format. It mainly provides operations and attempts to reuse existing data formats (ie "file system" path structure)


## Example usage
Assume a user has defined a function:

```python
from typing import TypedDict
from pathlib import Path
import SimpleITK as sitk

# use functional pattern as `.mha` is not a valid py identifier.
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

Note that because the core logic is just python, the user could easily embed arbitrary logic such as:

* Forwarding input as an output
* Running a PyTorch model

## Usage 1: Download s3 data to local dir

File dataset can download data

```python
s3_data = {"image.mha": "s3://my-bucket/image.mha"}
with file_dataset.reader(s3_data).into_temp_dir() as tmp:
    # Tmp contains a file: image.mha
    # for simplicty we set input=output though
    # general consumers may prefer two distinct temp paths.
    local_output = resample_local_image_file(
        input_dir=tmp,
        output_dir=tmp)
    # User may now continue processing with `local_output`.
```


## Usage 2: Upload local data to s3
Uploading data requires a user-defined ID.


```python
local_output = {
    "resampled_image.mha": Path("./resampled_image.mha")
}
file_dataset.write_files(
    local_output,
    into_path="s3://my-bucket/resampled/",
    id="1"
)
# Returns: {"image.mha": "s3://my-bucket/resampled/1/image.mha"}
# S3 Object contains the local contents
```

If one provides a local path then the temp files will be copied locally (rather than uploaded to s3):

```python
local_output = {
    "resampled_image.mha": Path("./resampled_image.mha")
}
file_dataset.write_files(
    local_output,
    into_path="./resampled/",
    id="1"
)
# Returns: {"image.mha": Path("./resampled/1/image.mha")}
```

## Usage 3: Implement a Pipeline over Data frames
This API also works for data frames.

When a data frame is provided then each row is sequentially processed. The dataset reads the files for each row into a local temp directory, invokes a user function on the directory, then uploads the output files back to s3.

To avoid out of disk errors, a new temporary directory is used for each row.


```python
file_dataframe = pandas.DataFrame({
    "image.mha": [
        "s3://my-bucket/image1.mha",
        "s3://my-bucket/image2.mha",
        "s3://my-bucket/image3.mha"],
    "id": [
        "1", "2", "3"
    ]
})

pipeline = file_dataset.Pipeline(
    fn=resample_local_image_file,
    write_options={
        "into_path": "s3://my-bucket/resampled/"
    })
output_dataframe = pipeline(file_dataframe)
output_dataframe
# Has contents:
# pandas.DataFrame({
#    "output.mha": [
#        "s3://my-bucket/resampled/1/output.mha",
#        "s3://my-bucket/resampled/2/output.mha",
#        "s3://my-bucket/resampled/3/output.mha"],
#    "id": [
#        1, 2, 3
#    ]
# })
```

If a row fails to process then it is dropped and its ID is not present in the output. Users are encouraged to confirm dataset size during processing.

Note that the output of this pipeline is a data frame can now be compatible with the inputs of another `file_dataset.Pipeline` call.

The Pipeline is pickle-able and it can be passed to a Ray Dataset's `map_batches` function when the batch format is Pandas. See [map_batches](https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.map_batches.html#ray.data.Dataset.map_batches)

```python
# inputs_in_s3.csv contains "image.mha" column and "id" columns.
dataset = ray.data.read_csv("inputs_in_s3.csv")
resampled_dataset = dataset.map_batches(
    file_dataset.Pipeline(
        fn=resample_local_image_file,
        write_options={
            "into_path": "s3://my-bucket/resampled/"
        }),
    batch_format="pandas",
    batch_size=8, # Or however many you'd like per batch.
    # Specify distribution options:
    concurrency=32,  # Or however many you'd like
    # num_gpus=1 if you need GPU
    # memory=512 * 1024**2 for specific memory needed
)
```

Note that here the dataset starts small (s3 urls are extremely small compared to the data they point to). The dataset here also ends small as only the s3 urls are returned, not the data.

So ray data will handle this pattern well out of the box.

## Usage 4: Read Blob data into PyArrow table
Often a user will have a need to load data from s3 into memory for the purposes of downstream processing.

This data can be then loaded into a PyArrow table. The blob data in s3 is fetched efficiently and stored in-memory.



```python
# Assume a 1MB s3 object `image_mask.mha`.
s3_data = {"image.mha": "s3://my-bucket/image_mask.mha"}
pyarrow_table = file_dataset.reader(row=s3_data).into_blob_table()
pyarrow_table.schema()  # {"image.mha": pa.binary()}
# has 1 row, which is 1MB in size
```

To avoid memory errors, the user may also load some metadata about the data first:

```python
# Assume a 1MB s3 object `image_mask.mha`.
s3_data = {"image.mha": "s3://my-bucket/image_mask.mha"}
pyarrow_table = file_dataset.reader(row=s3_data).into_size_table()
pyarrow_table.schema()  # {"image.mha": pa.int32()}
# has 1 row, which has `{"image.mha": 1MB}`
```

TIP: to load a file dataset into a Ray dataset use the optimized dataset reader.

## Ray Dataset Reader
Users may load their data frames into a blob dataset.


```python
dataset = file_dataset.ray.blob_reader(
    files_dataframe, batch_size=4,
    options=file_dataset.Options.default())
```

Under the hood, this function reads a data source that [creates a read task](https://docs.ray.io/en/latest/data/api/doc/ray.data.Datasource.get_read_tasks.html) for every `batch_size` rows to be read together. The file dataset uses `into_size_table` to estimate the in-memory dataset size for the first batch and assumes the estimate is good for the whole dataset. The dataset returns only binary data which the user may then re-interpret.

TIP: when the user calls `map_batches` on this Ray dataset, consider using zero copy mode.

TIP: If the object files are small, consider writing the data to parquet then simply reading the parquet files directly; this will be more efficient for small files but be significantly worse for large image files.

## S3 credentials management
All reader and write functions take as input `file_dataset.Options` which allows the user to specify options.

The default options (`file_dataset.Options.default()`):

* Use `adaptive` retry mode for boto3 session configuration
* Serialize the frozen_credentials of the default boto3 session into the Options

These defaults optimize for Ray where the head node can spin up many ec2 instances downstream to do the actual work

The dataset Options must always be serializable as Pickle objects, so they contain session_kwargs for the `boto3.Session()` object, and s3_client_kwargs for the `session.client("s3")` call. They lazily create this boto3 session and s3 client upon first usage (in a thread safe way) to support reusing one s3 client for each thread for maximum performance.
