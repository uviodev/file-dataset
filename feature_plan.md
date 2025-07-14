# Task 3: Implement simple Options class with S3 client caching

**Status**: In progress

**Description**: Create Options class to manage S3 credentials and client creation with thread-safe caching.

**Requirements**:
- Implement `file_dataset.Options` class
- Support `Options.default()` class method using boto3 default session frozen_credentials. use adapative client retries config retry mode is "adaptive" with 3 retries.
- Lazy initialization of S3 client with thread-safe caching
- Make Options serializable (pickle-able) for distributed computing
- Store session_kwargs and s3_client_kwargs instead of live clients

**Testing**:
- Test default options creation
- Test S3 client lazy initialization
- Test thread safety with concurrent access
- Test pickle serialization/deserialization
- Test s3transfer options (multipart threshold and multipart size can be configured)

## Planning

### High-level implementation plan:

1. Create the Options class in `src/file_dataset/options.py`
2. Implement the class with:
   - Constructor that stores session_kwargs and s3_client_kwargs
   - `default()` class method that creates Options with boto3 default credentials (frozen)
   - Lazy S3 client creation with thread-safe caching using threading.Lock
   - Properties to get s3_client and configure s3transfer settings
   - Support for pickle serialization (store only config, not live clients)

3. Key design considerations:
   - Always use default session frozen credentials (no custom credentials support)
   - Store configuration parameters, not live boto3 objects (for pickling)
   - Use threading.Lock for thread-safe client initialization
   - Configure adaptive retry mode with 3 retries as specified
   - Allow s3transfer options to be configurable (multipart_threshold and multipart_chunksize)

### Testing plan:
1. Test default options creation with boto3 default session
2. Test S3 client lazy initialization and caching
3. Test thread safety with concurrent client access
4. Test pickle/unpickle functionality
5. Test s3transfer configuration options

## Implementation

Now implementing the Options class with frozen credentials from default session.
