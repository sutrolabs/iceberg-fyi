from iceberg_test.base import TestContext
from iceberg_test.storage.minio import MinioStorage

def main():
    with(TestContext() as test_context, MinioStorage(test_context) as storage):
        print(f"bucket_url = {storage.bucket_url}")
        input()

if __name__ == "__main__":
    main()
