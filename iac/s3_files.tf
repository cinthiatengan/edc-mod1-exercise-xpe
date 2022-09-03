resource "aws_s3_object" "codigo_spark" {
    bucket = aws_s3_bucket.datalake.dl.id
    key    = "emr-code/pyspark/job_spark_from_tf.py"
    acl    = "private"
    source = "../job_spark.py"
    etag   = filemd5("../job_spark.py")
}