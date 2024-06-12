FROM amazon/aws-glue-libs:glue_libs_3.0.0_image_01

COPY script.py /home/glue_user/workspace/script.py
COPY raw.parquet /home/glue_user/workspace/raw.parquet

ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
ENV AWS_REGION=us-east-1

CMD ["spark-submit", "--master", "local[*]", "/home/glue_user/workspace/script.py"]
