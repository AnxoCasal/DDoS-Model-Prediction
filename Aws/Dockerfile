FROM amazon/aws-glue-libs:glue_libs_3.0.0_image_01

COPY script.py /home/glue_user/workspace/script.py
COPY raw.parquet /home/glue_user/workspace/raw.parquet
COPY glue-raw-staging.py /home/glue_user/workspace/glue-raw-staging.py
COPY glue-staging-business.py /home/glue_user/workspace/glue-staging-business.py

ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
ENV AWS_REGION=$AWS_REGION

CMD ["spark-submit", "--master", "local[*]", "/home/glue_user/workspace/script.py"]
