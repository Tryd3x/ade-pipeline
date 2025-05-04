Zip job packages before shipping it as dependency for spark jobs
```
zip -r process_raw_layer.zip process_raw_layer -x '*.pyc' -x '__pycache__/*'
```