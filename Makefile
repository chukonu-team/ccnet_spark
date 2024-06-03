VERSION=0.1.9
build_base_ccnet:
	sudo docker build -t base-service-ccnet .
run_ccnet:
	sudo docker run -itd --network host --name base-service-ccnet -v "$(shell pwd):/app" base-service-ccnet
clean:
	sudo docker stop base-service-ccnet
	sudo docker rm base-service-ccnet
use_ccnet:
	sudo docker exec -it base-service-ccnet bash
install_ccnet:
	python3 setup.py sdist && \
	pip install ./dist/ccnet_spark-$(VERSION).tar.gz
test_load:
	python3 test_module/test_load.py --dump=2019-18 --cache_dir="./cached_data/" 
test_pipeline:
	python3 test_module/test_pipeline.py 8 "/metadata0/wxl_data/pdf_result.parquet"