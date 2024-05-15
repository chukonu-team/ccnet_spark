VERSION=0.1.1
build_base_ccnet:
	sudo docker build -t base-service-ccnet .
run_ccnet:
	sudo docker run -itd --network host --name base-service-ccnet -v "$(shell pwd):/app" -v "/metadata0:/metadata0" base-service-ccnet
use_ccnet:
	sudo docker exec -it base-service-ccnet bash
install_ccnet:
	python3 setup.py sdist && \
	pip install ./dist/ccnet_spark-$(VERSION).tar.gz
profile_ccnet:
	python3 local_test.py 8 "/metadata0/wxl_data/pdf_result.parquet"
