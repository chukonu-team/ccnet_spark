build_base_kenlm:
	docker build -t base-service-kenlm -f base-service-kenlm.dockerfile .
build_base_ccnet:
	docker build -t base-service-ccnet -f base-service-ccnet.dockerfile .
server:
	docker build -t kenlm-service  .
	docker run -it -p 8000:8000 -v "/metadata0:/metadata0" kenlm-service
test_req:
	python test_request.py
run_ccnet:
	sudo docker run -itd --network host  --name  base-service-ccnet  -v "/home/zz:/app" -v "/metadata0:/metadata0" -v "/data0:/data0" -d base-service-ccnet
use_ccnet:
	sudo docker exec -it base-service-ccnet bash
profile_ccnet:
	python3 test_ccnet.py 8
profile_cluster:
	bash /opt/chukonu/python/run.sh /home/zz/kenlm_service/test_cluster.py