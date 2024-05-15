build_base_ccnet:
	docker build -t base-service-ccnet .
run_ccnet:
	sudo docker run -itd --network host  --name  base-service-ccnet  -v "/home/zz:/app" -v "/metadata0:/metadata0" -v "/data0:/data0" -d base-service-ccnet
use_ccnet:
	sudo docker exec -it base-service-ccnet bash
profile_ccnet:
	python3 local_test.py 8