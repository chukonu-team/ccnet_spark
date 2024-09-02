VERSION=0.1.10
process?=8
servers?=0
lang = en
# List of languages for LM.
langs?=af,ar,az,be,bg,bn,ca,cs,da,de,el,en,es,et,fa,fi,fr,gu,he,hi,hr,hu,hy,id,\
is,it,ja,ka,kk,km,kn,ko,lt,lv,mk,ml,mn,mr,my,ne,nl,no,pl,pt,ro,ru,uk,zh

# DISTRIBUTE will run locally, or on slurm if "servers" is set.
DISTRIBUTE=xargs -L1 -P $(process)
ifneq ($(servers), 0)
	DISTRIBUTE=xargs -L1 -P $(servers) srun -t 240 --mem 5000
endif



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
	python3 test_module/test_pipeline.py 8 "./cached_data/pdf_result.parquet"

# Installation
cached_data/lid.bin:
	# DL languages id from Fasttext releases.
	mkdir -p $(@D)
	wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin -O $@

dl_lm:
	# Download a pretrained language model
	mkdir -p cached_data/lm_sp
	wget -c  -P cached_data/lm_sp http://dl.fbaipublicfiles.com/cc_net/lm/$(lang).arpa.bin
	wget -c  -P cached_data/lm_sp http://dl.fbaipublicfiles.com/cc_net/lm/$(lang).sp.model

dl_all_lms:
	# Download all pretrained language models
	echo "$(langs)" \
		| tr -s ', ' '\n' | awk '{print "lang=" $$0 " dl_lm"}' \
		| $(DISTRIBUTE) make
### cluster:
profile_chu:
	bash cluster_run.sh use_chu test_module/test_cluster.py
profile_spark:
	bash cluster_run.sh use_spark test_module/test_cluster.py