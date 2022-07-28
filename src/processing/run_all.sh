for i in $(ls ../../data/raw/delfi/2022*.zip); 
do 
	python 0_find_fernverkehr_routen.py $i;
	python 1_processstops-db.py $i;
	python 2_pointcount_full-dataset.py $i; 
done
