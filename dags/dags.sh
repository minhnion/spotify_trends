echo "Make folder airflow"

mkdir mkdir -p airflow/{dags,logs,plugins}
sudo chown -R $USER:$USER airflow
chmod -R 775 airflow
