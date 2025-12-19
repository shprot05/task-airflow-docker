# 1 Клонировать репу
git clone https://github.com/shprot05/task-airflow-docker.git

# 2 создать .env файл
добавить строчку: AIRFLOW_UID=50000
# 3 Настроить конекшены в Airlow
----первый connection:----

Connection Id	-  fs_default

Connection Type  -  File (path)

Extra - {"path": "/"}

----Второй connection:----

Connection Id -	mongo_default

Connection Type -	MongoDB

Host	- mongo

Port -	27017

Login	- admin

Password - admin

Schema	- admin

# 4  Запустить докер
docker compose up -d --build

# 5 В папку data после запуска daga положить исходный csv
