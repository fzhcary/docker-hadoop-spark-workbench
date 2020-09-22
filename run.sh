#!/bin/sh
docker exec -it spark-master /bin/bash /spark/bin/spark-submit /code/ex1.py --jars /myjars/postgresql-42.2.16.jar
