#!/bin/sh
docker exec -it spark-master /bin/bash /spark/bin/spark-submit $@ --jars /myjars/postgresql-42.2.16.jar
