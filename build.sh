#!/bin/bash
mvn clean package -Pdist,native -DskipTests -Dtar
