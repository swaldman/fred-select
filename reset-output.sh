#!/bin/bash

mkdir -p output/dbms

rm -rf output/dbms/*

cd output/dbms
dolt sql --query "create database fredselect"





