@echo off
REM =========================
REM CONFIG
REM =========================
set "projectName=Exercise_1"
set "jarFile=Exercise_1-1.0-SNAPSHOT.jar"
set "localTextFolder=/home/elias/Documents/FHV/Master - Informatik /Batch-Processing Systeme/Exercise_1/all"
set "localJarFolder=target"
set "mapReduceClass=org.example.MaxTemperatureDriver"
set "container=hadoop-namenode-1"
set "hadoopBin=/usr/local/hadoop/bin/hadoop"

echo =========================
echo Hadoop WordCount Automatisierung - Mehrere Dateien
echo =========================


REM =========================
REM Step 1: JAR kopieren
REM =========================
echo Kopiere JAR ins Docker-Cluster...
docker cp "%localJarFolder%\%jarFile%" %container%:/home/hadoop/%jarFile%


REM =========================
REM Step 2: NUR .txt Dateien kopieren
REM =========================
echo Kopiere alle .txt Dateien aus %localTextFolder%...

for %%f in ("%localTextFolder%\*.txt") do (
    echo Kopiere %%~nxf ...
    docker cp "%%f" %container%:/home/hadoop/%%~nxf
)


