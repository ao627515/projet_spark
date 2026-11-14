# Projet Spark : Traitement des ventes

Application Spark/Scala (DataFrame API) qui lit `ventes.csv`, applique les étapes de nettoyage, contrôle qualité, calcul de chiffre d'affaires/marge, enrichissement temporel et segmentation, puis exporte le résultat en CSV partitionné par `year` et `month`.

## Compilation

```bash
cd projet_spark
sbt clean package
```

## Exécution

```bash
spark-submit \
  --class TraitementVentes \
  --master local[*] \
  target/scala-2.12/projet_spark_2.12-0.1.0.jar \
  ../ventes.csv \
  output_ventes
```

Note : sous WSL2, ajouter `SPARK_LOCAL_IP=127.0.0.1` devant `spark-submit` si Spark n'arrive pas à se connecter au driver.
