import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}

object TraitementVentes {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: TraitementVentes <chemin_csv_entree> <repertoire_sortie>")
      sys.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession.builder()
      .appName("TraitementVentes")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val dfBrut = spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .csv(inputPath)

      val dfFinal = dfBrut
        .transform(nettoyerEtNormaliser)
        .transform(convertirTypes)
        .transform(controlerQualite)
        .transform(calculerChiffreAffaires)
        .transform(calculerRentabilite)
        .transform(enrichirTemporellement)
        .transform(segmenterVentes)

      dfFinal.printSchema()
      dfFinal.show(10, truncate = false)

      dfFinal.write
        .mode("overwrite")
        .option("header", "true")
        .partitionBy("year", "month")
        .csv(outputPath)

      println(s"Export terminé dans : $outputPath")
    } finally {
      spark.stop()
    }
  }

  // T1 : nettoyage des espaces et uniformisation de la casse
  def nettoyerEtNormaliser(df: DataFrame): DataFrame = {
    val dfTrimmed = df.columns.foldLeft(df) { (acc, colName) =>
      acc.withColumn(colName, trim(col(colName)))
    }

    dfTrimmed
      .withColumn("product_code", upper(col("product_code")))
      .withColumn("payment_method", upper(col("payment_method")))
      .withColumn("category", initcap(col("category")))
      .withColumn("city", initcap(col("city")))
  }

  // T2 : conversion des types
  def convertirTypes(df: DataFrame): DataFrame = {
    df
      .withColumn("transaction_date", to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("quantity", col("quantity").cast(IntegerType))
      .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
      .withColumn("discount_rate", col("discount_rate").cast(DecimalType(5, 4)))
      .withColumn("unit_cost", col("unit_cost").cast(DecimalType(10, 2)))
  }

  // T3 : filtrage des lignes invalides
  def controlerQualite(df: DataFrame): DataFrame = {
    df.filter(
      col("transaction_id").isNotNull && col("transaction_id") =!= "" &&
      col("customer_id").isNotNull && col("customer_id") =!= "" &&
      col("transaction_date").isNotNull &&
      col("quantity").isNotNull && col("quantity") > 0 &&
      col("unit_price").isNotNull && col("unit_price") >= 0 &&
      col("unit_cost").isNotNull && col("unit_cost") >= 0 &&
      col("discount_rate").isNotNull && col("discount_rate") >= 0 && col("discount_rate") <= 1
    )
  }

  // T4 : calcul du chiffre d'affaires brut et net HT
  def calculerChiffreAffaires(df: DataFrame): DataFrame = {
    df
      .withColumn("gross_amount", round(col("quantity") * col("unit_price"), 2).cast(DecimalType(14, 2)))
      .withColumn("net_amount_ht", round(col("gross_amount") * (lit(1) - col("discount_rate")), 2).cast(DecimalType(14, 2)))
  }

  // T5 : calcul du coût total, de la marge et du taux de marge (0 si CA nul)
  def calculerRentabilite(df: DataFrame): DataFrame = {
    df
      .withColumn("total_cost", round(col("quantity") * col("unit_cost"), 2).cast(DecimalType(14, 2)))
      .withColumn("margin", round(col("net_amount_ht") - col("total_cost"), 2).cast(DecimalType(14, 2)))
      .withColumn(
        "margin_rate",
        when(col("net_amount_ht") === 0, lit(0.0).cast(DecimalType(10, 4)))
          .otherwise(round(col("margin") / col("net_amount_ht"), 4).cast(DecimalType(10, 4)))
      )
  }

  // T6 : extraction de l'année, du mois et du jour de la semaine
  def enrichirTemporellement(df: DataFrame): DataFrame = {
    df
      .withColumn("year", year(col("transaction_date")))
      .withColumn("month", month(col("transaction_date")))
      .withColumn("day_of_week", dayofweek(col("transaction_date")))
  }

  // T7 : segmentation des paniers et statut de marge
  def segmenterVentes(df: DataFrame): DataFrame = {
    df
      .withColumn(
        "sale_segment",
        when(col("net_amount_ht") >= 1000, "GROS_PANIER")
          .when(col("net_amount_ht") >= 300, "PANIER_MOYEN")
          .otherwise("PETIT_PANIER")
      )
      .withColumn(
        "margin_status",
        when(col("margin") > 0, "POSITIVE")
          .when(col("margin") < 0, "NEGATIVE")
          .otherwise("NULLE")
      )
  }
}
