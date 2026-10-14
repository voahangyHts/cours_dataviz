{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "c00",
   "metadata": {},
   "source": "# S02 — Préparation à l'échelle : *silver* puis *gold*\n\n```\nbronze/yellow_trips ──nettoyage──► silver/yellow_trips ──agrégats + météo──► gold/...  ──► PostgreSQL\n```\n\n- **silver** = données propres, typées, une ligne par course\n- **gold** = tables prêtes pour les dashboards (une question métier = une table)"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c01",
   "metadata": {},
   "outputs": [],
   "source": "import sys; sys.path.append(\"/home/jovyan/work\")\nfrom pyspark.sql import functions as F\nfrom commun.spark_utils import creer_session, RAW, LAKE\n\nfrom commun.spark_utils import format_tables, vers_postgres\nspark = creer_session(\"S02\")\nFMT = format_tables(spark)\nbronze = spark.read.format(FMT).load(f\"{LAKE}/bronze/yellow_trips\")\nprint(f\"bronze : {bronze.count():,} lignes\")"
  },
  {
   "cell_type": "markdown",
   "id": "c02",
   "metadata": {},
   "source": "## 1. Règles de nettoyage\n\nChaque règle est nommée et comptée : on doit pouvoir **justifier** chaque ligne retirée."
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c03",
   "metadata": {},
   "outputs": [],
   "source": "MOIS = \"2025-01\"\nimport datetime as dt\na, m = map(int, MOIS.split(\"-\"))\ndebut, fin = f\"{MOIS}-01\", dt.date(a + (m == 12), m % 12 + 1, 1).isoformat()\n\nregles = {\n    \"montant_negatif_ou_nul\": F.col(\"total_amount\") <= 0,\n    \"passagers_0_ou_vide\":    (F.col(\"passenger_count\") == 0) | F.col(\"passenger_count\").isNull(),\n    \"distance_aberrante\":     (F.col(\"trip_distance\") <= 0) | (F.col(\"trip_distance\") > 100),\n    \"hors_du_mois\":           (F.col(\"tpep_pickup_datetime\") < F.lit(debut)) | (F.col(\"tpep_pickup_datetime\") >= F.lit(fin)),\n    \"duree_aberrante\":        (F.col(\"tpep_dropoff_datetime\") <= F.col(\"tpep_pickup_datetime\")),\n}\nbronze.select([F.sum(cond.cast(\"int\")).alias(nom) for nom, cond in regles.items()]).show(vertical=True)"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c04",
   "metadata": {},
   "outputs": [],
   "source": "from functools import reduce\na_exclure = reduce(lambda x, y: x | y, regles.values())\n\nsilver = (bronze\n          .filter(~a_exclure)\n          .withColumn(\"duree_min\", (F.unix_timestamp(\"tpep_dropoff_datetime\") - F.unix_timestamp(\"tpep_pickup_datetime\")) / 60)\n          .withColumn(\"heure_pickup\", F.date_trunc(\"hour\", \"tpep_pickup_datetime\"))\n          .drop(\"_source\", \"_ingere_le\"))\n\nsilver.write.format(FMT).mode(\"overwrite\").save(f\"{LAKE}/silver/yellow_trips\")\nn_s = spark.read.format(FMT).load(f\"{LAKE}/silver/yellow_trips\").count()\nprint(f\"silver : {n_s:,} lignes ({n_s / bronze.count():.1%} du bronze conservé)\")"
  },
  {
   "cell_type": "markdown",
   "id": "c05",
   "metadata": {},
   "source": "## 2. Tables *gold*\n\nUne table par usage. On joint les zones (noms lisibles) et la météo (par heure)."
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c06",
   "metadata": {},
   "outputs": [],
   "source": "silver = spark.read.format(FMT).load(f\"{LAKE}/silver/yellow_trips\")\nzones  = spark.read.format(FMT).load(f\"{LAKE}/bronze/zones\")\nmeteo  = spark.read.format(FMT).load(f\"{LAKE}/bronze/meteo\")\n\nz = zones.select(F.col(\"LocationID\").alias(\"PULocationID\"),\n                 F.col(\"Borough\").alias(\"arrondissement\"), F.col(\"Zone\").alias(\"zone\"))\n\n# gold 1 : activité par heure et par arrondissement, avec la météo\ngold_horaire = (silver.join(z, \"PULocationID\", \"left\")\n    .groupBy(\"heure_pickup\", \"arrondissement\")\n    .agg(F.count(\"*\").alias(\"courses\"),\n         F.round(F.sum(\"total_amount\"), 2).alias(\"revenu\"),\n         F.round(F.avg(\"total_amount\"), 2).alias(\"prix_moyen\"),\n         F.round(F.avg(\"tip_amount\"), 2).alias(\"pourboire_moyen\"),\n         F.round(F.avg(\"trip_distance\"), 2).alias(\"distance_moyenne\"))\n    .join(meteo.withColumnRenamed(\"horodatage\", \"heure_pickup\"), \"heure_pickup\", \"left\"))\n\n# gold 2 : synthèse par zone de départ (pour les cartes)\ngold_zones = (silver.join(z, \"PULocationID\", \"left\")\n    .groupBy(\"PULocationID\", \"arrondissement\", \"zone\")\n    .agg(F.count(\"*\").alias(\"courses\"),\n         F.round(F.avg(\"total_amount\"), 2).alias(\"prix_moyen\"),\n         F.round(F.avg(F.col(\"tip_amount\") / F.col(\"total_amount\")), 3).alias(\"taux_pourboire\")))\n\nfor nom, df in {\"trajets_horaires\": gold_horaire, \"zones\": gold_zones}.items():\n    df.write.format(FMT).mode(\"overwrite\").save(f\"{LAKE}/gold/{nom}\")\n    print(f\"gold/{nom} : {df.count():,} lignes\")"
  },
  {
   "cell_type": "markdown",
   "id": "c07",
   "metadata": {},
   "source": "## 3. Exposer le *gold* dans PostgreSQL\n\nSuperset, Grafana et Streamlit liront ces tables."
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c08",
   "metadata": {},
   "outputs": [],
   "source": "vers_postgres(spark.read.format(FMT).load(f\"{LAKE}/gold/trajets_horaires\"), \"gold_trajets_horaires\")\nvers_postgres(spark.read.format(FMT).load(f\"{LAKE}/gold/zones\"), \"gold_zones\")"
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Relire les tables depuis PostgreSQL\n",
    "\n",
    "Les tables *gold* sont maintenant dans la base de restitution. On les relit avec pandas, comme le feront les outils de visualisation.\n",
    "\n",
    "Pour les parcourir à la souris : http://localhost:8080 (Adminer — système *PostgreSQL*, serveur `postgres`, `cours` / `cours`, base `nyctaxi`)."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "from commun.donnees import lire\n",
    "import matplotlib.pyplot as plt\n",
    "\n",
    "# 1. Les tables présentes dans la base\n",
    "display(lire(\"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'\"))\n",
    "\n",
    "# 2. Un aperçu de chaque table gold\n",
    "horaires = lire(\"gold_trajets_horaires\")\n",
    "zones = lire(\"gold_zones\")\n",
    "print(f\"gold_trajets_horaires : {len(horaires):,} lignes · gold_zones : {len(zones):,} lignes\")\n",
    "display(horaires.head())\n",
    "\n",
    "# 3. Un premier graphique, directement depuis PostgreSQL\n",
    "par_heure = lire(\"\"\"\n",
    "    SELECT EXTRACT(HOUR FROM heure_pickup)::int AS heure, SUM(courses) AS courses\n",
    "    FROM gold_trajets_horaires\n",
    "    GROUP BY 1 ORDER BY 1\n",
    "\"\"\")\n",
    "ax = par_heure.plot.bar(x=\"heure\", y=\"courses\", legend=False, color=\"steelblue\", figsize=(10, 3.5))\n",
    "ax.set(title=\"Courses par heure de prise en charge (mois entier)\", xlabel=\"Heure\", ylabel=\"Courses\")\n",
    "plt.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c09",
   "metadata": {},
   "source": "## 4. Premiers graphiques exploratoires (sur le *gold*, donc petits)"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c10",
   "metadata": {},
   "outputs": [],
   "source": "import matplotlib.pyplot as plt\nh = (spark.read.format(FMT).load(f\"{LAKE}/gold/trajets_horaires\")\n          .groupBy(F.hour(\"heure_pickup\").alias(\"heure\"))\n          .agg(F.sum(\"courses\").alias(\"courses\"))\n          .orderBy(\"heure\").toPandas())\nfig, ax = plt.subplots(figsize=(9, 4))\nax.plot(h[\"heure\"], h[\"courses\"], marker=\"o\")\nax.set_xlabel(\"Heure\"); ax.set_ylabel(\"Courses (mois entier)\")\nax.set_title(f\"Pic de demande à {int(h.loc[h['courses'].idxmax(), 'heure'])} h\")\nplt.show()"
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c11",
   "metadata": {},
   "outputs": [],
   "source": "p = (spark.read.format(FMT).load(f\"{LAKE}/gold/trajets_horaires\")\n          .withColumn(\"pluie\", F.when(F.col(\"precipitation_mm\") > 0, \"pluie\").otherwise(\"sec\"))\n          .groupBy(\"pluie\").agg(F.avg(\"courses\").alias(\"courses_par_heure_et_arrondissement\"))\n          .toPandas())\np"
  },
  {
   "cell_type": "markdown",
   "id": "c12",
   "metadata": {},
   "source": "## C'est réussi quand…\n- `silver` et `gold` sont visibles dans la console RustFS ;\n- `SELECT * FROM gold_trajets_horaires LIMIT 10;` répond dans PostgreSQL ;\n- vous savez justifier chaque règle de nettoyage et le nombre de lignes retirées.\n\n**Exercice :** ajoutez une table *gold* « aéroports » (zones JFK, LaGuardia, Newark) : courses, prix moyen, durée moyenne par heure."
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c13",
   "metadata": {},
   "outputs": [],
   "source": "spark.stop()"
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
