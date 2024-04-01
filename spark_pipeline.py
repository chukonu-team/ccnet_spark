{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "28e7aa31-b893-45a3-aa85-450f056caf1f",
   "metadata": {},
   "source": [
    "# 1. 导入依赖"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "1ad5b292-4eb3-4cc0-8837-2a8cf03fd7ff",
   "metadata": {},
   "outputs": [],
   "source": [
    "from ccnet_spark import open_read, parse_warc_file,compute_hashes,NaiveHashSet\n",
    "from pathlib import Path\n",
    "import numpy as np\n",
    "import time\n",
    "import pandas as pd\n",
    "\n",
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.types import ArrayType, StringType,IntegerType,StructType, StructField\n",
    "from pyspark.sql.functions import udf, explode\n",
    "from pyspark.sql.functions import sum as spark_sum\n",
    "\n",
    "# 初始化 SparkSession\n",
    "spark = SparkSession.builder.appName(\"CCNETSpark\")  \\\n",
    "                    .config(\"spark.executor.memory\", \"32g\") \\\n",
    "                    .config(\"spark.driver.memory\", \"32g\") \\\n",
    "                    .config(\"spark.driver.maxResultSize\", \"5g\") \\\n",
    "                    .getOrCreate()\n",
    "# spark"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "5f99a057-a8db-4d94-b772-988434ac58cc",
   "metadata": {},
   "source": [
    "# 2. 读取文件数据，处理成pandas DataFrame"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "0493e968-ee52-488d-bab9-5c54cff63dd6",
   "metadata": {},
   "source": [
    "## 2.1 获取cache文件路径"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "ceb17ca6-b75b-4307-8083-d5e48e5cd768",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "../cache_data/2019-09/CC-MAIN-20190215183319-20190215205319-00003.warc.wet.gz\n"
     ]
    }
   ],
   "source": [
    "cache_data=\"../cache_data/2019-09/\"\n",
    "def getWETURL(segment: int):\n",
    "    cache_file_prefix = \"CC-MAIN-20190215183319-20190215205319-\"\n",
    "    cache_file_sufix = \".warc.wet.gz\"\n",
    "    segment_str = str(segment).zfill(5)  # Pad with leading zeros\n",
    "    return cache_data+cache_file_prefix + segment_str + cache_file_sufix\n",
    "url = getWETURL(3)\n",
    "print(url)  # Output: CC-MAIN-20190215183319-20190215205319-00003.warc.wet.gz"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "d0530fda-e712-49b4-bab1-d9c76972b465",
   "metadata": {},
   "source": [
    "## 2.2 处理文件，存入pandas DataFrame"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "8ed3705e-128e-4f62-b4b8-f09dce29e89e",
   "metadata": {},
   "outputs": [],
   "source": [
    "def getpdf(segments):\n",
    "    pandas_dfs=[]\n",
    "    for seg in segments:\n",
    "        file_path=Path(getWETURL(seg))\n",
    "        file=open_read(file_path)\n",
    "        s=time.time()\n",
    "        pandas_df = parse_warc_file(file, 30)\n",
    "        random_save_n=5000\n",
    "        pandas_df = pandas_df.sample(n=random_save_n, random_state=1)\n",
    "        pandas_dfs.append(pandas_df)\n",
    "        e=time.time()\n",
    "        print(f\"parse one seg to pd_df consume:{e-s} s\")\n",
    "    return pandas_dfs"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "79e4a211-e298-4d0e-b6ab-168ebdb8918a",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2024-03-29 15:18 INFO 176792:ccnet_spark.load - Opening ../cache_data/2019-09/CC-MAIN-20190215183319-20190215205319-00000.warc.wet.gz with mode 'rt'\n",
      "2024-03-29 15:18 INFO 176792:root - Created DataFrame with 43855 documents\n",
      "2024-03-29 15:18 INFO 176792:ccnet_spark.load - Opening ../cache_data/2019-09/CC-MAIN-20190215183319-20190215205319-00001.warc.wet.gz with mode 'rt'\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "parse one seg to pd_df consume:6.239351511001587 s\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2024-03-29 15:18 INFO 176792:root - Created DataFrame with 43123 documents\n",
      "2024-03-29 15:18 INFO 176792:ccnet_spark.load - Opening ../cache_data/2019-09/CC-MAIN-20190215183319-20190215205319-00002.warc.wet.gz with mode 'rt'\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "parse one seg to pd_df consume:6.056180000305176 s\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2024-03-29 15:18 INFO 176792:root - Created DataFrame with 44017 documents\n",
      "2024-03-29 15:18 INFO 176792:ccnet_spark.load - Opening ../cache_data/2019-09/CC-MAIN-20190215183319-20190215205319-00003.warc.wet.gz with mode 'rt'\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "parse one seg to pd_df consume:6.148106813430786 s\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2024-03-29 15:18 INFO 176792:root - Created DataFrame with 43733 documents\n",
      "2024-03-29 15:18 INFO 176792:ccnet_spark.load - Opening ../cache_data/2019-09/CC-MAIN-20190215183319-20190215205319-00004.warc.wet.gz with mode 'rt'\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "parse one seg to pd_df consume:6.214878797531128 s\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2024-03-29 15:18 INFO 176792:root - Created DataFrame with 43523 documents\n",
      "2024-03-29 15:18 INFO 176792:ccnet_spark.load - Opening ../cache_data/2019-09/CC-MAIN-20190215183319-20190215205319-00005.warc.wet.gz with mode 'rt'\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "parse one seg to pd_df consume:6.191338062286377 s\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2024-03-29 15:18 INFO 176792:root - Created DataFrame with 43795 documents\n",
      "2024-03-29 15:18 INFO 176792:ccnet_spark.load - Opening ../cache_data/2019-09/CC-MAIN-20190215183319-20190215205319-00006.warc.wet.gz with mode 'rt'\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "parse one seg to pd_df consume:6.246756553649902 s\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2024-03-29 15:18 INFO 176792:root - Created DataFrame with 43418 documents\n",
      "2024-03-29 15:18 INFO 176792:ccnet_spark.load - Opening ../cache_data/2019-09/CC-MAIN-20190215183319-20190215205319-00007.warc.wet.gz with mode 'rt'\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "parse one seg to pd_df consume:6.241579532623291 s\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2024-03-29 15:18 INFO 176792:root - Created DataFrame with 43693 documents\n",
      "2024-03-29 15:18 INFO 176792:ccnet_spark.load - Opening ../cache_data/2019-09/CC-MAIN-20190215183319-20190215205319-00008.warc.wet.gz with mode 'rt'\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "parse one seg to pd_df consume:6.290111780166626 s\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2024-03-29 15:19 INFO 176792:root - Created DataFrame with 43762 documents\n",
      "2024-03-29 15:19 INFO 176792:ccnet_spark.load - Opening ../cache_data/2019-09/CC-MAIN-20190215183319-20190215205319-00009.warc.wet.gz with mode 'rt'\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "parse one seg to pd_df consume:6.28784704208374 s\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2024-03-29 15:19 INFO 176792:root - Created DataFrame with 43813 documents\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "parse one seg to pd_df consume:6.349085807800293 s\n"
     ]
    }
   ],
   "source": [
    "segments=[i for i in range(10)]\n",
    "pdfs=getpdf(segments)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "ef42772d-a0cd-44e7-b197-312deb3ef263",
   "metadata": {},
   "source": [
    "# 3. 将pandas DataFrame 转换成spark DataFrame"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "90a824c2-5d7c-46cf-8965-e34728d83215",
   "metadata": {},
   "outputs": [],
   "source": [
    "def pdf2sdf(dfs):\n",
    "    merged_df=None\n",
    "    for df in dfs:\n",
    "        spark_df = spark.createDataFrame(df)\n",
    "        if(merged_df):\n",
    "            merged_df = merged_df.unionAll(spark_df) # Merge three DataFrames\n",
    "        else:\n",
    "            merged_df=spark_df\n",
    "    return merged_df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "12719579-006a-4e30-bdad-a11cd62340b9",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/03/29 15:19:35 WARN TaskSetManager: Stage 0 contains a task of very large size (1963 KiB). The maximum recommended task size is 1000 KiB.\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+--------------------+--------------------+------+------+--------------------+-----------------------------+-----------------------------+\n",
      "|                 url|       date_download|              digest|length|nlines|       source_domain|                        title|                  raw_content|\n",
      "+--------------------+--------------------+--------------------+------+------+--------------------+-----------------------------+-----------------------------+\n",
      "|https://www.telel...|2019-02-15T19:35:48Z|sha1:VZYTYZZ7EH6E...|  4758|   111|    www.telelynx.com|         sean, Author at T...|         English\\tEnglish\\...|\n",
      "|http://www.ma.hu/...|2019-02-15T19:26:14Z|sha1:FA3DLWLJZKFI...|  4180|    70|           www.ma.hu|         Kiégett a Hűvösvö...|         hirdetés\\nma.hu n...|\n",
      "|http://angagement...|2019-02-15T18:57:03Z|sha1:EMAN4TLHXTXM...|  1326|    65|   angagement.com.ua|         Ballroom and lati...|         → по-русски\\nCost...|\n",
      "|http://resistther...|2019-02-15T19:55:31Z|sha1:UWDJJTE42LC7...|   912|    23|resisttherock.uco...|         Comments or Sugge...|         Unwanted Resistan...|\n",
      "|http://klimadiagr...|2019-02-15T19:05:24Z|sha1:IZ4YL65XZD2U...|  1918|    51|   klimadiagramme.de|         Das Klima in Karl...|         Das Klima in Karl...|\n",
      "|https://www.schna...|2019-02-15T19:57:31Z|sha1:5T6VBCCHCRGJ...|  2892|    65|    www.schnauzi.com|         Las palabras con ...|         Schnauzi.com\\nMen...|\n",
      "|http://chanjo.co/...|2019-02-15T18:48:35Z|sha1:5X5OSGE6R2ST...|  1523|   107|           chanjo.co|         Chicken Fryer Com...|         Skip to content\\n...|\n",
      "|https://www.jacks...|2019-02-15T19:57:54Z|sha1:SP363QQJ6L42...|  1538|    45|www.jackscomplian...|         Regulation E Arch...|         Skip to content\\n...|\n",
      "|https://gallery.t...|2019-02-15T20:09:39Z|sha1:AFKTGBUCHWUY...|   258|    22|    gallery.teele.eu|         Flowers - Flora -...|         Teele's Gallery\\n...|\n",
      "|http://gyerek-dal...|2019-02-15T19:53:11Z|sha1:7BJKAVO2QYJM...|  1580|    54|     gyerek-dalok.hu|         Gyerekdalok - Az ...|         ﻿\\nGyerek-dalok\\n...|\n",
      "|http://www.limone...|2019-02-15T19:38:54Z|sha1:LOD5RKH3ZOKE...|  9380|    94|www.limoneturismo.it|              Limone Piemonte|         MENU\\tLA VILLE\\tS...|\n",
      "|http://blog.winte...|2019-02-15T19:52:59Z|sha1:Q2XXARRG5JVP...|  2921|    84|blog.winterwomen.com|         Be The Judge: Bes...|         WinterWomen.com A...|\n",
      "|https://www.accen...|2019-02-15T18:40:01Z|sha1:NND3WDWKUNRE...|  6620|   203|   www.accenture.com|         Bhaskar Ghosh, Gr...|         Skip to main cont...|\n",
      "|https://www.xoxox...|2019-02-15T20:08:34Z|sha1:FGPO6OKINEYZ...|  3202|   181|       www.xoxoxo.pl|         Ubrania - Dla Nie...|         KOMFORT\\nNAJWYŻSZ...|\n",
      "|http://lirica-par...|2019-02-15T18:59:23Z|sha1:4LLNKF4XCF7H...|  5547|   265|lirica-parma.blog...|         Parma Lirica - Pa...|         Repubblica.it Par...|\n",
      "|https://www.tuv-s...|2019-02-15T19:13:23Z|sha1:ASLW62SWCRYJ...|  4134|   188|www.tuv-sud-ameri...|         About us | TÜV SÜ...|         Toggle navigation...|\n",
      "|https://gym-train...|2019-02-15T19:02:07Z|sha1:NHRK7KEUCSBX...|  4941|   113|    gym-training.com|         Предтренировочный...|         Сервис сравнения ...|\n",
      "|http://www.kdxb.n...|2019-02-15T19:15:02Z|sha1:YSRJWXE3M77Y...| 13300|   126|        www.kdxb.net|新疆风彩35选7开奖:3366小游...|新疆风彩35选7开奖:3366小游...|\n",
      "|http://alabamaliv...|2019-02-15T20:04:13Z|sha1:KNCVRSF5BESR...|  6889|    92|  alabamaliving.coop|         Alabama Outdoors ...|         Navigate / search...|\n",
      "|https://www.wingh...|2019-02-15T19:29:34Z|sha1:7S5MW76QDGAP...|  1117|    55|www.winghamwoolwo...|         Ashford Kiwi Slid...|         Products\\nWingham...|\n",
      "+--------------------+--------------------+--------------------+------+------+--------------------+-----------------------------+-----------------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# 将 pandas DataFrame 转换为 Spark DataFrame\n",
    "spark_df = pdf2sdf(pdfs)\n",
    "spark_df.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c9793de8-008f-4aec-9dc7-0bbad27feff3",
   "metadata": {},
   "source": [
    "# 4. hash计算"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "16dea9e5-b3a8-4b50-b40c-147fd7bbecbd",
   "metadata": {},
   "source": [
    "## 4.1 定义UDF,将doc 分割成paragraph "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "44be1b10-248f-470d-874b-3b3f4962a3b1",
   "metadata": {},
   "outputs": [],
   "source": [
    "# 定义一个函数，用于分割文本\n",
    "def split_raw_content(content):\n",
    "    lines = content.split('\\n')\n",
    "    line_ids = range(1, len(lines) + 1)  # 生成行号\n",
    "    return list(zip(line_ids, lines))\n",
    "\n",
    "# 注册为UDF\n",
    "split_udf = udf(split_raw_content, ArrayType(StructType([\n",
    "    StructField(\"raw_line_id\", IntegerType(), False),\n",
    "    StructField(\"raw_line\", StringType(), False)\n",
    "])))\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "ac16adaa-b1c1-4e99-8bdf-93312621e5f4",
   "metadata": {},
   "source": [
    "## 4.2 udf 处理添加新字段"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "2c505bf4-b9f4-4e1f-ad51-ad56939b28a5",
   "metadata": {},
   "outputs": [],
   "source": [
    "# 假设spark_df是您的DataFrame\n",
    "# 使用UDF对raw_content字段进行处理\n",
    "split_result = spark_df.withColumn(\"split_content\", split_udf(spark_df[\"raw_content\"]))"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "fd212cd6-25ca-434a-b633-01ca1e7b7ecd",
   "metadata": {},
   "source": [
    "## 4.3 将新字段展开获取paragraph级别row"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "70f305dc-6e04-40a9-8138-d3049dc4c070",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/03/29 15:19:44 WARN TaskSetManager: Stage 1 contains a task of very large size (1963 KiB). The maximum recommended task size is 1000 KiB.\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+------+------+--------------------+-----------+------------------------+\n",
      "|                 url|length|nlines|               title|raw_line_id|                raw_line|\n",
      "+--------------------+------+------+--------------------+-----------+------------------------+\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          1|    English\\tEnglish\\ten|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          2|繁體中文\\tChinese (Tr...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          3|                    Home|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          4|                Products|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          5|              Digital TV|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          6|    DVB / ISDB-T TV S...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          7|            AddTV System|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          8|                     CAS|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          9|          Card based CAS|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         10|            Cardless CAS|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         11|        Digital Heandend|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         12|     ASI to IP converter|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         13|                 Encoder|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         14|    IP Mux / Scramble...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         15|                     IRD|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         16|    Mux / Scrambler /...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         17|              Transcoder|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         18|                     EPG|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         19|         Push VOD System|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         20|    SMS / Billing / M...|\n",
      "+--------------------+------+------+--------------------+-----------+------------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "# Explode the split_content column and select the desired columns\n",
    "exploded_df = split_result.select(\"url\",\"length\",\"nlines\",\"title\", explode(split_result.split_content).alias(\"exploded_content\"))\n",
    "\n",
    "# Split the exploded_content struct into separate columns\n",
    "exploded_df = exploded_df.withColumn(\"raw_line_id\", exploded_df.exploded_content.raw_line_id)\n",
    "exploded_df = exploded_df.withColumn(\"raw_line\", exploded_df.exploded_content.raw_line)\n",
    "\n",
    "# Drop the exploded_content column if needed\n",
    "exploded_df = exploded_df.drop(\"exploded_content\")\n",
    "\n",
    "# Show the resulting dataframe\n",
    "exploded_df.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "5389ac85-4602-4be7-bbf5-53c809e85598",
   "metadata": {},
   "source": [
    "## 4.4 查看第[100，100+10)行"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "0a47702d-9b57-4a89-8069-edbfa484cb87",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/03/29 15:19:51 WARN TaskSetManager: Stage 2 contains a task of very large size (1963 KiB). The maximum recommended task size is 1000 KiB.\n",
      "[Stage 2:====================================================>  (152 + 8) / 160]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+------+------+--------------------+-----------+--------------------+\n",
      "|                 url|length|nlines|               title|raw_line_id|            raw_line|\n",
      "+--------------------+------+------+--------------------+-----------+--------------------+\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|        100|              Search|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|        101|         Latest News|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|        102|IBC2019, RAI Amst...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|        103|CABSAT Dubai 2019...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|        104|UHD/HD DVB-T2 STB...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|        105|            Facebook|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|        106|© 2018 Copyright ...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|        107|   - made by bouncin|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|        108|            Facebook|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|        109|             Youtube|\n",
      "+--------------------+------+------+--------------------+-----------+--------------------+\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "selected_rows = exploded_df.offset(99).limit(10)\n",
    "# 显示结果\n",
    "selected_rows.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c7a82cec-45d4-4121-b570-c38091c425fd",
   "metadata": {},
   "source": [
    "## 4.5 添加hash 列"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "31d74e40-0f12-4556-bac2-0bd91c94cada",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/03/29 15:20:01 WARN TaskSetManager: Stage 5 contains a task of very large size (1963 KiB). The maximum recommended task size is 1000 KiB.\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+------+------+--------------------+-----------+------------------------+--------------------+\n",
      "|                 url|length|nlines|               title|raw_line_id|                raw_line|          hash_value|\n",
      "+--------------------+------+------+--------------------+-----------+------------------------+--------------------+\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          1|    English\\tEnglish\\ten|[A7 E1 3C F2 70 F...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          2|繁體中文\\tChinese (Tr...|[3E DB 9E EF B5 2...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          3|                    Home|[E8 32 49 BD 3B A...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          4|                Products|[FB DC 4F 23 F9 3...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          5|              Digital TV|[59 44 27 AB 00 F...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          6|    DVB / ISDB-T TV S...|[EC 45 EC 5C 21 F...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          7|            AddTV System|[92 68 91 9C 6F A...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          8|                     CAS|[A7 0E 06 B7 0B E...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|          9|          Card based CAS|[08 DB 2E 2A FD 2...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         10|            Cardless CAS|[5A 55 8C E4 15 1...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         11|        Digital Heandend|[00 46 FB A7 E8 F...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         12|     ASI to IP converter|[62 DF 24 46 46 F...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         13|                 Encoder|[50 F6 3D 56 BC 4...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         14|    IP Mux / Scramble...|[13 B9 D0 37 B3 A...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         15|                     IRD|[D5 6C 79 A8 D9 A...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         16|    Mux / Scrambler /...|[53 F8 42 A6 C4 F...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         17|              Transcoder|[3F D0 94 D6 AE 9...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         18|                     EPG|[10 B3 92 3B 77 7...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         19|         Push VOD System|[01 96 70 05 BD B...|\n",
      "|https://www.telel...|  4758|   111|sean, Author at T...|         20|    SMS / Billing / M...|[CB 7D 53 8B 4B 7...|\n",
      "+--------------------+------+------+--------------------+-----------+------------------------+--------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "import hashlib\n",
    "from pyspark.sql.functions import udf\n",
    "from pyspark.sql.types import BinaryType\n",
    "from ccnet_spark import normalize_for_dedup\n",
    "HASH_SIZE = 10  # Define the desired size of the hash\n",
    "\n",
    "@udf(returnType=BinaryType())\n",
    "def compute_hashes(line):\n",
    "    if not line:\n",
    "        return None\n",
    "    normalized_line = normalize_for_dedup(line)  # Assuming normalize_for_dedup is defined\n",
    "    line_hash = hashlib.sha1(bytes(normalized_line, encoding=\"utf-8\")).digest()[:HASH_SIZE]\n",
    "    return line_hash\n",
    "\n",
    "# Assuming you have a dataframe named 'df' with a 'raw_line' column\n",
    "hash_df = exploded_df.withColumn(\"hash_value\", compute_hashes(exploded_df.raw_line))\n",
    "\n",
    "# Show the resulting dataframe\n",
    "hash_df.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "aec0d5a4-2cd6-4925-8f53-690b0213bf9c",
   "metadata": {},
   "source": [
    "## 4.5根据 hash 去重"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "620aaf6b-9774-4a6f-98a2-962db1f9a9db",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/03/29 15:20:04 WARN TaskSetManager: Stage 6 contains a task of very large size (1963 KiB). The maximum recommended task size is 1000 KiB.\n",
      "[Stage 6:======================================================>(158 + 2) / 160]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+------+------+----------------------------------+-----------+-------------------------------------+--------------------+\n",
      "|                 url|length|nlines|                             title|raw_line_id|                             raw_line|          hash_value|\n",
      "+--------------------+------+------+----------------------------------+-----------+-------------------------------------+--------------------+\n",
      "|http://opengov.se...|  9977|   259|    ﻿ 2015년 장애아동가족지원사...|         80|                         시민정보나눔|[00 00 10 A7 EA B...|\n",
      "|https://stephencw...| 35076|   273|              Wisdom from The L...|         68|                 Gandalf as Olorin...|[00 00 57 0A 2B 4...|\n",
      "|https://www.tripa...| 15373|   534|              Barrie: i miglior...|         15|                 Cose da fare: Barrie|[00 00 D2 BE 6A 7...|\n",
      "|http://ustvarjaln...| 34658|  1709|              ustvarjalnica pri...|        285|                               Zakaj?|[00 01 09 BD 36 3...|\n",
      "|http://sodesk.com...|  5513|   163|              Free Wallpapers: ...|        157|                       PSP Wallpapers|[00 01 22 AC 29 8...|\n",
      "|https://www.anima...| 96275|  6798|              Acquista online p...|       3247|                          (2)Jinberry|[00 01 25 71 09 D...|\n",
      "|http://www.italia...|  8653|   271|              La Gioia Dei Sens...|        206|                 Attenzione: stai ...|[00 01 33 5F 57 E...|\n",
      "|https://www.gayin...|  1120|    45|              Gay Los Angeles -...|          7|                 West Hollywood Ga...|[00 01 48 90 D3 E...|\n",
      "|https://akaboshi....| 10313|   361|期日前投票に行ってきました : 赤...|        354|駅前での期日前投票、いいですね！　...|[00 01 5F 8F 23 3...|\n",
      "|http://cafe.comeb...|  7999|   187|              The Black Flag Ca...|        125|                 Another friend re...|[00 01 93 B5 7F D...|\n",
      "|https://www.globa...| 10845|   453|              Adidas Ultimate 3...|        359|                 HTML, non-related...|[00 01 E8 34 D2 3...|\n",
      "|http://arles-agen...|  3661|    75|              Thomas Jocher - L...|         63|                 Heure : du mercre...|[00 01 EF 8D 4F 9...|\n",
      "|http://unisa.br/A...|  1452|    79|              :: Unisa - Univer...|         59|                      2ª Licenciatura|[00 01 FB E9 2C 2...|\n",
      "|http://www.mein-d...| 19839|  1708|              Y.A.S - 2018 Null...|       1384|                                  Hut|[00 02 0D 35 66 A...|\n",
      "|https://topleader...|  4125|   104|              Coaching empreend...|         11|                          Andrea Deis|[00 02 10 B7 E9 4...|\n",
      "|https://www.hojna...|  9638|   268|              Zdzisława Hojnack...|        264|                 (miesięczne prakt...|[00 02 28 25 82 8...|\n",
      "|http://allaitemen...| 17926|   249|              congé pathologiqu...|        105|                 Selon ce que vous...|[00 02 29 17 D8 4...|\n",
      "|https://seguridad...| 13883|   254|              Acronis Ransomwar...|         42|                 Verificación de F...|[00 02 C7 E6 F5 7...|\n",
      "|https://www.kosme...|  9665|   123|              كيفية حساب قدرة ك...|         82|                 كيفية حساب كفاءة ...|[00 02 EB 4F F3 C...|\n",
      "|http://www.future...| 15950|   414|     EBS의 정치뉴스, 문제 있다....|        358|      [브랜드평판] 걸그룹 브랜드 2...|[00 03 9D A4 8E B...|\n",
      "+--------------------+------+------+----------------------------------+-----------+-------------------------------------+--------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "deduplicated_df = hash_df.dropDuplicates(['hash_value'])\n",
    "deduplicated_df.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "42e869df-9d84-4f27-9285-d0e1f241f2b4",
   "metadata": {},
   "source": [
    "## 4.6 更新字符长度"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "6a36afac-46a1-481e-8b89-6b6c4480b775",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import length\n",
    "\n",
    "# 使用 length() 函数计算字符数量，并将结果存储在新列 new_length 中\n",
    "deduplicated_df = deduplicated_df.withColumn('new_length', length(deduplicated_df['raw_line']))\n",
    "\n",
    "# 更新 length 字段的值为 new_length 字段的值\n",
    "deduplicated_df = deduplicated_df.withColumn('length', deduplicated_df['new_length']).drop('new_length')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "3cf587d9-5efb-4433-9911-0da2066632fd",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/03/29 15:20:48 WARN TaskSetManager: Stage 9 contains a task of very large size (1963 KiB). The maximum recommended task size is 1000 KiB.\n",
      "[Stage 9:======================================================>(158 + 2) / 160]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+------+------+----------------------------------+-----------+-------------------------------------+--------------------+\n",
      "|                 url|length|nlines|                             title|raw_line_id|                             raw_line|          hash_value|\n",
      "+--------------------+------+------+----------------------------------+-----------+-------------------------------------+--------------------+\n",
      "|http://opengov.se...|     6|   259|    ﻿ 2015년 장애아동가족지원사...|         80|                         시민정보나눔|[00 00 10 A7 EA B...|\n",
      "|https://stephencw...|    37|   273|              Wisdom from The L...|         68|                 Gandalf as Olorin...|[00 00 57 0A 2B 4...|\n",
      "|https://www.tripa...|    20|   534|              Barrie: i miglior...|         15|                 Cose da fare: Barrie|[00 00 D2 BE 6A 7...|\n",
      "|http://ustvarjaln...|     6|  1709|              ustvarjalnica pri...|        285|                               Zakaj?|[00 01 09 BD 36 3...|\n",
      "|http://sodesk.com...|    14|   163|              Free Wallpapers: ...|        157|                       PSP Wallpapers|[00 01 22 AC 29 8...|\n",
      "|https://www.anima...|    11|  6798|              Acquista online p...|       3247|                          (2)Jinberry|[00 01 25 71 09 D...|\n",
      "|http://www.italia...|   266|   271|              La Gioia Dei Sens...|        206|                 Attenzione: stai ...|[00 01 33 5F 57 E...|\n",
      "|https://www.gayin...|    33|    45|              Gay Los Angeles -...|          7|                 West Hollywood Ga...|[00 01 48 90 D3 E...|\n",
      "|https://akaboshi....|    44|   361|期日前投票に行ってきました : 赤...|        354|駅前での期日前投票、いいですね！　...|[00 01 5F 8F 23 3...|\n",
      "|http://cafe.comeb...|    81|   187|              The Black Flag Ca...|        125|                 Another friend re...|[00 01 93 B5 7F D...|\n",
      "|https://www.globa...|   109|   453|              Adidas Ultimate 3...|        359|                 HTML, non-related...|[00 01 E8 34 D2 3...|\n",
      "|http://arles-agen...|    61|    75|              Thomas Jocher - L...|         63|                 Heure : du mercre...|[00 01 EF 8D 4F 9...|\n",
      "|http://unisa.br/A...|    15|    79|              :: Unisa - Univer...|         59|                      2ª Licenciatura|[00 01 FB E9 2C 2...|\n",
      "|http://www.mein-d...|     3|  1708|              Y.A.S - 2018 Null...|       1384|                                  Hut|[00 02 0D 35 66 A...|\n",
      "|https://topleader...|    11|   104|              Coaching empreend...|         11|                          Andrea Deis|[00 02 10 B7 E9 4...|\n",
      "|https://www.hojna...|    21|   268|              Zdzisława Hojnack...|        264|                 (miesięczne prakt...|[00 02 28 25 82 8...|\n",
      "|http://allaitemen...|    40|   249|              congé pathologiqu...|        105|                 Selon ce que vous...|[00 02 29 17 D8 4...|\n",
      "|https://seguridad...|    65|   254|              Acronis Ransomwar...|         42|                 Verificación de F...|[00 02 C7 E6 F5 7...|\n",
      "|https://www.kosme...|    89|   123|              كيفية حساب قدرة ك...|         82|                 كيفية حساب كفاءة ...|[00 02 EB 4F F3 C...|\n",
      "|http://www.future...|    62|   414|     EBS의 정치뉴스, 문제 있다....|        358|      [브랜드평판] 걸그룹 브랜드 2...|[00 03 9D A4 8E B...|\n",
      "+--------------------+------+------+----------------------------------+-----------+-------------------------------------+--------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "deduplicated_df.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "db59366d-6d0e-4ab8-937b-1dbcb39a1304",
   "metadata": {},
   "source": [
    "## 4.6 计算留存比例"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "id": "f1295a70-8dce-40c1-aad2-103efb493626",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/03/29 15:22:05 WARN TaskSetManager: Stage 30 contains a task of very large size (1963 KiB). The maximum recommended task size is 1000 KiB.\n",
      "24/03/29 15:22:06 WARN TaskSetManager: Stage 33 contains a task of very large size (1963 KiB). The maximum recommended task size is 1000 KiB.\n",
      "[Stage 33:====================================================> (155 + 5) / 160]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "origin chars:377.703559M,remain_chars:223.93559M \n",
      "         keep chars:59.289 %\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "origin_chars = spark_df.agg(spark_sum(\"length\")).collect()[0][0]\n",
    "remain_chars = deduplicated_df.agg(spark_sum(\"length\")).collect()[0][0]\n",
    "print(f\"origin chars:{origin_chars/1000/1000}M,remain_chars:{remain_chars/1000/1000}M \\n \\\n",
    "        keep chars:{round(remain_chars/origin_chars*100,3)} %\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c6245bce-7530-45bd-8d83-dbb8da5be13f",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.19"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
