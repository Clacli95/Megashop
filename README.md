# MegaShop — Pipeline di analisi delle vendite

Il progetto elabora i log delle transazioni e lo storico Parquet di MegaShop per calcolare l'importo medio per metodo di pagamento, il fatturato per categoria e il numero di transazioni per regione.

## Struttura della pipeline

| Script | Elaborazione |
| --- | --- |
| `01_ingestion.py` | Pandas legge un file JSON alla volta e accumula il totale di `amount`. Dask legge i file con il carattere jolly `*.json` e calcola la media di `amount` per `payment_type`. |
| `02_etl.py` | PySpark unisce le transazioni alle tabelle dei prodotti e delle regioni tramite `product_id` e `region_id`. Il risultato contiene `transaction_id`, `region_name`, `category`, `amount` e `year` e viene salvato in Parquet, partizionato per anno. |
| `03_reporting.py` | Spark somma gli importi per categoria. Solo la tabella aggregata passa a Pandas con `.toPandas()`. Matplotlib genera il grafico a barre. |
| `04_streaming.py` | Spark Structured Streaming legge i file JSON aggiunti alla cartella di ingresso e stampa in console i conteggi cumulativi per regione. |

La lettura sequenziale con Pandas limita la memoria necessaria al singolo file. In Dask il calcolo viene eseguito quando si chiama `.compute()`. Nell'ETL i join interni escludono le transazioni senza riscontro nelle anagrafiche. Le chiavi duplicate nelle anagrafiche causano un errore perché produrrebbero più righe per la stessa vendita e altererebbero il fatturato. Vengono inoltre scartate le righe con valori nulli nelle colonne finali.

## Risultati della prova

La verifica è stata eseguita su sei transazioni sintetiche generate con `genera_demo.py`. Il generatore del docente non era disponibile per la prova.

| Controllo | Risultato |
| --- | --- |
| Totale vendite | 500,00 |
| Importo medio per pagamento | Carta 76,67 · Contanti 90,00 |
| Parquet elaborato | 6 righe nelle partizioni `year=2024` e `year=2025` |
| Fatturato per categoria | Elettronica 370,00 · Casa 130,00 |
| Transazioni per regione | Nord 3 · Sud 3 |

Il grafico della prova è in [demo_fatturato_per_categoria.png](demo_fatturato_per_categoria.png).

## Esecuzione su Windows

Ambiente utilizzato per la prova: Python 3.12, Spark 4.3.0 e JDK 17. Il launcher usa PySpark incluso in Spark. Se Spark o Java sono installati in percorsi diversi da quelli del launcher, impostare `SPARK_HOME` e `JAVA_HOME`. Per l'accesso ai file locali, `configura_hadoop.py` scarica i binari Windows di Hadoop 3.5.0 nella cartella del progetto e verifica l'impronta dell'archivio. Se sono già presenti in una distribuzione compatibile, impostare `HADOOP_HOME`.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe configura_hadoop.py
```

Per riprodurre la prova dalla radice del repository:

```powershell
.\.venv\Scripts\python.exe genera_demo.py
.\.venv\Scripts\python.exe 01_ingestion.py --json-dir demo_dataset/transazioni_json
powershell -ExecutionPolicy Bypass -File .\avvia-spark.ps1 02_etl.py --dataset demo_dataset --output data_local/demo_processed_sales
powershell -ExecutionPolicy Bypass -File .\avvia-spark.ps1 03_reporting.py --input data_local/demo_processed_sales --output demo_fatturato_per_categoria.png
powershell -ExecutionPolicy Bypass -File .\avvia-spark.ps1 04_streaming.py --input demo_dataset/transazioni_json --once
```

Per lo streaming continuo, avviare `04_streaming.py` con lo stesso launcher senza argomenti e aggiungere i file JSON uno alla volta in `data_local/json`. La console mostra il totale aggiornato a ogni micro-batch.

Con il dataset della consegna in `dataset/`, eseguire i primi tre script con i percorsi predefiniti. L'ETL scrive in `data_local/processed_sales` e il report in `fatturato_per_categoria.png`. Se i JSON sono array, passare `--array` al primo script. Se lo storico ha una colonna data al posto di `year`, indicarla all'ETL con `--date-column`. Per ripetere l'ETL usare una nuova cartella di output e passarla al report con `--input`.

