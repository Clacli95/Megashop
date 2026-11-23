# MegaShop — Data Engineering Pipeline

## Preparazione su Windows

Apri PowerShell nella cartella `MegaShop`. Usa Python 3.11 o successivo e Spark 4.3.0. Verifica che `python --version` punti a un'installazione Python effettiva.

```powershell
$python = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $python) {
    $python = Join-Path $env:USERPROFILE '.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
}
& $python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe configura_hadoop.py
```

Per lanciare Spark, `avvia-spark.ps1` usa il JDK 17 e la posizione di Spark rilevati sul tuo PC. Puoi indicare un'altra installazione con `$env:JAVA_HOME` e `$env:SPARK_HOME` nella stessa finestra. Il progetto usa PySpark incluso in Spark. [Requisiti di Spark 4.3.0](https://spark.apache.org/docs/4.3.0/).

Su questo PC Spark si avvia con il JDK 17. Per accedere ai file locali su Windows la distribuzione usa Hadoop 3.5.0 e richiede `hadoop.dll` e `winutils.exe`. Il comando di configurazione scarica i due binari di terze parti dalla [release Windows di zehelh/winutils](https://github.com/zehelh/winutils/releases/tag/hadoop-3.5.0), verifica l'impronta SHA-256 e li salva nella cartella `.hadoop` del progetto. Se hai già una distribuzione compatibile, puoi impostare `$env:HADOOP_HOME` e saltare il comando.

## Prova con dati dimostrativi

```powershell
.\.venv\Scripts\python.exe genera_demo.py
.\.venv\Scripts\python.exe 01_ingestion.py --json-dir demo_dataset/transazioni_json
powershell -ExecutionPolicy Bypass -File .\avvia-spark.ps1 02_etl.py --dataset demo_dataset --output data_local/demo_processed_sales
powershell -ExecutionPolicy Bypass -File .\avvia-spark.ps1 03_reporting.py --input data_local/demo_processed_sales --output demo_fatturato_per_categoria.png
```

Risultati attesi sui sei record sintetici

| Verifica | Risultato atteso |
| --- | --- |
| Totale Pandas | 500,00 |
| Media Dask | carta 76,67 · contanti 90,00 |
| Parquet Spark | 6 righe, partizioni `year=2024` e `year=2025` |
| Fatturato | Elettronica 370,00 · Casa 130,00 |
| Streaming | Nord 3 · Sud 3 dopo entrambi i file |

## Esecuzione con i dati del docente

Lo ZIP della consegna contiene il PDF. Quando hai `generator.py` del docente, mettilo nella cartella del progetto, controlla le dimensioni configurate ed eseguilo dalla stessa cartella.

```powershell
.\.venv\Scripts\python.exe generator.py
.\.venv\Scripts\python.exe 01_ingestion.py
powershell -ExecutionPolicy Bypass -File .\avvia-spark.ps1 02_etl.py
powershell -ExecutionPolicy Bypass -File .\avvia-spark.ps1 03_reporting.py
```

- **Esercizio 1.** Pandas somma un file per volta. Dask legge il glob `*.json` e calcola in parallelo la media per metodo di pagamento con `.compute()`. Il formato predefinito è JSON Lines. Per array JSON usa `--array`. Se la cartella è diversa usa `--json-dir`.
- **Esercizio 2.** PySpark cerca i batch in `dataset/transazioni_parquet` o `dataset/parquet` e le dimensioni in `dataset/anagrafiche` o `dataset/parquet`. Se i percorsi sono diversi, usa `--dataset` oppure `--transactions`, `--products` e `--regions`. Se manca `year`, indica la colonna data con `--date-column`. I join interni scartano le transazioni senza una corrispondenza nelle due anagrafiche. L'ETL rifiuta chiavi duplicate nelle anagrafiche per evitare di moltiplicare gli importi e rimuove le righe con campi finali nulli. La scrittura crea `data_local/processed_sales` senza sovrascriverla. Per una nuova prova usa `--output` con un percorso nuovo e passalo al report con `--input`.
- **Esercizio 3.** Spark aggrega il fatturato per categoria e `.toPandas()` trasferisce le sole righe aggregate. Matplotlib salva `fatturato_per_categoria.png`.
- **Bonus.** Lo streaming legge JSON Lines da `data_local/json` e mostra i conteggi cumulativi in console a ogni micro-batch. Usa `--region-column region_id` se i log contengono l'identificativo invece del campo `region`. I conteggi ripartono all'avvio e Spark rilegge i file già presenti.

Per la prova live, avvia lo streaming in un terminale.

```powershell
powershell -ExecutionPolicy Bypass -File .\avvia-spark.ps1 04_streaming.py
```

In un secondo terminale, nella stessa cartella del progetto, copia il primo file e aspetta che compaia la tabella. Copia poi il secondo file per vedere il conteggio aggiornato.

```powershell
Copy-Item demo_dataset/transazioni_json/batch_1.json data_local/json/
Copy-Item demo_dataset/transazioni_json/batch_2.json data_local/json/
```

Interrompi con Ctrl+C. Per una prova che termina da sola, usa `04_streaming.py --input demo_dataset/transazioni_json --once` con lo stesso launcher.
