-- 1. Creación del stream
CREATE STREAM stock_updates (symbol VARCHAR, price DOUBLE, volume DOUBLE)
	WITH (kafka_topic='stock-updates', value_format='json', partitions=1);

-- 2. Creación de la vista materializada
CREATE TABLE stock_queries AS
	SELECT
	 symbol,
	 AVG(price) AS avg_price,
	 COUNT(1)   AS qty_transactions,
	 MAX(price) AS max_price,
	 MIN(price) AS min_price
	FROM stock_updates
	GROUP BY symbol EMIT CHANGES;

-- 3. Consulta 1: ¿Cuál fue el promedio ponderado de precio de una unidad por cada uno de los símbolos procesados? (e.j. AAPL)
SELECT avg_price, symbol FROM stock_queries;

-- 4. Consulta 2: ¿Cuántas transacciones se procesaron por símbolo?
SELECT qty_transactions, symbol FROM stock_queries;

-- 5. Consulta 3: ¿Cuál fue el máximo precio registrado por símbolo?
SELECT max_price, symbol FROM stock_queries;

-- 6. Consulta 4: ¿Cuál fue el mínimo precio registrado por símbolo?
SELECT min_price, symbol FROM stock_queries;