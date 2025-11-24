-- Clientes activados reales por fecha Octubre
WITH activaciones_reales AS (
	SELECT
		CAST(SUBSTRING(CAST(id_usuario AS VARCHAR), 10, 30) AS BIGINT) AS id_usuario,
		MIN(DATE(fecha_tx)) AS fecha_activacion_real
	FROM fidelidad_reportes.resumen_actividad_productos_app
	GROUP BY 
		1
)
SELECT
	id_usuario,
	1 AS target_real -- Marcamos como 1 (activó)
FROM activaciones_reales
WHERE
	fecha_activacion_real BETWEEN DATE '2025-10-01' AND DATE '2025-10-07'
;