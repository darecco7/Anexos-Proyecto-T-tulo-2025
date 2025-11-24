--Base de usuarios
--Que se haya reistrado en los ult 6 meses, no activados
--Que haya transaccionado más de 1 vez
WITH fecha_activacion AS (
	SELECT
		CAST(SUBSTRING(CAST(id_usuario AS VARCHAR), 10, 30) AS BIGINT) AS id_usuario,
		MIN(DATE(fecha_tx)) AS fecha_activacion
	FROM fidelidad_reportes.resumen_actividad_productos_app
	WHERE 
		DATE(fecha_tx) <= DATE '2025-09-30' -- <<< CAMBIO: Fecha de corte de predicción
	GROUP BY 
		1
),
cabecera AS (
	SELECT
		cab.fecha_transaccion,
		cab.fecha_tx,
		CAST(SUBSTRING(CAST(cab.id_usuario AS VARCHAR), 10, 30) AS BIGINT) AS id_usuario,
		CASE WHEN cab.codigo_empresa = 'COPECCOMBUSTIBLE' THEN 'combustible' ELSE 'tienda' END AS codigo_empresa,
		cab.codigo_empresa_codigo_tx AS codigo_tx,
		cab.codigo_sitio,
		cab.descripcion_forma_pago,
		cab.monto_total,
		DATE_DIFF('DAY', LAG(cab.fecha_transaccion) OVER (PARTITION BY cab.id_usuario ORDER BY cab.fecha_transaccion), cab.fecha_transaccion) AS dias_tx
	FROM fidelidad_staging.trx_cabecera cab
	WHERE
		cab.codigo_empresa IN ('COPECCOMBUSTIBLE','PRONTO','PUNTO')
		AND cab.codigo_cliente IN ('MUEVOPERSONAS','CAJA','POSES')
		AND cab.fecha_transaccion BETWEEN DATE '2025-09-30' - INTERVAL '4' MONTH AND DATE '2025-09-30'
		AND cab.tipo_autorizador = 'PERSONAS'
), 
usuarios AS (
	SELECT 
		ru.id_usuario
		,DATE(ru.fechaprimerainteraccionapp) AS fecha_registro_app
		,DATE '2025-09-30' AS fecha_activacion_app
		,DATE_DIFF('DAY', DATE(ru.fechaprimerainteraccionapp), DATE '2025-09-30') AS dias_registro_activacion
	FROM fidelidad_reportes.resumen_usuario AS ru
	LEFT JOIN fecha_activacion AS fa ON 
		fa.id_usuario = ru.id_usuario 
	WHERE 
		fa.id_usuario IS NULL 
		AND DATE(ru.fechaprimerainteraccionapp) BETWEEN DATE '2025-09-30' - INTERVAL '4' MONTH AND DATE '2025-09-30'
),
pago_preferido AS (
	SELECT 
		id_usuario
		,descripcion_forma_pago
	FROM (
		SELECT
			cab.id_usuario,
			cab.descripcion_forma_pago,
      		COUNT(DISTINCT cab.codigo_tx) AS tx,
      		ROW_NUMBER() OVER (PARTITION BY cab.id_usuario ORDER BY COUNT(DISTINCT cab.codigo_tx) DESC) AS rn
    	FROM cabecera AS cab 
		LEFT JOIN usuarios AS u ON 
			cab.id_usuario = u.id_usuario
		WHERE 
			cab.fecha_transaccion < u.fecha_activacion_app -- <-- Esto ahora es < 30-09-2025
    	GROUP BY 
    		1,2
  )
	WHERE 
		rn = 1
),
puntos_copec AS (
	SELECT 
		CAST(SUBSTRING(cc.id_usuario,10,30) AS BIGINT) AS id_usuario
		,SUM(cc.monto_total) AS puntos
	FROM fidelidad_staging.trx_cuenta_corriente_cabecera AS cc 
	LEFT JOIN usuarios AS u ON 
        CAST(SUBSTRING(cc.id_usuario,10,30) AS BIGINT) = u.id_usuario
		AND cc.fecha_transaccion < u.fecha_activacion_app -- <-- Esto ahora es < 30-09-2025
	WHERE 
	    u.id_usuario IS NOT NULL 
	GROUP BY 
		1
),
emails_pre_t0 AS (
	  SELECT
	    u.id_usuario,
	    SUM(CASE WHEN e.src = 'sent' THEN 1 ELSE 0 END) AS emails_sent_pre_t0,
	    SUM(CASE WHEN e.src = 'open' THEN 1 ELSE 0 END) AS emails_open_pre_t0
	  FROM (
	    SELECT 'sent' AS src, LOWER(s.subscriber_key) AS sk_lc, DATE(s.event_date) AS d
	    FROM mktcloud_staging.sent s
	    WHERE s.event_type = 'Sent'
	      AND s.subscriber_key IS NOT NULL
	      AND s.event_date < TIMESTAMP '2025-10-01 00:00:00' 
	    UNION ALL
	    SELECT 'open' AS src, LOWER(o.subscriber_key) AS sk_lc, DATE(o.event_date) AS d
	    FROM mktcloud_staging.opens o
	    WHERE o.event_type = 'Open'
	      AND o.subscriber_key IS NOT NULL
	      AND o.event_date < TIMESTAMP '2025-10-01 00:00:00'
	  ) e
	  JOIN (
	    SELECT LOWER(id_contact) AS id_contact_lc, CAST(id_usuario AS BIGINT) AS id_usuario
	    FROM crm_staging.cuentas_b2c_bulk
	    WHERE id_contact IS NOT NULL
	  ) cb ON e.sk_lc = cb.id_contact_lc
	  JOIN usuarios u ON cb.id_usuario = u.id_usuario
	  WHERE e.d < u.fecha_activacion_app 
	  GROUP BY u.id_usuario
),
cabecera_pre_t0_agg AS (
	SELECT 
		cab.id_usuario
		,COUNT(DISTINCT cab.codigo_tx) AS tx
		,COUNT(DISTINCT CASE WHEN cab.codigo_empresa = 'combustible' THEN cab.codigo_tx END) AS tx_comb
		,COUNT(DISTINCT CASE WHEN cab.codigo_empresa != 'combustible' THEN cab.codigo_tx END) AS tx_tienda
		,COUNT(DISTINCT CASE WHEN cab.fecha_transaccion > u.fecha_activacion_app - INTERVAL '1' MONTH THEN cab.codigo_tx END) AS tx_1m
		,COUNT(DISTINCT CASE WHEN cab.fecha_transaccion > u.fecha_activacion_app - INTERVAL '3' MONTH THEN cab.codigo_tx END) AS tx_3m
		,COUNT(DISTINCT CASE WHEN cab.fecha_transaccion > u.fecha_activacion_app - INTERVAL '6' MONTH THEN cab.codigo_tx END) AS tx_6m
		,SUM(cab.monto_total) AS total_gastado
		,SUM(CASE WHEN cab.codigo_empresa = 'combustible' THEN cab.monto_total END) AS total_gastado_comb
		,SUM(CASE WHEN cab.codigo_empresa != 'combustible' THEN cab.monto_total END) AS total_gastado_tienda
		,ROUND(AVG(CASE WHEN cab.codigo_empresa = 'combustible' THEN cab.monto_total END),0) AS ticket_prom_comb
		,ROUND(AVG(CASE WHEN cab.codigo_empresa != 'combustible' THEN cab.monto_total END),0) AS ticket_prom_tienda
		,SUM(CASE WHEN DAY_OF_WEEK(cab.fecha_transaccion) IN (6, 7) THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(cab.codigo_tx), 0) AS share_fin_semana
		,SUM(CASE WHEN (HOUR(COALESCE(cab.fecha_tx, cab.fecha_transaccion)) BETWEEN 7 AND 9) 
					  OR (HOUR(COALESCE(cab.fecha_tx, cab.fecha_transaccion)) BETWEEN 18 AND 20) 
				 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(cab.codigo_tx), 0) AS share_horario_punta 
		,COUNT(DISTINCT cab.codigo_empresa) AS categorias_usadas 
		,AVG(CASE WHEN cab.dias_tx > 0 THEN cab.dias_tx END) AS frecuencia_copec 
	FROM cabecera AS cab 
	JOIN usuarios AS u ON 
		cab.id_usuario = u.id_usuario
		AND cab.fecha_transaccion < u.fecha_activacion_app 
	GROUP BY 
		1
),
eventos AS ( 
	SELECT 
		u.id_usuario
		,COUNT(CASE WHEN ev.event_name LIKE '%cata%' THEN ev.event_name END) AS catalogo
		,COUNT(CASE WHEN ev.event_name LIKE '%cup%' THEN ev.event_name END) AS cupones
		,COUNT(CASE WHEN ev.event_name LIKE '%promo%' THEN ev.event_name END) AS promo
	FROM eventos_app_copec_staging.eventos_app_copec AS ev
	INNER JOIN fidelidad_staging.usuario AS u ON 
		u.push_token_app_personas_id_equipo = ev.id_equipo 
	INNER JOIN usuarios AS us ON 
		us.id_usuario = u.id_usuario
		AND ev.event_date < us.fecha_activacion_app 
	WHERE	
		1=1
		AND (event_name LIKE '%visualizacion_promo%' OR ev.event_name LIKE '%visualizacion_cupo%' OR ev.event_name LIKE '%visualizacion_cata%')
	GROUP BY 
		1
)
SELECT 
	u.id_usuario
	-- <<< CAMBIO: Esta es la variable a predecir. Para este set, todos son 0 (no activados).
	,0 AS es_app_historico
	--,u.fecha_activacion_app 
	--,u.dias_registro_activacion 
	,CASE 
		WHEN pf.descripcion_forma_pago IN ('TARJETA DE CREDITO','CREDITO TBK') THEN 'CREDITO'
		WHEN pf.descripcion_forma_pago IN ('TARJETA DE DEBITO','DEBITO TBK')   THEN 'DEBITO'
		WHEN pf.descripcion_forma_pago IN ('EFECTIVO','DINERO')                THEN 'EFECTIVO'
		WHEN pf.descripcion_forma_pago IN ('APP COPEC','APP COPEC MUEVO')      THEN 'APP COPEC'
		ELSE pf.descripcion_forma_pago
	END AS forma_pago_preferida
	,COALESCE(pc.puntos, 0) AS puntos
	,COALESCE(agg.tx, 0) 		AS tx
	,COALESCE(agg.tx_comb, 0) 	AS tx_comb
	,COALESCE(agg.tx_tienda, 0) AS tx_tienda
	,COALESCE(agg.tx_1m, 0) 	AS tx_1m
	,COALESCE(agg.tx_3m, 0) 	AS tx_3m
	,COALESCE(agg.tx_6m, 0) 	AS tx_6m
	,COALESCE(agg.total_gastado, 0) AS total_gastado
	,agg.total_gastado_comb
	,agg.total_gastado_tienda
	,agg.ticket_prom_comb
	,agg.ticket_prom_tienda
	,COALESCE(em.emails_sent_pre_t0, 0) AS mails_enviados
	,COALESCE(em.emails_open_pre_t0, 0) AS mails_abiertos
	,ROUND(agg.frecuencia_copec, 2) AS frecuencia_copec 
	,ROUND(agg.share_fin_semana, 3) AS share_fin_semana 
	,ROUND(agg.share_horario_punta, 3) AS share_horario_punta 
	,CASE WHEN agg.categorias_usadas = 2 THEN 1 ELSE 0 END AS usuario_cross_sell 
	,CASE WHEN ev.catalogo > 0 THEN 1 ELSE 0 END AS vio_catalogo
	,CASE WHEN ev.promo > 0 THEN 1 ELSE 0 END AS vio_promo
	,CASE WHEN ev.cupones > 0 THEN 1 ELSE 0 END AS vio_cupones
FROM usuarios AS u
-- <<< CAMBIO: Este JOIN ya no es necesario para el target, pero lo dejo por si lo usas.
-- Siempre será NULL porque ya filtramos en el CTE 'usuarios'.
LEFT JOIN fecha_activacion AS fa ON 
	fa.id_usuario = u.id_usuario 
LEFT JOIN pago_preferido AS pf ON 
	pf.id_usuario = u.id_usuario
LEFT JOIN puntos_copec AS pc ON 
	pc.id_usuario = u.id_usuario
LEFT JOIN emails_pre_t0 AS em ON
	em.id_usuario = u.id_usuario
LEFT JOIN cabecera_pre_t0_agg AS agg ON
    agg.id_usuario = u.id_usuario
LEFT JOIN eventos AS ev ON 
	ev.id_usuario = u.id_usuario
WHERE 
	agg.tx > 1
;
