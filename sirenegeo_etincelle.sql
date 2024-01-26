--- VUE VERS SIRENE GEO NATIONAL : 1 Go
CREATE OR REPLACE VIEW sirenegeo AS 
FROM 'https://static.data.gouv.fr/resources/sirene-geolocalise-parquet/20240107-143656/sirene2024-geo.parquet' ;

SELECT count(*) FROM sirenegeo ;

--- EXTRACTION DES ÉTABLISSEMENTS ÉTINCELLE COWORKING
CREATE OR REPLACE TABLE etincelle AS 
FROM sirenegeo SELECT geometry, denominationUniteLegale nom, geo_adresse adr
WHERE codeCommuneEtablissement = '31555' AND siren = '808183610' ;

FROM etincelle ;

LOAD spatial ;

--- ÉTABLISSEMENTS À 100 M DE ÉTINCELLE COWORKING
CREATE OR replace TABLE autour_etincelle AS 
FROM sirenegeo CROSS JOIN etincelle 
SELECT ST_distance(
	etincelle.geometry.ST_geomFromWkb().ST_transform('EPSG:4326','EPSG:2154',true),
	sirenegeo.geometry.ST_geomFromWkb().ST_transform('EPSG:4326','EPSG:2154',true)
)::int AS distance,
siret, denominationUniteLegale AS nom, activitePrincipaleEtablissement AS NIV5, 
geo_adresse, sirenegeo.geometry
WHERE codeCommuneEtablissement = '31555' AND distance < 100 ;

FROM autour_etincelle 
ORDER BY distance, (nom ilike 'etincelle%')::int desc;

--- RÉDUCTION A 30 M ET APPARIEMENT AVEC LA NOMENCLATURE NAF
FROM autour_etincelle
LEFT JOIN 'https://static.data.gouv.fr/resources/naf-1/20231123-121750/nafr2.parquet' n 
USING (NIV5)
SELECT n.LIB_NIV5, autour_etincelle.* EXCLUDE(geometry), geometry.ST_geomFromWkb()::varchar AS geom 
WHERE distance < 50;