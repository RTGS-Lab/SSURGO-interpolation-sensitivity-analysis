WITH ott_mupol_table as
(
SELECT gid as ott_mupol_gid,
   mukey as ott_mupol_mukey,
   shape_leng,
   shape_area,
   geom
FROM mupoly_ott_4326
),
component_table as
(
SELECT oid_ as co_oid_,
   compname,
   compkind,
   slope_r,
   elev_r,
   mukey as co_mukey,
   cokey as co_cokey
FROM component_copy
),
horizon_table as
(
SELECT oid_ as hz_oid_,
   hzname,
   hzdept_r,
   hzdepb_r,
   hzthk_r,
   awc_r,
   cokey as hz_cokey,
   chkey
FROM chorizon_copy
),
ottmupol_component_join as
(
SELECT *
FROM ott_mupol_table m
INNER JOIN component_table c
ON CAST(m.ott_mupol_mukey as int) = c.co_mukey
ORDER BY ott_mupol_mukey ASC
),
ott_hz_join as
(
SELECT *
FROM ottmupol_component_join c
INNER JOIN horizon_table h
ON c.co_cokey = h.hz_cokey
ORDER BY co_mukey ASC
),
spl25_yes_no as
(
SELECT ott_mupol_gid,
   co_oid_
   hz_oid,
   ott_mupol_mukey,
   co_mukey,
   co_cokey,
   hz_cokey,
   chkey,
   awc_r,
   hzdept_r,
   hzdepb_r,
   hzthk_r,
   compname,
   compkind,
   elev_r,
   shape_leng,
   shape_area,
   geom,
   25 BETWEEN hzdept_r AND hzdepb_r as fall_on_25_spl,
   hzdepb_r - hzdept_r as OG_thk_r,
   ROW_NUMBER() OVER (ORDER BY chkey)
FROM ott_hz_join
),
fallonspl25_thk as
(
SELECT chkey,
   hzdept_r,
   hzdepb_r,
   fall_on_25_spl,
   hzthk_r,
   25 - hzdept_r as thk_upto_spl25,
   row_number
FROM spl25_yes_no
WHERE fall_on_25_spl = TRUE
),
joined_true_false_spl as
(
SELECT ott_mupol_gid,
   hz_oid,
   ott_mupol_mukey,
   co_cokey,
   hz_cokey,
   tf.chkey,
   awc_r,
   tf.hzdept_r,
   tf.hzdepb_r,
   tf.hzthk_r,
   compname,
   compkind,
   elev_r,
   shape_leng,
   shape_area,
   tf.fall_on_25_spl,
   thk_upto_spl25,
   og_thk_r,
   tf.row_number,
   geom
FROM spl25_yes_no tf
LEFT JOIN fallonspl25_thk t
ON tf.row_number = t.row_number
),
working_thk_table as
(
SELECT *,
   CASE WHEN fall_on_25_spl = FALSE
   THEN og_thk_r ELSE thk_upto_spl25 END AS working_thk
FROM joined_true_false_spl
WHERE hzdepb_r <= 25 OR fall_on_25_spl
),
final_ott_25hzthk as
(
SELECT *
FROM working_thk_table
WHERE working_thk <> 0
),
frac_thickness as
(
SELECT *,
   (CAST(working_thk as float))/25 as thk_fraction
FROM final_ott_25hzthk
),
grouped_by_hzkey as
(
SELECT hz_oid,
   ott_mupol_mukey,
   co_cokey,
   hz_cokey,
   chkey,
   awc_r,
   hzdept_r,
   hzdepb_r,
   hzthk_r,
   compname,
   compkind,
   elev_r,
   fall_on_25_spl,
   thk_upto_spl25,
   og_thk_r,
   working_thk,
   thk_fraction
FROM frac_thickness
GROUP BY hz_oid,
   ott_mupol_mukey,
   co_cokey,
   hz_cokey,
   chkey,
   awc_r,
   hzdept_r,
   hzdepb_r,
   hzthk_r,
   compname,
   compkind,
   elev_r,
   fall_on_25_spl,
   thk_upto_spl25,
   og_thk_r,
   working_thk,
   thk_fraction
ORDER BY co_cokey
)
SELECT sum(awc_r * thk_fraction) as wghtd_ave_awc_co, co_cokey, ott_mupol_mukey
FROM grouped_by_hzkey
GROUP BY co_cokey, ott_mupol_mukey
ORDER BY ott_mupol_mukey







ottsoilsblob as
(
SELECT ST_Union(geom) as geom
FROM final_ott_25hzthk
)
--clipped_parcels_where_soil as
--(
SELECT p.gid as parcel_gid,
   p.county_pin,
   p.state_pin,
   p.anumber,
   ST_Multi(
      ST_Buffer(
	     ST_Intersection(s.geom, p.geom),
		 0.0
	  )
   ) clippedgeom
FROM ottsoilsblob s
INNER JOIN parcels_ott_4326 p on ST_Intersects(s.geom, p.geom)
WHERE NOT St_IsEmpty(ST_Buffer(ST_Intersection(s.geom, p.geom), 0.0))
--)
--eq_area_sqm as
--(
--SELECT parcel_gid,
   --county_pin,
   --state_pin,
   --anumber,
   --ST_Transform(clippedgeom, 2163) as EqAreaGeom,
   --ST_Area(ST_Transform(clippedgeom, 2163)) as sqm,
   --'gbcode' as gbcode
--FROM clipped_parcels_where_soil
--),
--sum_of_areas as
--(
--SELECT sum(sqm)
--FROM eq_area_sqm
--GROUP BY gbcode
--)
--SELECT e.*, s.sum, e.sqm/s.sum as frac_of_sum
--FROM eq_area_sqm e, sum_of_areas s








with identity_eq_area as
(
SELECT *,
ST_Transform(geom, 2163) as EqAreaGeom,
ST_Area(ST_Transform(geom, 2163)) as sqm
FROM parcels_where_soil_2_identit
),
tot_parcel_eq_area as
(
SELECT parcel_gid,
   ST_Transform(ST_Union(geom), 2163) as eq_area_geom,
   ST_Area(ST_Transform(ST_Union(geom), 2163)) as tot_parcel_area
FROM parcels_where_soil_2_identit
GROUP BY parcel_gid
),
joined_tot_area_identity as
(
SELECT i.*, t.tot_parcel_area
FROM identity_eq_area i
LEFT JOIN tot_parcel_eq_area t
ON i.parcel_gid = t.parcel_gid
)
