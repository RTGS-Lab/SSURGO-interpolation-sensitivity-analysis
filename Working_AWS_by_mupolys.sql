
WITH mupol_table as
(
SELECT gid as mupol_gid,
   mukey as mupol_mukey
   --shape_leng,
   --shape_area,
   --geom
FROM mupolygoncopy
),
component_table as
(
SELECT oid_ as co_oid_,
   compname,
   compkind,
   comppct_r,
   majcompflag,
   slope_r,
   elev_r,
   mukey as co_mukey,
   cokey as co_cokey
FROM component_copy
--WHERE majcompflag  = 'Yes'
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
mupol_component_join as
(
SELECT *
FROM mupol_table m
INNER JOIN component_table c
ON CAST(m.mupol_mukey as int) = c.co_mukey
ORDER BY mupol_mukey ASC
),
hz_join as
(
SELECT *
FROM mupol_component_join c
INNER JOIN horizon_table h
ON c.co_cokey = h.hz_cokey
ORDER BY co_mukey ASC
),
spl25_yes_no as
(
SELECT mupol_gid,
   co_oid_
   hz_oid_,
   mupol_mukey,
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
   comppct_r,
   elev_r,
   --shape_leng,
   --shape_area,
   --geom,
   25 BETWEEN hzdept_r AND hzdepb_r as fall_on_25_spl,
   hzdepb_r - hzdept_r as OG_thk_r,
   ROW_NUMBER() OVER (ORDER BY chkey)
FROM hz_join
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
fallon25_aliases as
(
SELECT chkey as chkey_true,
   hzdept_r as hzdep_tr_true,
   hzdepb_r as hzdep_br_true,
   fall_on_25_spl as fallonspltrue,
   hzthk_r as hzthk_r_true,
   thk_upto_spl25 as thkupto25_true,
   row_number as row_number_true
FROM fallonspl25_thk
ORDER BY chkey_true
),
joined_true_false_spl as
(
SELECT *
FROM spl25_yes_no tf
LEFT JOIN fallon25_aliases t
ON tf.row_number = t.row_number_true
),
working_thk_table as
(
SELECT *,
   CASE WHEN fall_on_25_spl = FALSE
   THEN og_thk_r ELSE thkupto25_true END AS working_thk
FROM joined_true_false_spl
WHERE hzdepb_r <= 25 OR fall_on_25_spl
),
final_25hzthk as
(
SELECT *
FROM working_thk_table
WHERE working_thk <> 0
),
frac_thickness as
(
SELECT *,
   (CAST(working_thk as float))/25 as thk_fraction
FROM final_25hzthk
),
grouped_by_hzkey as
(
SELECT hz_oid_,
   mupol_mukey,
   co_cokey,
   hz_cokey,
   chkey,
   awc_r,
   hzdept_r,
   hzdepb_r,
   hzthk_r,
   compname,
   compkind,
   comppct_r,
   elev_r,
   fall_on_25_spl,
   thkupto25_true,
   og_thk_r,
   working_thk,
   thk_fraction
FROM frac_thickness
GROUP BY hz_oid_,
   mupol_mukey,
   co_cokey,
   hz_cokey,
   chkey,
   awc_r,
   hzdept_r,
   hzdepb_r,
   hzthk_r,
   compname,
   compkind,
   comppct_r,
   elev_r,
   fall_on_25_spl,
   thkupto25_true,
   og_thk_r,
   working_thk,
   thk_fraction
ORDER BY mupol_mukey
),
awc_by_components as
(
SELECT sum(awc_r * thk_fraction) as wghtd_ave_awc_co,
   co_cokey,
   CAST(comppct_r as FLOAT)/100 as comp_fraction,
   comppct_r,
   mupol_mukey
FROM grouped_by_hzkey
GROUP BY co_cokey, mupol_mukey, comp_fraction, comppct_r
ORDER BY mupol_mukey
),
summed_comppcts as
(
SELECT sum(comppct_r) as summed_comppct,
   mupol_mukey as mukey
FROM awc_by_components
GROUP BY mukey
ORDER BY mukey
),
joined_compsums as
(
SELECT *
FROM awc_by_components ac
LEFT JOIN summed_comppcts sc
ON ac.mupol_mukey = sc.mukey
),
majorcmpfracs as
(
SELECT *,
   (CAST(comppct_r AS FLOAT))/(CAST(summed_comppct AS FLOAT)) as majorcmpfrac
FROM joined_compsums
)
SELECT sum(wghtd_ave_awc_co * comp_fraction) as wghtd_mu_awc,
   mupol_mukey
FROM majorcmpfracs
GROUP BY mupol_mukey
ORDER BY mupol_mukey
