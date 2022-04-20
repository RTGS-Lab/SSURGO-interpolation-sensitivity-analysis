CREATE TABLE cm25_aws_nonull_420(
    mukey integer,
	muaggatt_25awc float,
	calcd_25awc float
);


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
   comppct_r,
   mukey as co_mukey,
   cokey as co_cokey
FROM component_copy
),
horizon_table as
(
SELECT oid_ as hz_oid_,
   hzdept_r,
   hzdepb_r,
   awc_r,
   cokey as hz_cokey,
   chkey
FROM chorizon_copy
),
mupol_component_join as
(
SELECT mupol_gid,
   mupol_mukey,
   co_oid_,
   comppct_r,
   co_mukey,
   co_cokey
FROM mupol_table m
INNER JOIN component_table c
ON CAST(m.mupol_mukey as int) = c.co_mukey
ORDER BY mupol_mukey ASC
),
hz_join as
(
SELECT mupol_gid,
   mupol_mukey,
   co_oid_,
   comppct_r,
   co_mukey,
   co_cokey,
   hz_oid_,
   hzdept_r,
   hzdepb_r,
   awc_r,
   hz_cokey,
   chkey
FROM mupol_component_join c
LEFT JOIN horizon_table h
ON c.co_cokey = h.hz_cokey
ORDER BY co_mukey ASC
),
distinct_hzkeys as
(
SELECT --mupol_gid,
   mupol_mukey,
   co_mukey,
   co_cokey,
   hz_cokey,
   chkey,
   hzdept_r,
   hzdepb_r,
   awc_r,
   comppct_r,
   co_oid_,
   hz_oid_
FROM hz_join
GROUP BY --mupol_gid,
   mupol_mukey,
   co_mukey,
   co_cokey,
   hz_cokey,
   chkey,
   hzdept_r,
   hzdepb_r,
   awc_r,
   comppct_r,
   co_oid_,
   hz_oid_
),
null_chkey_mukeys as
(
SELECT mupol_mukey, chkey
FROM distinct_hzkeys
WHERE chkey IS NULL
),
spl25_yes_no as
(
SELECT --mupol_gid,
   mupol_mukey,
   co_mukey,
   co_cokey,
   hz_cokey,
   chkey,
   awc_r,
   hzdept_r,
   hzdepb_r,
   comppct_r,
   25 BETWEEN hzdept_r AND hzdepb_r as fall_on_25_spl,
   hzdepb_r - hzdept_r as OG_thk_r,
   ROW_NUMBER() OVER (ORDER BY chkey),
   co_oid_,
   hz_oid_
FROM distinct_hzkeys
WHERE mupol_mukey NOT IN
   (SELECT mupol_mukey
   FROM null_chkey_mukeys)
),
fallonspl25_thk as
(
SELECT chkey,
   hzdept_r,
   hzdepb_r,
   fall_on_25_spl,
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
   thk_upto_spl25 as thkupto25_true,
   row_number as row_number_true
FROM fallonspl25_thk
ORDER BY chkey_true
),
joined_true_false_spl as
(
SELECT *,
   awc_r IS NOT NULL as awc_notnull
FROM spl25_yes_no tf
LEFT JOIN fallon25_aliases t
ON tf.row_number = t.row_number_true
),
nullawctable as
(
SELECT mupol_mukey, chkey, awc_notnull
FROM joined_true_false_spl
WHERE awc_notnull = FALSE
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
awc_by_components as
(
SELECT sum(awc_r * thk_fraction) as wghtd_ave_awc_co,
   co_cokey,
   CAST(comppct_r as FLOAT)/100 as comp_fraction,
   comppct_r,
   mupol_mukey
FROM frac_thickness
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
),
awc_by_mukey as
(
SELECT sum(wghtd_ave_awc_co * comp_fraction) as wghtd_mu_awc,
   mupol_mukey
FROM majorcmpfracs
GROUP BY mupol_mukey
ORDER BY mupol_mukey
),
data_output as
(
SELECT *
FROM awc_by_mukey a
WHERE a.mupol_mukey NOT IN
	(SELECT mupol_mukey
	FROM nullawctable)
),
muaggatt_awsvals as
(
SELECT mukey as muagg_mukey,
   aws025wta
FROM muaggatt_copy
),
joined_calcd_muagg_aws as
(
SELECT *
FROM data_output c
INNER JOIN muaggatt_awsvals m
ON CAST(c.mupol_mukey AS int) = m.muagg_mukey
ORDER BY mupol_mukey ASC
),
calculated_data as
(
SELECT wghtd_mu_awc*25 as calcd_25awc,
   CAST(mupol_mukey AS int) as int_mupol_mukey,
   muagg_mukey,
   aws025wta as muaggatt_25awc
FROM joined_calcd_muagg_aws
)
INSERT INTO cm25_aws_nonull_420(mukey, muaggatt_25awc, calcd_25awc)
SELECT int_mupol_mukey, muaggatt_25awc, calcd_25awc
FROM calculated_data
