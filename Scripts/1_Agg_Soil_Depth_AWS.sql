CREATE TABLE cm25_aws_nonull(
    mukey integer,
	  muaggatt_25awc float,
	  calcd_25awc float
);


/* The first part of this script involves joining the "MUPOLYGON", "Component"
   table, and "Horizon" table together, based on their common keys/ 1-to-many
   joins. This is necessary because we need summarized Available Water Storage
   (AWS) values for each SSURGO map unit, and there are many soil components
   within a single Map Unit polygon, and many horizons within a single
   component. (The full SSURGO data model can be viewed here:
   https://www.nrcs.usda.gov/Internet/FSE_DOCUMENTS/nrcs142p2_050900.pdf)
*/

-- First we capture the mupolygon, component, and horizon tables, and the
-- relevant information within them in CTEs:
WITH mupolygon_table AS
(
SELECT gid as mupol_gid, --columns aliased to prevent confusion after joins
       mukey as mupol_mukey
FROM mupolygoncopy
),

component_table AS
(
SELECT oid_ as co_oid_,
       comppct_r as prcnt_comp_in_mapunit,
       mukey as co_mukey,
       cokey as co_cokey
FROM component_copy
),

horizon_table AS
(
SELECT oid_ as hz_oid_,
       hzdept_r as dist_to_hz_top,
       hzdepb_r as dist_to_hz_bottom,
       awc_r as avail_water_capacity,
       cokey as hz_cokey,
       chkey
FROM chorizon_copy
),

-- Next we join (1::M) the MUPolygon and Component table on common ID "mukey"
mupol_component_join as
(
SELECT mupol_gid,
       mupol_mukey,
       co_oid_,
       prcnt_comp_in_mapunit,
       co_mukey,
       co_cokey
FROM mupolygon_table m
INNER JOIN component_table c
ON CAST(m.mupol_mukey as int) = c.co_mukey
ORDER BY mupol_mukey ASC
),

mupol_comp_hz_join AS -- (joining horizon table to the above joined table)
(
SELECT mupol_gid,
       mupol_mukey,
       co_oid_,
       prcnt_comp_in_mapunit,
       co_mukey,
       co_cokey,
       hz_oid_,
       dist_to_hz_top,
       dist_to_hz_bottom,
       avail_water_capacity,
       hz_cokey,
       chkey
FROM mupol_component_join c
LEFT JOIN horizon_table h
ON c.co_cokey = h.hz_cokey
ORDER BY co_mukey ASC
),

/* Because there are duplicate values for mukeys in the MUPOLYGON table (map
   units that for some reason are broken into two polygons), and all map units
   w/ the same 'mukey' have the same AWS (by SSURGO's logic), we may get rid of
   duplicate values using a GROUP BY function:
*/
distinct_hzkeys AS
(
SELECT mupol_mukey,
       co_mukey,
       co_cokey,
       hz_cokey,
       chkey,
       dist_to_hz_top,
       dist_to_hz_bottom,
       avail_water_capacity,
       prcnt_comp_in_mapunit,
       co_oid_,
       hz_oid_
FROM mupol_comp_hz_join
GROUP BY mupol_mukey,
         co_mukey,
         co_cokey,
         hz_cokey,
         chkey,
         dist_to_hz_top,
         dist_to_hz_bottom,
         avail_water_capacity,
         prcnt_comp_in_mapunit,
         co_oid_,
         hz_oid_
),

null_chkey_mukeys AS --Mapunits with NULL horizon keys are captured here
(
SELECT mupol_mukey,
       chkey
FROM distinct_hzkeys
WHERE chkey IS NULL
),

/* For the following part of the script, I chose to focus on one depth interval,
   0-25cm, to compare my script's calculated Available Water Storage values for
   map units with SSURGO's pre-aggregated/reported values at this interval. The
   following code will later include an user-inputted int variable, which would
   take the place of any "25's" seen below.

   Here a table/field is created which designates whether or not a horizon
   falls on/across the 25cm depth line. We need to do this because we are using
   horizon thickness as the "weight" for our area weighted average, and if the
   depth interval we are concerned with is 0-25cm, any horizon which extends
   beyond the 25cm line needs its original thickness updated to only represent
   the area it occupies above the 25cm line.
*/
falls_on_25cm_line_bool AS
(
SELECT mupol_mukey,
       co_mukey,
       co_cokey,
       hz_cokey,
       chkey,
       avail_water_capacity,
       dist_to_hz_top,
       dist_to_hz_bottom,
       prcnt_comp_in_mapunit,
       --boolean which reports if horizons falls 25cm line:
       25 BETWEEN dist_to_hz_top
          AND dist_to_hz_bottom as fall_on_25_line,
       dist_to_hz_bottom - dist_to_hz_top as OG_thk_r,
       -- I "re-indexed" my rows to have unique keys here because I had multiple
       -- entries with duplicate values (there are instances in the SSURGO data
       -- where different mapunit polygons have the same map unit key). At This
       -- time I wanted to preserve this data, so I created a new temp index.
       ROW_NUMBER() OVER (ORDER BY chkey) as temp_row_ID
FROM distinct_hzkeys
WHERE mupol_mukey NOT IN
                 (SELECT mupol_mukey
                  FROM null_chkey_mukeys) --avoiding mapunits with null horizons
),

fall_on_25cm_line_thk AS
(
SELECT chkey,
       dist_to_hz_top,
       dist_to_hz_bottom,
       fall_on_25_line,
       -- Getting updated thickness of horizons that fall on 25cm line by subtracting
       -- top-of-horizon depth from 25:
       25 - dist_to_hz_top as thk_upto_25line,
       temp_row_ID
FROM falls_on_25cm_line_bool
WHERE fall_on_25_line = TRUE
),

fallon25_aliases AS --aliases created so no redundant names after join
(
SELECT chkey as chkey_true,
       fall_on_25_line as fall_on_25_line_true,
       thk_upto_25line as thk_upto_25line_true,
       temp_row_ID as temp_row_ID_true
FROM fall_on_25cm_line_thk
ORDER BY chkey_true
),

joined_true_false_fallonline AS
(
SELECT mupol_mukey,
	     co_mukey,
	     co_cokey,
	     hz_cokey,
	     chkey,
	     avail_water_capacity,
	     dist_to_hz_top,
	     dist_to_hz_bottom,
	     prcnt_comp_in_mapunit,
	     fall_on_25_line,
	     og_thk_r,
	     temp_row_ID,
	     fall_on_25_line_true,
	     thk_upto_25line_true,
	     temp_row_ID_true,
       avail_water_capacity IS NOT NULL as awc_notnull
FROM falls_on_25cm_line_bool tf
LEFT JOIN fallon25_aliases t
ON tf.temp_row_ID = t.temp_row_ID_true
),

nullawctable AS -- capturing horizons (keys) with NULL avail water capacities
(
SELECT mupol_mukey, chkey, awc_notnull
FROM joined_true_false_fallonline
WHERE awc_notnull = FALSE
),

working_thk_table AS
(
SELECT mupol_mukey,
	     co_mukey,
	     hz_cokey,
	     chkey,
	     avail_water_capacity,
	     dist_to_hz_top,
	     dist_to_hz_bottom,
	     prcnt_comp_in_mapunit,
	     fall_on_25_line,
	     og_thk_r,
	     thk_upto_25line_true,
	     awc_notnull,
       -- if horizon doesnt fall on 25cm line, then use its original thickness.
       -- else: use the updated thickness calculated in this script.
       CASE WHEN fall_on_25_line = FALSE
            THEN og_thk_r ELSE thk_upto_25line_true
	          END AS working_thk
FROM joined_true_false_fallonline
--only selecting horizons that are below or fall on the 25cm line:
WHERE dist_to_hz_bottom <= 25 OR fall_on_25_line
),

final_25hzthk AS
(
SELECT mupol_mukey,
	     co_mukey,
	     hz_cokey,
	     chkey,
	     avail_water_capacity,
	     dist_to_hz_top,
	     dist_to_hz_bottom,
	     prcnt_comp_in_mapunit,
	     fall_on_25_line,
	     awc_notnull,
	     working_thk
FROM working_thk_table
--throwing out horizons w/ a thickness of 0 (horizons with tops *at* 25cm line):
WHERE working_thk <> 0
),

/* Next part of the script deals with summarizing available water capacity For
   map units using an area weighted average based on horizon AWC and thickness.
   AWA formula = (horizon A area ratio x horizon A's AWC) + (horizon B area
   ratio x horizon B's AWC) + (horizon C area ratio x horizon C's AWC) */

frac_thickness AS
(
SELECT mupol_mukey,
	     co_mukey,
	     hz_cokey,
	     chkey,
	     avail_water_capacity,
	     dist_to_hz_top,
	     dist_to_hz_bottom,
	     prcnt_comp_in_mapunit,
	     fall_on_25_line,
	     awc_notnull,
	     working_thk,
       (CAST(working_thk as float))/25 as thk_fraction
FROM final_25hzthk
),

-- Summarizing AWC by *components* using area weighted average of horizons:
awc_by_components AS
(
SELECT sum(avail_water_capacity * thk_fraction) as wghtd_ave_awc_co,
       hz_cokey,
       CAST(prcnt_comp_in_mapunit as FLOAT)/100 as comp_fraction,
       prcnt_comp_in_mapunit,
       mupol_mukey
FROM frac_thickness
GROUP BY hz_cokey,
         mupol_mukey,
         comp_fraction,
         prcnt_comp_in_mapunit
ORDER BY mupol_mukey
),

/* I didn't end up using/needing the following two CTE's, but I've left them in
   in case I end up going this route later.

   My reasoning was, a table/field is needed b/c we've dropped data (NULL; depth
   filter) and we're probably missing some components within mapunits. Because
   of this, the SSURGO pre-reported "percent component in mapunit" field may not
   add up to 100%. We need to adjust the 'total' component fraction value to
   represent only the components that are included in our data. */
summed_comppcts AS
(
SELECT sum(prcnt_comp_in_mapunit) as summed_comppct,
       mupol_mukey as mukey
FROM awc_by_components
GROUP BY mukey
ORDER BY mukey
),

joined_compsums AS
(
SELECT wghtd_ave_awc_co,
	     hz_cokey,
	     comp_fraction,
	     prcnt_comp_in_mapunit,
	     mupol_mukey,
	     summed_comppct,
	     mukey
FROM awc_by_components ac
LEFT JOIN summed_comppcts sc
ON ac.mupol_mukey = sc.mukey
),

updated_cmp_fracs AS
(
SELECT wghtd_ave_awc_co,
	     hz_cokey,
	     comp_fraction,
	     prcnt_comp_in_mapunit,
	     mupol_mukey,
	     summed_comppct,
       (
         CAST(prcnt_comp_in_mapunit AS FLOAT))/
         (CAST(summed_comppct AS FLOAT)
       ) as updatedcmpfrac
FROM joined_compsums
),

awc_by_mukey AS
(
--Summarizing AWC by *map units* using component % within map unit:
SELECT sum(wghtd_ave_awc_co * comp_fraction) as wghtd_mu_awc,
       mupol_mukey
FROM updated_cmp_fracs
GROUP BY mupol_mukey
ORDER BY mupol_mukey
),

data_output as
(
SELECT wghtd_mu_awc,
	     mupol_mukey
FROM awc_by_mukey a
WHERE a.mupol_mukey NOT IN
      (
        SELECT mupol_mukey
        FROM nullawctable
      )
),

-- capturring SSURGO's pre-aggregated AWC values by map units for 0-25cm:
muaggatt_awsvals AS
(
SELECT mukey as muagg_mukey,
       aws025wta
FROM muaggatt_copy
),

-- joining SSURGO's pre-agg'd AWC vals (by map unit) to this scripts calc'd vals
-- to allow analysis concerning similarity in reporting:
joined_calcd_muagg_aws AS
(
SELECT wghtd_mu_awc,
	     mupol_mukey,
	     muagg_mukey,
	     aws025wta
FROM data_output c
INNER JOIN muaggatt_awsvals m
ON CAST(c.mupol_mukey AS int) = m.muagg_mukey
ORDER BY mupol_mukey ASC
),

data_comparison_ssurgo_vs_customscript AS
(
SELECT wghtd_mu_awc*25 as calcd_25awc, --multiply by 25 to get AWS @ 0-25cm
       CAST(mupol_mukey AS int) as int_mupol_mukey,
       muagg_mukey,
       aws025wta as muaggatt_25awc
FROM joined_calcd_muagg_aws
)

INSERT INTO cm25_aws_nonull(mukey,muaggatt_25awc, calcd_25awc)
SELECT int_mupol_mukey,
       muaggatt_25awc,
       calcd_25awc
FROM data_comparison_ssurgo_vs_customscript
