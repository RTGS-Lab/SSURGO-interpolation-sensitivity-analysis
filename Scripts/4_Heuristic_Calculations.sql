/*
This query is the second of two used to complete Research Question 2, which
assess how the use of different spatial interpolation algorithms for summarizing
Available Water Capicity (AWC) from SSURGO map units to parcel geometries vary
the final reported parcel-level AWC values.

This query calculates the value of parcel-level available water storage when
two heuristics are used:

   1. If a soil type (map unit) takes up over 80% of a parcel's area, use that
	    soil's (map unit's) avail water storage for the whole parcel

	 2. If a soil type takes up less than 20% of a parcel's area, drop that soil
	    type from the area-weighted-average calculation of parcel avail water
			storage.
*/

WITH fragments_groupedby_gid_mukey as --using fragments (and associated vals)
                                      --from previous script
(
SELECT gid,
	   mukey,
	   SUM(fragment_sqm) as sumdmukeysparc
FROM ott_parc_frag_totareajn_inches
GROUP BY gid, mukey --two different soil polygons may have the same map unit key
                    --(eg. crescent shaped polygon cut by border of MN) so need
					--to group by distinct gid as well
ORDER BY gid
),

parcel_tot_areas as
(
SELECT gid as parc_gid,
       AVG(tot_parcel_area) as totparcarea --all values same; ave used here to
										   --simply capture the value
FROM ott_parc_frag_totareajn_inches
GROUP BY parc_gid
ORDER BY parc_gid
),

mukeyfractotal as --calculating fraction of area map unit takes up in parcel
(
SELECT gid,
       mukey,
	   sumdmukeysparc,
	   parc_gid,
	   totparcarea,
       sumdmukeysparc/totparcarea as fractotal
FROM fragments_groupedby_gid_mukey d
LEFT JOIN parcel_tot_areas p
ON d.gid = p.parc_gid
),

mkft_aliases as --re-aliasing to prevent redundant names after join
(
SELECT gid as gid_mkft,
	   mukey as mukey_mkft,
	   sumdmukeysparc as summupar_mkft,
	   parc_gid as parc_gid_mkft,
	   totparcarea as totparcarea_mkft,
	   fractotal as fractotal_mkft
FROM mukeyfractotal
),

singleottmukeyaws as
(
SELECT mukey,
       --AVE used here again to capture single value for mukeys (all vals same)
	   AVG(aws_0_12in) as aws0_12,
	   AVG(aws_12_24in) as aws12_24,
	   AVG(aws_24_36in) as aws24_36,
	   AVG(aws_36_48in) as aws36_48
FROM ott_4326_jn_inch_aws
GROUP BY mukey
ORDER BY mukey
),

joinedawsfractot as
(
SELECT gid_mkft,
       mukey_mkft,
	   summupar_mkft,
	   parc_gid_mkft,
	   totparcarea_mkft,
	   fractotal_mkft,
	   mukey,
	   aws0_12,
	   aws12_24,
	   aws24_36,
	   aws36_48
FROM mkft_aliases mkft
LEFT JOIN singleottmukeyaws soma
ON mkft.mukey_mkft = soma.mukey
),

clean_aws_fractot as --cleaning up data after join
(
SELECT gid_mkft,
       mukey,
	   aws0_12,
	   aws12_24,
	   aws24_36,
	   aws36_48,
       summupar_mkft as summupar,
       totparcarea_mkft as totparcarea,
       fractotal_mkft as fractotal
FROM joinedawsfractot
),

flagged as --flagging rows that fit into either heuristic category
(
SELECT gid_mkft,
       mukey,
	   aws0_12,
	   aws12_24,
	   aws24_36,
	   aws36_48,
	   summupar,
	   totparcarea,
	   fractotal,
       fractotal > 0.80 as greater80flag,
       fractotal < 0.20 as less20flag,
       ROW_NUMBER() OVER (ORDER BY gid_mkft) --creating temp set of unique IDs
FROM clean_aws_fractot
),

truegreat80flags as --capturing only mapunits that take up >80% parcel
(
SELECT row_number as rownum_g80,
       gid_mkft as gid_g80,
       mukey as mukey_g80,
       greater80flag g80flag,
       aws0_12 as aws0_12_g80,
       aws12_24 as aws12_24_g80,
       aws24_36 as aws24_36_g80,
       aws36_48 as aws36_48_g80
FROM flagged
WHERE greater80flag = TRUE
),

trueless20flags as --capturing only mapunits that take up <20% parcel
(
SELECT row_number as rownum_l20,
       gid_mkft as gid_l20,
       mukey as mukey_l20,
       less20flag l20flag,
       aws0_12 as aws0_12_l20,
       aws12_24 as aws12_24_l20,
       aws24_36 as aws24_36_l20,
       aws36_48 as aws36_48_l20
FROM flagged
WHERE less20flag = TRUE
),

joined_g80s as --joining >80% map units back to orig dataset
(
SELECT gid_mkft,
       mukey,
	   aws0_12,
	   aws12_24,
	   aws24_36,
	   aws36_48,
       summupar,
       totparcarea,
       fractotal,
       greater80flag,
       CASE WHEN gid_mkft IN
			      (
				  SELECT gid_g80
				  FROM truegreat80flags
				  )
	          THEN 1 ELSE 0 -- 1 = does take up >80%; 0 = does not
						END AS flag80,
       aws0_12_g80,
       aws12_24_g80,
       aws24_36_g80,
       aws36_48_g80
FROM flagged g
LEFT JOIN truegreat80flags t --left join so no repeated values
ON g.gid_mkft = t.gid_g80
),

nol20s as --capturing all mapunits that do NOT take up <20% of a parcel
(
SELECT gid_mkft,
       mukey,
	   aws0_12,
	   aws12_24,
	   aws24_36,
	   aws36_48,
       summupar,
       totparcarea,
       fractotal,
       less20flag
FROM flagged g
WHERE row_number NOT IN
          (
						SELECT rownum_l20
						FROM trueless20flags
					)
),

--because we are throwing out map units which take up <20% of parcels, we need
--an updated area calculation which reflects the sum of the parcels after those
--entries are removed. This is necessary for our area weighted ave calculation.
new_l20_totals as
(
SELECT gid_mkft as nft_gid,
       SUM(summupar) as l20newtotal
FROM nol20s
GROUP BY gid_mkft
ORDER BY gid_mkft
),

joined_new_totals as --joining updated summed area back to orig dataset
(
SELECT gid_mkft,
       mukey,
	   aws0_12,
	   aws12_24,
	   aws24_36,
	   aws36_48,
       summupar,
       l20newtotal
FROM nol20s n
LEFT JOIN new_l20_totals t
ON n.gid_mkft = t.nft_gid
),

--getting the area fraction each map unit takes up in parcel, with the updated
--area (sum of map unit fragments) in parcel when <20% fragments are removed:
l20_new_fractots as
(
SELECT gid_mkft,
       mukey,
	   aws0_12,
	   aws12_24,
	   aws24_36,
	   aws36_48,
	   summupar,
	   l20newtotal,
	   summupar/l20newtotal as l20newfractot
FROM joined_new_totals
),

-- final available water storage values, when <20% heuristic is applied during
-- area weighted average calculation:
l20_heuristic_table as
(
SELECT gid_mkft as gid_l20,
       SUM(l20newfractot*aws0_12) as aws0_12_l20,
       SUM(l20newfractot*aws12_24) as aws12_24_l20,
       SUM(l20newfractot*aws24_36) as aws24_36_l20,
       SUM(l20newfractot*aws36_48) as aws36_48_l20
FROM l20_new_fractots
GROUP BY gid_mkft
ORDER BY gid_mkft
),

-- final available water storage values, when >800% heuristic is applied during
-- area weighted average calculation:
g80_heuristic_table as
(
select gid_mkft as gid_g80,

       --if not flagged as >80%, calculate area weighted ave using normal method
			 --else, just use the soil type's value which takes up >80% of parcel:
       CASE WHEN flag80 = 0
	     THEN SUM(fractotal * aws0_12) ELSE AVG(aws0_12_g80)
			    END AS aws0_12_g80,

       CASE WHEN flag80 = 0
	     THEN SUM(fractotal * aws12_24) ELSE AVG(aws12_24_g80)
			    END AS aws12_24_g80,

       CASE WHEN flag80 = 0
	     THEN SUM(fractotal * aws24_36) ELSE AVG(aws24_36_g80)
			    END AS aws24_36_g80,

       CASE WHEN flag80 = 0
	     THEN SUM(fractotal * aws36_48) ELSE AVG(aws36_48_g80)
			    END AS aws36_48_g80

FROM joined_g80s
GROUP BY gid_mkft, flag80
ORDER BY gid_mkft
)

--joining values from both heuristic calculation, based on parcel GIDs:
SELECT gid_g80,
       aws0_12_g80,
	   aws12_24_g80,
	   aws24_36_g80,
	   aws36_48_g80,
	   gid_l20,
	   aws0_12_l20,
	   aws12_24_l20,
	   aws24_36_l20,
	   aws36_48_l20
FROM g80_heuristic_table g
LEFT JOIN l20_heuristic_table l
ON g.gid_g80 = l.gid_l20
