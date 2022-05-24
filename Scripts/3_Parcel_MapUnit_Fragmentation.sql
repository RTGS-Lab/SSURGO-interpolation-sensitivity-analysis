/*
This script is the first of two used to complete Research Question 2, which
assess how the use of different spatial interpolation algorithms for summarizing
Available Water Capicity (AWC) from SSURGO map units to parcel geometries vary
the final reported parcel-level AWC values.

This script splits the parcel and map unit polygons into a mosaic of smaller
polygons, by using the intersection of the parcel and map unit borders to define
new polygons. The smaller 'fragments' retain both the IDs of the parcel and
map unit polygons that define them.
*/

WITH mapunit_parcel_intersect_fragments AS
(
--ID's for both parcel & map unit polygons:
SELECT p.gid, s.mukey,
       s.aws_0_12in, --available water storage of map unit @ 0-25cm depth
       s.aws_12_24in, --available water storage of map unit @ 0-50cm depth
       s.aws_24_36in, --available water storage of map unit @ 0-100cm depth
       s.aws_36_48in, --available water storage of map unit @ 0-150cm depth
       --splitting parcels & map units into smaller fragments by their borders:
       ST_Intersection(p.geom, s.geom) as splitgeom
FROM parcels_ott_4326 p,
     ott_4326_jn_inch_aws s
WHERE ST_Intersects(p.geom, s.geom) --retain ID's of each (where they overlap)
),

equal_area_fragments as --data originally projected in EPSG 4326; data reprojec-
                        --ted to US National Atlas Equal Area to get parcel area
(
SELECT gid,
       mukey,
       aws_0_12in,
       aws_12_24in,
       aws_24_36in,
       aws_36_48in,
       ST_Transform(splitgeom, 2163) as EqAreaGeom,
       ST_Area(ST_Transform(splitgeom, 2163)) as sqm --area of fragments in sqm
FROM mapunit_parcel_intersect_fragments
),

total_parcel_area AS --getting area of original, unsplit parcel polygons
(
SELECT gid as tot_gid,
       ST_Transform(geom, 2163) as eq_area_geom,
       ST_Area(ST_Transform(geom, 2163)) as tot_parcel_area
FROM parcels_ott_4326
)

--totalparcel_fragment_join as --joining original parcel area to fragments (to
                             --calculate ratio of area for Area Weighted Ave)
--(
SELECT gid,
       mukey,
       aws_0_12in,
       aws_12_24in,
       aws_24_36in,
       aws_36_48in,
       eqareageom as fraggeom,
       sqm as fragment_sqm,
       eq_area_geom,
       tot_parcel_area,
       sqm/tot_parcel_area as frag_prcnt_parcel
INTO ott_parc_frag_totareajn_inches
FROM equal_area_fragments s
INNER JOIN total_parcel_area t
ON s.gid = t.tot_gid --join on parcel ID
