/* This script is used to join together the Available Water Storage (AWS) values
for the 0-12in, 12-24in, 24-36in, and 36-48in depth ranges (calculated via
SSURGO's "Create Soil Map" ArcMap tool) for all SSURGO map units, based on
common map unit keys ('mukey').
*/

WITH aws_0_12 AS
(
SELECT mukey as mukey_0_12,
       aws_wta as aws_0_12in
FROM aws_zerothirty
GROUP BY mukey_0_12, aws_0_12in
),

aws_12_24 AS
(
SELECT mukey as mukey_12_24,
       aws_wta as aws_12_24in
FROM aws_thirtysixty
GROUP BY mukey_12_24, aws_12_24in
),

aws_24_36 AS
(
SELECT mukey as mukey_24_36,
       aws_wta as aws_24_36in
FROM aws_sixtyninty
GROUP BY mukey_24_36, aws_24_36in
),

aws_36_48 AS
(
SELECT mukey as mukey_36_48,
       aws_wta as aws_36_48in
FROM aws_nintyonetwent
GROUP BY mukey_36_48, aws_36_48in
),

jn_012_1224 as
(
SELECT *
FROM aws_0_12 a
INNER JOIN aws_12_24 b
ON a.mukey_0_12 = b.mukey_12_24
),

jn_012_1224_2436 as
(
SELECT *
FROM jn_012_1224 c
INNER JOIN aws_24_36 d
ON c.mukey_12_24 = d.mukey_24_36
),

jn_012_1224_2436_3648 as
(
SELECT *
FROM jn_012_1224_2436 e
INNER JOIN aws_36_48 f
ON e.mukey_12_24 = f.mukey_36_48
),
mukey_aws_inch_intervals as
(
SELECT CAST(mukey_0_12 AS text) as in_mukey,
       aws_0_12in,
	   aws_12_24in,
       aws_24_36in,
       aws_36_48in
FROM jn_012_1224_2436_3648
),
ott_4326_joined_inch_aws as
(
SELECT *
FROM mupoly_ott_4326 m
INNER JOIN mukey_aws_inch_intervals i
ON CAST(m.mukey as text) = i.in_mukey
)
SELECT *
INTO ott_4326_jn_inch_aws
FROM ott_4326_joined_inch_aws
