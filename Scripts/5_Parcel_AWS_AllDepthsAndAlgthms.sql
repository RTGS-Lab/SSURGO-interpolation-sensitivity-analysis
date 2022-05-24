SELECT gid,
       awa_aws_0_12,
	     awa_aws_12_24,
	     awa_aws_24_36,
	     awa_aws_36_48,
	     aws0_12_g80,
	     aws12_24_g80,
	     aws24_36_g80,
	     aws36_48_g80,
	     aws0_12_l20,
	     aws12_24_l20,
	     aws24_36_l20,
	     aws36_48_l20
INTO ottinches_all_awa_joined
FROM ott_inches_parcel_awa_aws a
LEFT JOIN ottinches_heuristic_table h
ON a.gid = h.gid_g80
