DROP TABLE IF EXISTS mapunitcopy;

CREATE TABLE mapunitcopy
(
  OID_ int,
  musym text,
  muname text,
  mukind text,
  mustatus text,
  muacres int,
  mapunitlfw_l int,
  mapunitlfw_r int,
  mapunitlfw_h int,
  mapunitpfa_l text,
  mapunitpfa_r text,
  mapunitpfa_h text,
  farmlndcl text,
  muhelcl text,
  muwathelcl text,
  muwndhelcl text,
  interpfocus text,
  invesintens text,
  iacornsr text,
  nhiforsoigrp text,
  nhspiagr text,
  vtsepticsyscl text,
  mucertstat text,
  lkey int,
  mukey int
);
\COPY mapunitcopy FROM '/Users/michaelfelzan/Desktop/LCCMR_Soils/table_outputs/mapunitcopy.csv' WITH CSV Header ENCODING 'LATIN-1';
