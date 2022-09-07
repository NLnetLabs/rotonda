/prefixes/<IP-ADDR>/<LEN>?include=<FIELD_NAMES,...>&details[FIELD_NAME]=<DETAILS_LEVEL>

DETAIL_LEVEL

"all" | "low" | "high" | <FIELD_NAME>'['<DETAIL_LEVEL>']'


/prefixes/10.1.0.0/24?include=communities,less_specifics&detail[less_specific]=none

/prefixes/10.1.0.0/24?include=specifics_range[0..32]&detail[less_specifics]=communities[all]

/prefixes/10.1.0.0/24?include=less_specifics_range[0..16]&detail[less_specifics]=all

/prefixes/10.1.0.0/24?include=less_specifics_range[0..16],more_specific[32]&detail[less_specifics]=low