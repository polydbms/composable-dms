import pyarrow as pa

aka_name_schema = pa.schema([
    ('id', pa.int32()),
    ('person_id', pa.int32()),
    ('name', pa.string()),
    ('imdb_index', pa.string()),
    ('name_pcode_cf', pa.string()),
    ('name_pcode_nf', pa.string()),
    ('surname_pcode', pa.string()),
    ('md5sum', pa.string())
])

aka_title_schema = pa.schema([
    ('id', pa.int32()),
    ('movie_id', pa.int32()),
    ('title', pa.string()),
    ('imdb_index', pa.string()),
    ('kind_id', pa.int32()),
    ('production_year', pa.int32()),
    ('phonetic_code', pa.string()),
    ('episode_of_id', pa.int32()),
    ('season_nr', pa.int32()),
    ('episode_nr', pa.int32()),
    ('note', pa.string()),
    ('md5sum', pa.string())
])

cast_info_schema = pa.schema([
    ('id', pa.int32()),
    ('person_id', pa.int32()),
    ('movie_id', pa.int32()),
    ('person_role_id', pa.int32()),
    ('note', pa.string()),
    ('nr_order', pa.int32()),
    ('role_id', pa.int32())
])

char_name_schema = pa.schema([
    ('id', pa.int32()),
    ('name', pa.string()),
    ('imdb_index', pa.string()),
    ('imdb_id', pa.int32()),
    ('name_pcode_nf', pa.string()),
    ('surname_pcode', pa.string()),
    ('md5sum', pa.string())
])

comp_cast_type_schema = pa.schema([
    ('id', pa.int32()),
    ('kind', pa.string())
])

company_name_schema = pa.schema([
    ('id', pa.int32()),
    ('name', pa.string()),
    ('country_code', pa.string()),
    ('imdb_id', pa.int32()),
    ('name_pcode_nf', pa.string()),
    ('name_pcode_sf', pa.string()),
    ('md5sum', pa.string())
])

company_type_schema = pa.schema([
    ('id', pa.int32()),
    ('kind', pa.string())
])

complete_cast_schema = pa.schema([
    ('id', pa.int32()),
    ('movie_id', pa.int32()),
    ('subject_id', pa.int32()),
    ('status_id', pa.int32())
])

info_type_schema = pa.schema([
    ('id', pa.int32()),
    ('info', pa.string())
])

keyword_schema = pa.schema([
    ('id', pa.int32()),
    ('keyword', pa.string()),
    ('phonetic_code', pa.string())
])

kind_type_schema = pa.schema([
    ('id', pa.int32()),
    ('kind', pa.string())
])

link_type_schema = pa.schema([
    ('id', pa.int32()),
    ('link', pa.string())
])

movie_companies_schema = pa.schema([
    ('id', pa.int32()),
    ('movie_id', pa.int32()),
    ('company_id', pa.int32()),
    ('company_type_id', pa.int32()),
    ('note', pa.string())
])

movie_info_schema = pa.schema([
    ('id', pa.int32()),
    ('movie_id', pa.int32()),
    ('info_type_id', pa.int32()),
    ('info', pa.string()),
    ('note', pa.string())
])

movie_info_idx_schema = pa.schema([
    ('id', pa.int32()),
    ('movie_id', pa.int32()),
    ('info_type_id', pa.int32()),
    ('info', pa.string()),
    ('note', pa.string())
])

movie_keyword_schema = pa.schema([
    ('id', pa.int32()),
    ('movie_id', pa.int32()),
    ('keyword_id', pa.int32())
])

movie_link_schema = pa.schema([
    ('id', pa.int32()),
    ('movie_id', pa.int32()),
    ('linked_movie_id', pa.int32()),
    ('link_type_id', pa.int32())
])

name_schema = pa.schema([
    ('id', pa.int32()),
    ('name', pa.string()),
    ('imdb_index', pa.string()),
    ('imdb_id', pa.int32()),
    ('gender', pa.string()),
    ('name_pcode_cf', pa.string()),
    ('name_pcode_nf', pa.string()),
    ('surname_pcode', pa.string()),
    ('md5sum', pa.string())
])

person_info_schema = pa.schema([
    ('id', pa.int32()),
    ('person_id', pa.int32()),
    ('info_type_id', pa.int32()),
    ('info', pa.string()),
    ('note', pa.string())
])

role_type_schema = pa.schema([
    ('id', pa.int32()),
    ('role', pa.string())
])

title_schema = pa.schema([
    ('id', pa.int32()),
    ('title', pa.string()),
    ('imdb_index', pa.string()),
    ('kind_id', pa.int32()),
    ('production_year', pa.int32()),
    ('imdb_id', pa.int32()),
    ('phonetic_code', pa.string()),
    ('episode_of_id', pa.int32()),
    ('season_nr', pa.int32()),
    ('episode_nr', pa.int32()),
    ('series_years', pa.string()),
    ('md5sum', pa.string())
])