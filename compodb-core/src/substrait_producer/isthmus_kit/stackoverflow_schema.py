import pyarrow as pa

account_schema = pa.schema([
    ('Id', pa.int32()),
    ('UserId', pa.int32()),
    ('Balance', pa.float64()),
    ('LastUpdated', pa.timestamp('ms'))
])

answer_schema = pa.schema([
    ('Id', pa.int32()),
    ('QuestionId', pa.int32()),
    ('AnswerText', pa.string()),
    ('CreationDate', pa.timestamp('ms')),
    ('UserId', pa.int32())
])

badge_schema = pa.schema([
    ('Id', pa.int32()),
    ('UserId', pa.int32()),
    ('Name', pa.string()),
    ('Date', pa.timestamp('ms'))
])

comment_schema = pa.schema([
    ('Id', pa.int32()),
    ('PostId', pa.int32()),
    ('UserId', pa.int32()),
    ('Text', pa.string()),
    ('CreationDate', pa.timestamp('ms'))
])

post_link_schema = pa.schema([
    ('Id', pa.int32()),
    ('PostId', pa.int32()),
    ('RelatedPostId', pa.int32()),
    ('LinkTypeId', pa.int32()),
    ('CreationDate', pa.timestamp('ms'))
])

question_schema = pa.schema([
    ('Id', pa.int32()),
    ('Title', pa.string()),
    ('Body', pa.string()),
    ('CreationDate', pa.timestamp('ms')),
    ('UserId', pa.int32()),
    ('Score', pa.int32()),
    ('ViewCount', pa.int32())
])

site_schema = pa.schema([
    ('Id', pa.int32()),
    ('Name', pa.string()),
    ('Url', pa.string()),
    ('Description', pa.string())
])

so_user_schema = pa.schema([
    ('Id', pa.int32()),
    ('DisplayName', pa.string()),
    ('Reputation', pa.int32()),
    ('CreationDate', pa.timestamp('ms')),
    ('LastAccessDate', pa.timestamp('ms'))
])

tag_schema = pa.schema([
    ('Id', pa.int32()),
    ('TagName', pa.string()),
    ('Count', pa.int32()),
    ('ExcerptPostId', pa.int32()),
    ('WikiPostId', pa.int32())
])

tag_question_schema = pa.schema([
    ('TagId', pa.int32()),
    ('QuestionId', pa.int32())
])