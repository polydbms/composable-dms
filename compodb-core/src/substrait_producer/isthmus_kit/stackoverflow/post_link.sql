CREATE TABLE post_link (
    Id BIGINT PRIMARY KEY,
    PostId BIGINT NOT NULL,
    RelatedPostId BIGINT NOT NULL,
    LinkTypeId BIGINT,
    CreationDate TIMESTAMP
);