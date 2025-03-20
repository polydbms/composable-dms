CREATE TABLE tag (
    Id BIGINT PRIMARY KEY,
    TagName VARCHAR(255),
    Count INT,
    ExcerptPostId BIGINT,
    WikiPostId BIGINT
);