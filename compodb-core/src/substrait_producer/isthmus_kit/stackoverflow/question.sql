CREATE TABLE question (
    Id BIGINT PRIMARY KEY,
    Title TEXT,
    Body TEXT,
    CreationDate TIMESTAMP,
    UserId BIGINT,
    Score INT,
    ViewCount INT
);