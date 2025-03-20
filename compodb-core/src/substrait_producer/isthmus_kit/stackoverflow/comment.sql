CREATE TABLE comment (
    Id BIGINT PRIMARY KEY,
    PostId BIGINT NOT NULL,
    UserId BIGINT,
    Text TEXT,
    CreationDate TIMESTAMP
);