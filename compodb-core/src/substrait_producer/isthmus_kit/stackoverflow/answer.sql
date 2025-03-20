CREATE TABLE answer (
    Id BIGINT PRIMARY KEY,
    QuestionId BIGINT NOT NULL,
    AnswerText TEXT,
    CreationDate TIMESTAMP,
    UserId BIGINT
);