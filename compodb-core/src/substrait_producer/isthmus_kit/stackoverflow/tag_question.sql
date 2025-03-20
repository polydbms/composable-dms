CREATE TABLE tag_question (
    TagId BIGINT NOT NULL,
    QuestionId BIGINT NOT NULL,
    PRIMARY KEY (TagId, QuestionId)
);