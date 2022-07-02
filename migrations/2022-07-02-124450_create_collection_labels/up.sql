CREATE TABLE collection_labels (
    collection_id UUID NOT NULL,
    label_id UUID NOT NULL,
    PRIMARY KEY (collection_id, label_id),
    CONSTRAINT fk_collection_id FOREIGN KEY (collection_id) REFERENCES collections(id),
    CONSTRAINT fk_label_id FOREIGN KEY (label_id) REFERENCES labels(id)
)