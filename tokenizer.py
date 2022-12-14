from spacy.tokens import DocBin


def get_doc_bin_in_bytes(tokenizer, record_item, text_attributes):
    doc_bin = DocBin()
    for key in record_item.data:
        if key in text_attributes:
            doc = tokenizer(record_item.data[key])
            doc_bin.add(doc)
    return doc_bin.to_bytes()
