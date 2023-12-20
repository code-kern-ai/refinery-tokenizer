import traceback

from submodules.model.cognition_objects import markdown_file, markdown_dataset
from handler.tokenizer_handler import get_tokenizer
from submodules.model.business_objects import general
from submodules.model.enums import CognitionMarkdownFileState
from spacy.language import Language


def rework_markdown_file_content(org_id: str, file_id: str, step: str) -> bool:
    if step.lower() == "SEGMENT_SENTENCES":
        return __rework_segment_sentences(org_id, file_id)
    return True


def __rework_segment_sentences(org_id: str, file_id: str) -> bool:
    markdown_file_item = markdown_file.get(org_id, file_id)
    if markdown_file_item is None:
        return False

    dataset_item = markdown_dataset.get(org_id, markdown_file_item.dataset_id)
    if dataset_item is None:
        return False
    content = markdown_file_item.content
    try:
        nlp = get_tokenizer(dataset_item.tokenizer)
        # Split the content into smaller chunks if it's too large
        if len(content) > nlp.max_length:
            chunks = __chunk_text(content)
            processed_chunks = []

            for chunk in chunks:
                doc = nlp(chunk)
                processed_chunk = "\n\n".join(
                    [sentence for sentence in __segment_sentences(doc)]
                )
                processed_chunks.append(processed_chunk)

            content = "\n\n".join(processed_chunks)
        else:
            doc = nlp(content)
            content = "\n\n".join([sentence for sentence in __segment_sentences(doc)])
        markdown_file_item.content = content
        general.commit()
        return True
    except Exception:
        full_traceback = traceback.format_exc()
        print(full_traceback, flush=True)
        markdown_file.update(
            org_id=org_id,
            markdown_file_id=file_id,
            state=CognitionMarkdownFileState.FAILED.value,
            error=full_traceback,  # Store the full stack trace instead of just the error message
        )
        return False


# custom segmentation rule to build very likely sentences from chunk of text
def __segment_sentences(doc: Language):
    sentences = []
    current_sentence = None
    for sent in doc.sents:
        if len(sent.text.strip()) == 0:
            continue
        last_char = sent.text.strip()[-1]

        if current_sentence is None:
            current_sentence = sent.text
        else:
            current_sentence += " " + sent.text

        if last_char in [".", ";", "?", "!"]:
            sentences.append(current_sentence)
            current_sentence = None

    if current_sentence is not None:
        sentences.append(current_sentence)
    return sentences


def __chunk_text(text: str, chunk_size: int = 1_000_000):
    return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]
