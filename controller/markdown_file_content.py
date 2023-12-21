import traceback

from submodules.model.cognition_objects import markdown_file, markdown_dataset
from handler.tokenizer_handler import get_tokenizer
from submodules.model.business_objects import general
from submodules.model.enums import CognitionMarkdownFileState
from spacy.language import Language

SEGMENT_DIVIDER = "\n\n"

def rework_markdown_file_content(org_id: str, file_id: str, step: str) -> bool:
    if step == "SEGMENT_SENTENCES":
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
        max_length = __lookup_final_max_length(nlp)
        # Split the content into smaller chunks if it's too large
        if __utf8len(content) > max_length:
            chunks = __chunk_text_on_bytes(content,max_length - 100)
            processed_chunks = []

            for chunk in chunks:
                doc = nlp(chunk)
                processed_chunk = SEGMENT_DIVIDER.join(
                    [sentence for sentence in __segment_sentences(doc)]
                )
                processed_chunks.append(processed_chunk)

            content = SEGMENT_DIVIDER.join(processed_chunks)
        else:
            doc = nlp(content)
            content = SEGMENT_DIVIDER.join([sentence for sentence in __segment_sentences(doc)])
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

# splits not after x bytes but ensures that max x bytes are used without destroying the characters 
def __chunk_text_on_bytes(text: str, max_chunk_size: int = 1_000_000):
    factor = len(text) / __utf8len(text)
    increase_by = int(max(min(max_chunk_size*.1,10),1))
    initial_size_guess = int(max(max_chunk_size * factor - 10,1))
    final_list = []
    remaining = text
    while len(remaining):
        part = remaining[:initial_size_guess]
        if __utf8len(part) > max_chunk_size:
            initial_size_guess = max(initial_size_guess - min(max_chunk_size *.001,10),1) 
            continue
        cut_after = initial_size_guess
        while __utf8len(part) < max_chunk_size and part != remaining:
            cut_after = min(len(remaining), cut_after+increase_by)
            part = remaining[:cut_after]
            
        if __utf8len(part) > max_chunk_size:
            cut_after-=increase_by
        final_list.append(remaining[:cut_after])
        remaining = remaining[cut_after:]

    return final_list



MAX_LENGTH_OVERWRITE = {
    # japanese has a max length restriction by sudachi so the spacy max_length only applies if < sudachi
    "ja":49149
}

def __lookup_final_max_length(nlp:Language) -> int:
    overwrite = MAX_LENGTH_OVERWRITE.get(nlp.meta["lang"])
    
    if overwrite and overwrite < nlp.max_length:
        return overwrite
    return nlp.max_length


# note that "H" uses up 1 byte while "私" takes 3 bytes
# len(s) would still give 1 but this runs into issues for reserved/allocated spacy memory
def __utf8len(s:str):
    return len(s.encode('utf-8'))
