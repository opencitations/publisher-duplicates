import polars as pl


def is_cjk(text):

    # Unicode ranges for CJK characters
    cjk_ranges = [
        (0x4E00, 0x9FFF),  # CJK Unified Ideographs
        (0x3040, 0x309F),  # Hiragana
        (0x30A0, 0x30FF),  # Katakana
        (0x3400, 0x4DBF),  # CJK Unified Ideographs Extension A
        (0xF900, 0xFAFF),  # CJK Compatibility Ideographs
        (0xAC00, 0xD7AF),  # Hangul Syllables
        (0x20000, 0x2A6DF),  # CJK Unified Ideographs Extension B
        (0x2A700, 0x2B73F),  # CJK Unified Ideographs Extension C
        (0x2B740, 0x2B81F),  # CJK Unified Ideographs Extension D
        (0x2B820, 0x2CEAF),  # CJK Unified Ideographs Extension E
        (0x2CEB0, 0x2EBEF),  # CJK Unified Ideographs Extension F
        (0x30000, 0x3134F),  # CJK Unified Ideographs Extension G
        (0x31350, 0x323AF),  # CJK Unified Ideographs Extension H
        (
            0xFF00,
            0xFFEF,
        ),  # Half-width and Full-width Forms (includes Japanese punctuation)
    ]

    # Check if any character in text falls within CJK ranges
    for char in text:
        code = ord(char)
        if any(start <= code <= end for start, end in cjk_ranges):
            return None
    return text


sample_df = (
    pl.read_csv("./lit_dupes.csv")
    .with_columns(pl.col("literal").map_elements(is_cjk, return_dtype=pl.String()))
    .filter(pl.col("literal") != "")
    .sample(100, seed=42, shuffle=True)
)
sample_df.write_csv("./sample_lit_dupes.csv")
