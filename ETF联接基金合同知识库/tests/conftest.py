import os


# Keep pytest deterministic; local Word COM field updates are covered by production code paths.
os.environ.setdefault("DISABLE_WORD_FIELD_UPDATE", "1")
