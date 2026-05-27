class TfidfVectorizer:
    def __init__(
        self,
        *,
        ngram_range: tuple[int, int],
        min_df: int,
        max_df: float,
        max_features: int,
    ) -> None: ...
    def fit_transform(self, raw_documents: list[str]) -> object: ...
    def get_feature_names_out(self) -> object: ...
