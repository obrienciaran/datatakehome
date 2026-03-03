"""Abstract base classes for LLM-powered data operations.

Defines three categories of LLM usage in data pipelines:

- **Transform**: Extracts structured information from unstructured text
  e.g. extracting primary disorder from free-text clinical notes.

  The below are added just for illustrative purposes:
- **Augment**: Enriches existing structured records with LLM-derived fields
  e.g. adding ICD-10 codes or severity scores.
- **Synthetic**: Generates artificial records for model testing or training
  e.g. synthetic triage notes.
"""

from abc import ABC, abstractmethod

import pandas as pd


class Transform(ABC):
    """Extract structured data from unstructured text using an LLM."""

    @abstractmethod
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the LLM transformation and return the enriched DataFrame."""
        ...


class Augment(ABC):
    """Enrich existing structured records with LLM-derived fields."""

    pass


class Synthetic(ABC):
    """Generate synthetic records using an LLM."""

    pass
