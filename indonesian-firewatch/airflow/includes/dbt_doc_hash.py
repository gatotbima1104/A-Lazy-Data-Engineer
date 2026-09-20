import hashlib
from pathlib import Path


class DbtDocsHash:
    HASH_FILE = ".dbt_docs.hash"

    def __init__(self, project_dir: str | Path):
        self.project_dir = Path(project_dir)
        self.hash_file = self.project_dir / self.HASH_FILE
        self.target_dir = self.project_dir / "target"

    def _compute_hash(self) -> str:
        hasher = hashlib.sha256()

        for folder in ["models", "macros", "seeds"]:
            path = self.project_dir / folder

            if not path.exists():
                continue

            for file in sorted(path.rglob("*")):
                if file.is_file():
                    hasher.update(
                        str(file.relative_to(self.project_dir)).encode()
                    )
                    hasher.update(file.read_bytes())

        return hasher.hexdigest()

    def _load_hash(self) -> str | None:
        if not self.hash_file.exists():
            return None

        return self.hash_file.read_text().strip()

    def _save_hash(self, value: str):
        self.hash_file.write_text(value)

    def docs_exist(self) -> bool:
        return (
            (self.target_dir / "manifest.json").exists()
            and
            (self.target_dir / "catalog.json").exists()
        )

    def should_generate(self) -> bool:
        # First time: documentation has never been generated.
        if not self.docs_exist():
            return True

        # Later: generate only when dbt project changed.
        return self._compute_hash() != self._load_hash()

    def update_hash(self):
        self._save_hash(self._compute_hash())