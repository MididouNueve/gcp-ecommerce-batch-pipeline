"""Render the repository's SQL templates locally; never execute a query."""

import argparse
import os
import re
from pathlib import Path
from string import Template


def render_sql(template: str, project_id: str) -> str:
    # Restrict substitutions to identifiers, not arbitrary SQL fragments.
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", project_id):
        raise ValueError("PROJECT_ID doit être un identifiant GCP de 6 à 30 caractères.")
    return Template(template).substitute(PROJECT_ID=project_id, BQ_RAW_DS="raw")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("template", type=Path)
    parser.add_argument("--project", default=os.environ.get("PROJECT_ID", ""))
    args = parser.parse_args()
    try:
        result = render_sql(args.template.read_text(encoding="utf-8"), args.project)
    except (OSError, ValueError, KeyError) as error:
        parser.exit(2, f"Erreur : {error}\n")
    print(result, end="" if result.endswith("\n") else "\n")


if __name__ == "__main__":
    main()
